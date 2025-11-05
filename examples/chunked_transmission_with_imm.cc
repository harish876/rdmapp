#include "acceptor.h"
#include "connector.h"
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <rdmapp/rdmapp.h>

constexpr size_t DEFAULT_BUFFER_SIZE = 4096;
constexpr size_t DEFAULT_CHUNK_SIZE = 1500;

rdmapp::task<void> handle_qp(std::shared_ptr<rdmapp::qp> qp,
                             size_t buffer_size) {
  std::vector<uint8_t> recv_buffer(buffer_size);
  auto recv_mr = std::make_shared<rdmapp::local_mr>(
      qp->pd_ptr()->reg_mr(recv_buffer.data(), recv_buffer.size()));

  auto recv_mr_serialized = recv_mr->serialize();
  co_await qp->send(recv_mr_serialized.data(), recv_mr_serialized.size());
  std::cout << "Sent receive buffer MR to client: addr=" << recv_mr->addr()
            << " length=" << recv_mr->length() << " rkey=" << recv_mr->rkey()
            << std::endl;

  char remote_mr_serialized[rdmapp::remote_mr::kSerializedSize];
  co_await qp->recv(remote_mr_serialized, sizeof(remote_mr_serialized));
  auto remote_mr = rdmapp::remote_mr::deserialize(remote_mr_serialized);
  std::cout << "Received remote MR from client: addr=" << remote_mr.addr()
            << " length=" << remote_mr.length() << " rkey=" << remote_mr.rkey()
            << std::endl;

  size_t num_chunks =
      (buffer_size + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE;
  std::cout << "Receiving " << buffer_size << " bytes in " << num_chunks
            << " chunks via RDMA write_with_imm" << std::endl;

  // Warm up the connection with a small send/recv (like helloworld does)
  char warmup_buffer[6];
  auto [warmup_n, __] = co_await qp->recv(warmup_buffer, sizeof(warmup_buffer));
  char warmup_send[6] = "world";
  co_await qp->send(warmup_send, sizeof(warmup_send));
  std::cout << "Connection warmed up, received " << warmup_n << " bytes" << std::endl;

  // Post one receive for each chunk (each chunk uses write_with_imm)
  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    auto [_, imm] = co_await qp->recv(recv_mr);
    if (imm.has_value()) {
      std::cout << "Received chunk " << chunk_idx << " (imm=" << imm.value() 
                << ")" << std::endl;
    }
  }

  std::cout << "All chunks received! Buffer size: " << recv_buffer.size()
            << " bytes" << std::endl;

  std::cout << "First 16 bytes of received buffer: ";
  for (size_t i = 0; i < std::min(16UL, recv_buffer.size()); ++i) {
    std::cout << std::hex << static_cast<int>(recv_buffer[i]) << " ";
  }
  std::cout << std::dec << std::endl;

  co_return;
}

rdmapp::task<void> server(rdmapp::acceptor &acceptor, size_t buffer_size) {
  while (true) {
    auto qp = co_await acceptor.accept();
    handle_qp(qp, buffer_size).detach();
  }
  co_return;
}

rdmapp::task<void> client(rdmapp::connector &connector, size_t buffer_size) {
  auto qp = co_await connector.connect();

  char remote_mr_serialized[rdmapp::remote_mr::kSerializedSize];
  co_await qp->recv(remote_mr_serialized, sizeof(remote_mr_serialized));
  auto remote_mr = rdmapp::remote_mr::deserialize(remote_mr_serialized);
  std::cout << "Received remote MR from server: addr=" << remote_mr.addr()
            << " length=" << remote_mr.length() << " rkey=" << remote_mr.rkey()
            << std::endl;

  std::vector<uint8_t> send_buffer(buffer_size);
  for (size_t i = 0; i < buffer_size; ++i) {
    send_buffer[i] = static_cast<uint8_t>(rand() % 256);
  }

  auto send_mr = std::make_shared<rdmapp::local_mr>(
      qp->pd_ptr()->reg_mr(send_buffer.data(), send_buffer.size()));

  auto send_mr_serialized = send_mr->serialize();
  co_await qp->send(send_mr_serialized.data(), send_mr_serialized.size());
  std::cout << "Sent send buffer MR to server: addr=" << send_mr->addr()
            << " length=" << send_mr->length() << " rkey=" << send_mr->rkey()
            << std::endl;

  size_t num_chunks =
      (buffer_size + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE;
  std::cout << "Sending " << buffer_size << " bytes in " << num_chunks
            << " chunks of " << DEFAULT_CHUNK_SIZE
            << " bytes each via RDMA write_with_imm" << std::endl;

  // Warm up the connection with a small send/recv (like helloworld does)
  char warmup_buffer[6] = "hello";
  co_await qp->send(warmup_buffer, sizeof(warmup_buffer));
  char warmup_recv[6];
  auto [n, _] = co_await qp->recv(warmup_recv, sizeof(warmup_recv));
  std::cout << "Connection warmed up, received " << n << " bytes" << std::endl;

  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t chunk_offset = chunk_idx * DEFAULT_CHUNK_SIZE;
    size_t chunk_length =
        std::min(DEFAULT_CHUNK_SIZE, buffer_size - chunk_offset);

    rdmapp::remote_mr chunk_remote_mr(
        reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(remote_mr.addr()) +
                                 chunk_offset),
        static_cast<uint32_t>(chunk_length), remote_mr.rkey());

    std::cout << "Writing chunk " << chunk_idx << "..." << std::endl;
    std::cout.flush();

    // Use write_with_imm for each chunk (imm value is the chunk index)
    co_await qp->write_with_imm(chunk_remote_mr,
                                send_buffer.data() + chunk_offset, chunk_length,
                                static_cast<uint32_t>(chunk_idx));

    std::cout << "Sent chunk " << chunk_idx << ": offset=" << chunk_offset
              << " length=" << chunk_length << std::endl;
  }

  std::cout << "All chunks sent!" << std::endl;
  co_return;
}

int main(int argc, char *argv[]) {
  srand(42);

  size_t buffer_size = DEFAULT_BUFFER_SIZE;

  auto device = std::make_shared<rdmapp::device>(0, 1, 3);
  auto pd = std::make_shared<rdmapp::pd>(device);
  auto cq = std::make_shared<rdmapp::cq>(device);
  auto cq_poller = std::make_shared<rdmapp::cq_poller>(cq);
  auto loop = rdmapp::socket::event_loop::new_loop();
  auto looper = std::thread([loop]() { loop->loop(); });

  if (argc == 2) {
    rdmapp::acceptor acceptor(loop, std::stoi(argv[1]), pd, cq);
    server(acceptor, buffer_size);
  } else if (argc == 3) {
    if (std::string(argv[1]).find('.') != std::string::npos ||
        std::string(argv[1]).find(':') != std::string::npos) {
      rdmapp::connector connector(loop, argv[1], std::stoi(argv[2]), pd, cq);
      client(connector, buffer_size);
    } else {
      buffer_size = std::stoull(argv[2]);
      rdmapp::acceptor acceptor(loop, std::stoi(argv[1]), pd, cq);
      server(acceptor, buffer_size);
    }
  } else if (argc == 4) {
    buffer_size = std::stoull(argv[3]);
    rdmapp::connector connector(loop, argv[1], std::stoi(argv[2]), pd, cq);
    client(connector, buffer_size);
  } else {
    std::cout << "Usage: " << argv[0] << " [port] [buffer_size] for server"
              << std::endl;
    std::cout << "       " << argv[0]
              << " [server_ip] [port] [buffer_size] for client" << std::endl;
    std::cout << "       buffer_size defaults to " << DEFAULT_BUFFER_SIZE
              << " bytes" << std::endl;
  }

  loop->close();
  looper.join();
  return 0;
}

