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

constexpr size_t TOTAL_BUFFER_SIZE = 4096;
constexpr size_t CHUNK_SIZE = 1500;

rdmapp::task<void> handle_qp(std::shared_ptr<rdmapp::qp> qp) {
  std::vector<uint8_t> recv_buffer(TOTAL_BUFFER_SIZE);
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
  
  size_t num_chunks = (TOTAL_BUFFER_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE;
  std::cout << "Receiving " << TOTAL_BUFFER_SIZE << " bytes in " << num_chunks 
            << " chunks" << std::endl;
  
  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    auto [_, imm] = co_await qp->recv(recv_mr);
    if (imm.has_value()) {
      std::cout << "Received chunk " << imm.value() << " completion" << std::endl;
    }
  }
  
  std::cout << "All chunks received! Buffer size: " << recv_buffer.size() << " bytes" << std::endl;
  
  std::cout << "First 16 bytes of received buffer: ";
  for (size_t i = 0; i < std::min(16UL, recv_buffer.size()); ++i) {
    std::cout << std::hex << static_cast<int>(recv_buffer[i]) << " ";
  }
  std::cout << std::dec << std::endl;
  
  co_return;
}

rdmapp::task<void> server(rdmapp::acceptor &acceptor) {
  while (true) {
    auto qp = co_await acceptor.accept();
    handle_qp(qp).detach();
  }
  co_return;
}

rdmapp::task<void> client(rdmapp::connector &connector) {
  auto qp = co_await connector.connect();
  
  char remote_mr_serialized[rdmapp::remote_mr::kSerializedSize];
  co_await qp->recv(remote_mr_serialized, sizeof(remote_mr_serialized));
  auto remote_mr = rdmapp::remote_mr::deserialize(remote_mr_serialized);
  std::cout << "Received remote MR from server: addr=" << remote_mr.addr()
            << " length=" << remote_mr.length() << " rkey=" << remote_mr.rkey()
            << std::endl;
  
  std::vector<uint8_t> send_buffer(TOTAL_BUFFER_SIZE);
  for (size_t i = 0; i < TOTAL_BUFFER_SIZE; ++i) {
    send_buffer[i] = static_cast<uint8_t>(rand() % 256);
  }
  
  auto send_mr = std::make_shared<rdmapp::local_mr>(
      qp->pd_ptr()->reg_mr(send_buffer.data(), send_buffer.size()));
  
  auto send_mr_serialized = send_mr->serialize();
  co_await qp->send(send_mr_serialized.data(), send_mr_serialized.size());
  std::cout << "Sent send buffer MR to server: addr=" << send_mr->addr()
            << " length=" << send_mr->length() << " rkey=" << send_mr->rkey()
            << std::endl;
  
  size_t num_chunks = (TOTAL_BUFFER_SIZE + CHUNK_SIZE - 1) / CHUNK_SIZE;
  std::cout << "Sending " << TOTAL_BUFFER_SIZE << " bytes in " << num_chunks 
            << " chunks of " << CHUNK_SIZE << " bytes each" << std::endl;
  
  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t chunk_offset = chunk_idx * CHUNK_SIZE;
    size_t chunk_length = std::min(CHUNK_SIZE, TOTAL_BUFFER_SIZE - chunk_offset);
    
    rdmapp::remote_mr chunk_remote_mr(
        reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(remote_mr.addr()) + chunk_offset),
        static_cast<uint32_t>(chunk_length),
        remote_mr.rkey());
    
    co_await qp->write_with_imm(chunk_remote_mr, 
                                send_buffer.data() + chunk_offset, 
                                chunk_length, 
                                static_cast<uint32_t>(chunk_idx));
    
    std::cout << "Sent chunk " << chunk_idx << ": offset=" << chunk_offset 
              << " length=" << chunk_length << std::endl;
  }
  
  std::cout << "All chunks sent!" << std::endl;
  co_return;
}

int main(int argc, char *argv[]) {
  srand(42);
  
  auto device = std::make_shared<rdmapp::device>(0, 1);
  auto pd = std::make_shared<rdmapp::pd>(device);
  auto cq = std::make_shared<rdmapp::cq>(device);
  auto cq_poller = std::make_shared<rdmapp::cq_poller>(cq);
  auto loop = rdmapp::socket::event_loop::new_loop();
  auto looper = std::thread([loop]() { loop->loop(); });
  
  if (argc == 2) {
    rdmapp::acceptor acceptor(loop, std::stoi(argv[1]), pd, cq);
    server(acceptor);
  } else if (argc == 3) {
    rdmapp::connector connector(loop, argv[1], std::stoi(argv[2]), pd, cq);
    client(connector);
  } else {
    std::cout << "Usage: " << argv[0] << " [port] for server and " << argv[0]
              << " [server_ip] [port] for client" << std::endl;
  }
  
  loop->close();
  looper.join();
  return 0;
}

