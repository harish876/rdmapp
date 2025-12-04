#include "rdma_sender.h"

#include "rdma_logger.h"
#include <cstdint>
#include <cstring>
#include <future>
#include <iostream>
#include <sys/types.h>

namespace RDMA_EC {

RDMASender::RDMASender(std::shared_ptr<rdmapp::connector> connector,
                       const Config &config)
    : connector_(connector), config_(config) {
  Logger::set_enabled(config_.enable_logging);
  Logger::set_level(Logger::level_from_string(config_.logging_level));
  Logger::info() << "Sender: Initialized with MTU=" << config_.mtu
                 << ", chunk_size=" << config_.chunk_size;
}

rdmapp::task<void> RDMASender::send_data(const void *data, size_t size,
                                         uint8_t msg_id) {
  Logger::info() << "Sender: Connecting...";
  connector_->set_user_data_fields(msg_id, size);
  qp_ = co_await connector_->connect();
  Logger::info() << "Sender: Connected";

  co_await wait_for_cts();
  Logger::info() << "Sender: Received CTS - remote_addr=0x" << std::hex
                 << cts_info_.remote_addr << ", rkey=0x" << cts_info_.rkey
                 << std::dec << ", packets=" << cts_info_.total_packets;

    // Enable inline sends on QP if configured
    qp_->set_inline_sends_enabled(config_.enable_inline_sends);
  auto pd = qp_->pd_ptr();
  local_mr_ = std::make_shared<rdmapp::local_mr>(
      pd->reg_mr(const_cast<void *>(data), size));

  const uint8_t *data_ptr = static_cast<const uint8_t *>(data);
  size_t num_packets = calculate_num_packets(size, config_.mtu);

  // Ensure total number of packets doesn't exceed 24-bit limit
  if (num_packets > 0xFFFFFF) {
    throw std::runtime_error(
        "Total number of packets exceeds maximum value (2^24 - 1)");
  }

  size_t num_chunks = calculate_num_chunks(num_packets, config_.chunk_size);

  // Note: current_msg_id_ is set from CTS message, not incremented here

  Logger::info() << "Sender: Sending " << size << " bytes in " << num_packets
                 << " packets across " << num_chunks << " chunks";

  // Send chunks in batches — create tasks for several chunks and wait for the
  // batch to complete. This allows multiple outstanding WRs to be posted and
  // increases throughput compared to awaiting each chunk serially.
  std::vector<std::future<void>> futs;
  const size_t chunk_batch =
      config_.num_concurrent_chunks; // tune: number of concurrent chunk-tasks

  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t chunk_start_offset = chunk_idx * config_.chunk_size * config_.mtu;
    size_t packets_in_chunk = std::min(
        config_.chunk_size, num_packets - chunk_idx * config_.chunk_size);
    Logger::info() << "Sender: Scheduling chunk " << chunk_idx << " with "
                   << packets_in_chunk << " packets";

    auto t = config_.enable_batched_sends
                 ? send_chunk_batch(chunk_idx, data_ptr, chunk_start_offset,
                                    packets_in_chunk)
                 : send_chunk(chunk_idx, data_ptr, chunk_start_offset,
                              packets_in_chunk);

    auto &fref = t.get_future();
    t.detach();
    futs.emplace_back(std::move(fref));

    if (futs.size() >= chunk_batch) {
      for (auto &ff : futs) {
        try {
          ff.wait();
        } catch (...) {
        }
      }
      futs.clear();
    }
  }

  for (auto &ff : futs) {
    ff.wait();
  }

  packets_sent_ += num_packets;
  bytes_sent_ += size;

  Logger::info() << "Sender: Transfer complete. Sent " << num_packets
                 << " packets (" << size << " bytes)";

  co_return;
}

rdmapp::task<void> RDMASender::wait_for_cts() {
  auto [bytes, imm_opt] = co_await qp_->recv(&cts_info_, sizeof(CTSInfo));

  if (bytes != sizeof(CTSInfo)) {
    throw std::runtime_error("Invalid CTS message size");
  }
  current_msg_id_ = cts_info_.msg_id;

  co_return;
}

rdmapp::task<void> RDMASender::send_chunk(size_t chunk_idx, const uint8_t *data,
                                          size_t /* chunk_start_offset */,
                                          size_t packets_in_chunk) {
  for (size_t pkt_idx = 0; pkt_idx < packets_in_chunk; ++pkt_idx) {
    size_t global_packet_idx = chunk_idx * config_.chunk_size + pkt_idx;
    size_t offset = global_packet_idx * config_.mtu;
    size_t packet_size = std::min(config_.mtu, cts_info_.buffer_size - offset);

    co_await send_packet(global_packet_idx, data, offset, packet_size);
  }

  co_return;
}

rdmapp::task<void> RDMASender::send_chunk_batch(size_t chunk_idx,
                                                const uint8_t *data,
                                                size_t /* chunk_start_offset */,
                                                size_t packets_in_chunk) {
  if (!qp_ || !local_mr_) {
    throw std::runtime_error(
        "send_chunk_batch: QP or local MR not initialized");
  }

  std::vector<ibv_sge> sges(packets_in_chunk);
  std::vector<ibv_send_wr> wrs(packets_in_chunk);

  for (size_t pkt_idx = 0; pkt_idx < packets_in_chunk; ++pkt_idx) {
    size_t global_packet_idx = chunk_idx * config_.chunk_size + pkt_idx;
    size_t offset = global_packet_idx * config_.mtu;
    size_t packet_size = std::min(config_.mtu, cts_info_.buffer_size - offset);

    rdmapp::remote_mr remote_mr(
        reinterpret_cast<void *>(
            reinterpret_cast<uintptr_t>(cts_info_.remote_addr) + offset),
        static_cast<uint32_t>(packet_size), cts_info_.rkey);

    sges[pkt_idx] = {};
    sges[pkt_idx].addr =
        reinterpret_cast<uint64_t>(const_cast<uint8_t *>(data + offset));
    sges[pkt_idx].length = static_cast<uint32_t>(packet_size);
    sges[pkt_idx].lkey = local_mr_->lkey();

    auto &wr = wrs[pkt_idx];
    std::memset(&wr, 0, sizeof(wr));
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.next = nullptr; // set below when linking
    wr.num_sge = 1;
    wr.sg_list = &sges[pkt_idx];

    uint32_t imm = encode_immediate(current_msg_id_.load(),
                                    static_cast<uint32_t>(global_packet_idx));
    wr.imm_data = imm;
    wr.wr.rdma.remote_addr = reinterpret_cast<uint64_t>(remote_mr.addr());
    wr.wr.rdma.rkey = remote_mr.rkey();

    wr.send_flags = 0;
  }

  for (size_t i = 0; i + 1 < packets_in_chunk; ++i) {
    wrs[i].next = &wrs[i + 1];
  }

  struct ibv_send_wr *head = &wrs[0];
  struct ibv_send_wr *tail = &wrs[packets_in_chunk - 1];

  co_await qp_->post_batch_and_await(*head, *tail);

  co_return;
}

rdmapp::task<void> RDMASender::send_packet(size_t packet_idx,
                                           const uint8_t *data, size_t offset,
                                           size_t packet_size) {
  rdmapp::remote_mr remote_mr(
      reinterpret_cast<void *>(
          reinterpret_cast<uintptr_t>(cts_info_.remote_addr) + offset),
      static_cast<uint32_t>(packet_size), cts_info_.rkey);

  uint32_t imm = encode_immediate(current_msg_id_.load(),
                                  static_cast<uint32_t>(packet_idx));

  Logger::debug() << "Sender: Sending packet " << packet_idx
                  << " offset=" << offset << " size=" << packet_size
                  << " imm=0x" << std::hex << imm << std::dec;

  co_await qp_->write_with_imm(remote_mr, const_cast<uint8_t *>(data + offset),
                               packet_size, imm);

  co_return;
}

} // namespace RDMA_EC
