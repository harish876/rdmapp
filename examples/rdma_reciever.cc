#include "rdma_receiver.h"
#include <iostream>
#include <chrono>
#include <cstring>
#include <pthread.h>

namespace RDMA_EC {

RDMAReceiver::RDMAReceiver(std::shared_ptr<rdmapp::connector> connector,
                           std::shared_ptr<rdmapp::cq> recv_cq,
                           const Config& config)
    : connector_(connector), recv_cq_(recv_cq), config_(config) {
    std::cout << "Receiver: Initialized with MTU=" << config_.mtu 
              << ", chunk_size=" << config_.chunk_size << std::endl;
    
    // Allocate dummy buffer for receives
    dummy_recv_buffer_.resize(1);
}

RDMAReceiver::~RDMAReceiver() {
    stop_thread_ = true;
    completion_cv_.notify_all();
    if (completion_thread_.joinable()) {
        completion_thread_.join();
    }
    if (frontend_thread_.joinable()) {
        frontend_thread_.join();
    }
}

rdmapp::task<std::vector<uint8_t>> RDMAReceiver::receive_data(size_t expected_size) {
    expected_size_ = expected_size;
    
    // Connect to sender
    std::cout << "Receiver: Connecting..." << std::endl;
    qp_ = co_await connector_->connect();
    std::cout << "Receiver: Connected" << std::endl;
    
    // Allocate and register receive buffer
    recv_buffer_.resize(config_.buffer_size);
    auto pd = qp_->pd_ptr();
    local_mr_ = std::make_shared<rdmapp::local_mr>(
        pd->reg_mr(recv_buffer_.data(), recv_buffer_.size()));
    
    // Calculate expected packets and chunks
    total_packets_ = calculate_num_packets(expected_size, config_.mtu);
    total_chunks_ = calculate_num_chunks(total_packets_, config_.chunk_size);
    
    // Initialize packet bitmap: each element is atomic<uint16_t> representing 16 packets
    // Note: atomic types are not copyable/movable, so we must construct with the right size
    // from the start. The constructor will default-construct each element (initialized to 0)
    size_t bitmap_size = (total_packets_ + 15) / 16;  // Round up to nearest 16
    packet_bitmap_ = std::vector<std::atomic<uint16_t>>(bitmap_size);
    
    // Initialize chunk bitmap
    chunk_bitmap_.store(0, std::memory_order_relaxed);
    
    std::cout << "Receiver: Expecting " << total_packets_ << " packets in " 
              << total_chunks_ << " chunks for " << expected_size << " bytes" << std::endl;
    
    // Post receives for immediate values
    // Must post before sending CTS
    co_await post_receives(total_packets_ + 10); // Extra for safety
    
    // Send CTS to sender
    co_await send_cts(expected_size);
    
    // Start background threads for processing completions and frontend polling
    completion_thread_ = std::thread(&RDMAReceiver::process_completions, this);
    frontend_thread_ = std::thread(&RDMAReceiver::frontend_poller, this);
    
    // Set CPU affinity if configured
    if (config_.cpu_core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(config_.cpu_core_id, &cpuset);
        pthread_setaffinity_np(completion_thread_.native_handle(), 
                              sizeof(cpu_set_t), &cpuset);
        std::cout << "Receiver: Pinned completion thread to CPU " 
                  << config_.cpu_core_id << std::endl;
    }
    
    // Wait for all packets to arrive
    {
        std::unique_lock<std::mutex> lock(completion_mutex_);
        auto timeout = std::chrono::seconds(10);
        bool success = completion_cv_.wait_for(lock, timeout, [this] {
            return reception_complete_.load();
        });
        
        if (!success) {
            std::cout << "Receiver: Timeout waiting for packets" << std::endl;
        }
    }
    
    // Stop background threads
    stop_thread_ = true;
    if (completion_thread_.joinable()) {
        completion_thread_.join();
    }
    if (frontend_thread_.joinable()) {
        frontend_thread_.join();
    }
    
    // Update statistics
    bytes_received_ = expected_size;
    
    // Return the received data
    std::vector<uint8_t> result(recv_buffer_.begin(), 
                                recv_buffer_.begin() + expected_size);
    
    std::cout << "Receiver: Transfer complete. Received " 
              << packets_received_.load() << " packets (" 
              << expected_size << " bytes)" << std::endl;
    
    co_return result;
}

rdmapp::task<void> RDMAReceiver::send_cts(size_t buffer_size) {
    CTSInfo cts;
    cts.remote_addr = reinterpret_cast<uint64_t>(recv_buffer_.data());
    cts.rkey = local_mr_->rkey();
    cts.buffer_size = buffer_size;
    cts.total_packets = total_packets_;
    cts.msg_id = current_msg_id_++;
    
    co_await qp_->send(&cts, sizeof(cts));
    
    std::cout << "Receiver: Sent CTS - addr=0x" << std::hex 
              << cts.remote_addr << ", rkey=0x" << cts.rkey 
              << std::dec << std::endl;
    
    co_return;
}

rdmapp::task<void> RDMAReceiver::post_receives(size_t count) {
    // Post dummy receives to catch immediate values
    // In RDMA Write with Immediate, the immediate value comes
    // in the completion, not in the receive buffer
    
    for (size_t i = 0; i < count; ++i) {
        // Each receive will catch one immediate value
        // The actual data is written directly to memory via RDMA Write
        auto recv_task = qp_->recv(dummy_recv_buffer_.data(), 
                                   dummy_recv_buffer_.size());
        // Don't await here - let them be processed asynchronously
    }
    
    std::cout << "Receiver: Posted " << count << " receives for immediates" << std::endl;
    
    co_return;
}

void RDMAReceiver::process_completions() {
    std::cout << "Receiver: Completion thread started" << std::endl;
    
    constexpr size_t batch_size = 32;
    std::vector<struct ibv_wc> wc_vec(batch_size);
    
    while (!stop_thread_) {
        // Poll the completion queue
        size_t num_completions = recv_cq_->poll(wc_vec);
        
        for (size_t i = 0; i < num_completions; ++i) {
            const auto& wc = wc_vec[i];
            
            // Check if this completion has an immediate value
            if (wc.wc_flags & IBV_WC_WITH_IMM) {
                uint32_t imm = wc.imm_data;
                
                // Decode immediate value to get packet index
                auto [msg_id, packet_idx] = decode_immediate(imm);
                
                // Verify message ID matches
                if (msg_id != current_msg_id_ - 1) {
                    std::cout << "Receiver: Warning - message ID mismatch: expected " 
                              << (current_msg_id_ - 1) << ", got " << msg_id << std::endl;
                    continue;
                }
                
                // Verify packet index is valid
                if (packet_idx >= total_packets_) {
                    std::cout << "Receiver: Warning - invalid packet index: " 
                              << packet_idx << " (max: " << total_packets_ << ")" << std::endl;
                    continue;
                }
                
                // Get the bitmap entry index (packet_idx / 16)
                // Each bitmap entry represents 16 packets
                size_t bitmap_idx = packet_idx / 16;
                
                // Set the bit atomically using fetch_or
                uint16_t bit_mask = 1U << (packet_idx % 16);
                packet_bitmap_[bitmap_idx].fetch_or(bit_mask, std::memory_order_release);
                
                packets_received_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        
        // Check if all packets have been received
        size_t received_count = packets_received_.load(std::memory_order_acquire);
        if (received_count >= total_packets_) {
            std::lock_guard<std::mutex> lock(completion_mutex_);
            reception_complete_ = true;
            completion_cv_.notify_all();
            break;
        }
        
        // Small sleep to avoid busy-waiting when no completions
        if (num_completions == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
    }
    
    std::cout << "Receiver: Completion thread exiting" << std::endl;
}

void RDMAReceiver::frontend_poller() {
    std::cout << "Receiver: Frontend poller thread started" << std::endl;
    
    while (!stop_thread_) {
        // Poll the packet bitmap and update chunk bitmap
        // Check each packet bitmap entry
        for (size_t i = 0; i < packet_bitmap_.size(); ++i) {
            // Read the packet bitmap entry atomically
            uint16_t packet_mask = packet_bitmap_[i].load(std::memory_order_acquire);
            
            // Check if the mask (0xFFFF & packet_bitmap_[i]) indicates all bits are set
            // This means all 16 packets in this bitmap entry are received
            if ((packet_mask & 0xFFFF) == 0xFFFF) {
                // Calculate which packets this bitmap entry covers
                size_t first_packet = i * 16;
                size_t last_packet = std::min(first_packet + 15, total_packets_ - 1);
                
                // For each chunk that overlaps with these packets, check if it's complete
                // and mark it atomically if so
                size_t first_chunk = first_packet / config_.chunk_size;
                size_t last_chunk = last_packet / config_.chunk_size;
                
                for (size_t chunk_idx = first_chunk; chunk_idx <= last_chunk && chunk_idx < total_chunks_; ++chunk_idx) {
                    // Check if this chunk is already marked
                    uint64_t chunk_bit = 1ULL << chunk_idx;
                    uint64_t current_chunk_bitmap = chunk_bitmap_.load(std::memory_order_acquire);
                    if (current_chunk_bitmap & chunk_bit) {
                        continue; // Already marked
                    }
                    
                    // Check if all packets in this chunk are received
                    bool chunk_complete = true;
                    size_t chunk_start_packet = chunk_idx * config_.chunk_size;
                    size_t chunk_end_packet = std::min(chunk_start_packet + config_.chunk_size - 1, 
                                                       total_packets_ - 1);
                    
                    for (size_t p = chunk_start_packet; p <= chunk_end_packet; ++p) {
                        size_t bmp_idx = p / 16;
                        size_t bit_pos = p % 16;
                        uint16_t bit_mask = 1U << bit_pos;
                        
                        uint16_t bmp_val = packet_bitmap_[bmp_idx].load(std::memory_order_acquire);
                        if ((bmp_val & bit_mask) == 0) {
                            chunk_complete = false;
                            break;
                        }
                    }
                    
                    // If chunk is complete, mark it atomically
                    if (chunk_complete) {
                        chunk_bitmap_.fetch_or(chunk_bit, std::memory_order_release);
                    }
                }
            }
        }
        
        // Check if we should exit
        if (reception_complete_.load(std::memory_order_acquire)) {
            break;
        }
        
        // Small sleep to avoid busy-waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    std::cout << "Receiver: Frontend poller thread exiting" << std::endl;
}

bool RDMAReceiver::is_complete() const {
    // Check if all chunks are complete by checking chunk bitmap
    uint64_t expected_mask = (total_chunks_ == 64) ? UINT64_MAX : ((1ULL << total_chunks_) - 1);
    uint64_t current_mask = chunk_bitmap_.load(std::memory_order_acquire);
    return (current_mask & expected_mask) == expected_mask;
}

} // namespace RDMA_EC
