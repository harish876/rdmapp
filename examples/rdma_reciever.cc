#include "rdma_receiver.h"
#include <iostream>
#include <chrono>
#include <cstring>
#include <pthread.h>
#include <infiniband/verbs.h>

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
    
    // Verify dummy_recv_mr_ is set
    if (!dummy_recv_mr_) {
        std::cerr << "Receiver: ERROR - dummy_recv_mr_ not set after post_receives!" << std::endl;
        throw std::runtime_error("dummy_recv_mr_ not initialized");
    }
    std::cout << "Receiver: Verified dummy_recv_mr_ is set (addr=0x" << std::hex 
              << reinterpret_cast<uint64_t>(dummy_recv_mr_->addr()) << std::dec 
              << ", length=" << dummy_recv_mr_->length() << ")" << std::endl;
    
    // Send CTS to sender
    co_await send_cts(expected_size);
    
    // Verify all member variables are ready before starting threads
    std::cout << "Receiver: Pre-thread checks - packet_bitmap_.size()=" << packet_bitmap_.size()
              << ", total_packets_=" << total_packets_ 
              << ", total_chunks_=" << total_chunks_ << std::endl;
    
    // Start background threads for processing completions and frontend polling
    std::cout << "Receiver: Starting completion thread..." << std::endl;
    completion_thread_ = std::thread(&RDMAReceiver::process_completions, this);
    std::cout << "Receiver: Completion thread started successfully" << std::endl;
    
    std::cout << "Receiver: Starting frontend thread..." << std::endl;
    frontend_thread_ = std::thread(&RDMAReceiver::frontend_poller, this);
    std::cout << "Receiver: Frontend thread started successfully" << std::endl;
    
    // Give threads time to fully start before setting CPU affinity
    // This prevents potential race conditions where affinity is set before thread is ready
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // Set CPU affinity if configured
    if (config_.cpu_core_id >= 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(config_.cpu_core_id, &cpuset);
        
        // Set affinity for completion thread
        int ret = pthread_setaffinity_np(completion_thread_.native_handle(), 
                                        sizeof(cpu_set_t), &cpuset);
        if (ret != 0) {
            std::cerr << "Receiver: Warning - failed to set CPU affinity for completion thread: " 
                      << ret << std::endl;
        } else {
            std::cout << "Receiver: Pinned completion thread to CPU " 
                      << config_.cpu_core_id << std::endl;
        }
        
        // Set affinity for frontend thread
        ret = pthread_setaffinity_np(frontend_thread_.native_handle(), 
                                    sizeof(cpu_set_t), &cpuset);
        if (ret != 0) {
            std::cerr << "Receiver: Warning - failed to set CPU affinity for frontend thread: " 
                      << ret << std::endl;
        } else {
            std::cout << "Receiver: Pinned frontend thread to CPU " 
                      << config_.cpu_core_id << std::endl;
        }
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
    // Post receives to catch immediate values from RDMA Write with Immediate
    // In RDMA Write with Immediate, the immediate value comes in the receive completion
    // The actual data is written directly to memory via RDMA Write
    
    // Register a memory region for the dummy receive buffer
    auto pd = qp_->pd_ptr();
    dummy_recv_mr_ = std::make_shared<rdmapp::local_mr>(
        pd->reg_mr(dummy_recv_buffer_.data(), dummy_recv_buffer_.size()));
    
    // Post initial batch of receives (limited by queue capacity ~128)
    // We'll repost receives as they're consumed in process_completions
    constexpr size_t max_initial_receives = 128;
    size_t initial_count = std::min(count, max_initial_receives);
    
    std::cout << "Receiver: Posting initial batch of " << initial_count 
              << " receives (queue capacity limited, will repost as consumed)..." << std::endl;
    
    for (size_t i = 0; i < initial_count; ++i) {
        post_single_receive();
    }
    
    std::cout << "Receiver: Posted " << initial_count << " initial receives" << std::endl;
    
    co_return;
}

void RDMAReceiver::post_single_receive() {
    // Create local copies of shared_ptrs to ensure they stay alive during the operation
    auto mr = dummy_recv_mr_;
    auto qp = qp_;
    
    // Check if dummy_recv_mr_ and qp_ are initialized
    if (!mr) {
        std::cerr << "Receiver: Error - dummy_recv_mr_ not initialized!" << std::endl;
        return;
    }
    
    if (!qp) {
        std::cerr << "Receiver: Error - qp_ not initialized!" << std::endl;
        return;
    }
    
    struct ibv_sge recv_sge;
    recv_sge.addr = reinterpret_cast<uint64_t>(mr->addr());
    recv_sge.length = mr->length();
    recv_sge.lkey = mr->lkey();
    
    struct ibv_recv_wr recv_wr = {};
    struct ibv_recv_wr *bad_recv_wr = nullptr;
    recv_wr.next = nullptr;
    recv_wr.num_sge = 1;
    recv_wr.wr_id = 0;  // We'll identify by immediate value in completion
    recv_wr.sg_list = &recv_sge;
    
    try {
        qp->post_recv(recv_wr, bad_recv_wr);
    } catch (const std::exception& e) {
        std::cerr << "Receiver: Failed to post receive: " << e.what() << std::endl;
        // Don't throw - just log, we'll try again later
    } catch (...) {
        std::cerr << "Receiver: Unknown exception in post_recv!" << std::endl;
    }
}

void RDMAReceiver::process_completions() {
    std::cout << "Receiver: Completion thread started" << std::endl;
    
    // Wait a bit to ensure dummy_recv_mr_, recv_cq_, and packet_bitmap_ are initialized
    // This is a safety measure - post_receives() should complete before threads start
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    if (!dummy_recv_mr_) {
        std::cerr << "Receiver: FATAL - dummy_recv_mr_ not initialized in completion thread!" << std::endl;
        return;
    }
    
    if (!recv_cq_) {
        std::cerr << "Receiver: FATAL - recv_cq_ not initialized in completion thread!" << std::endl;
        return;
    }
    
    if (packet_bitmap_.empty()) {
        std::cerr << "Receiver: FATAL - packet_bitmap_ is empty in completion thread!" << std::endl;
        return;
    }
    
    std::cout << "Receiver: Completion thread ready - packet_bitmap_.size()=" 
              << packet_bitmap_.size() << ", total_packets_=" << total_packets_ << std::endl;
    
    constexpr size_t batch_size = 32;
    std::vector<struct ibv_wc> wc_vec(batch_size);
    size_t total_polled = 0;
    size_t total_with_imm = 0;
    
    while (!stop_thread_) {
        // Poll the completion queue
        if (!recv_cq_) {
            std::cerr << "Receiver: recv_cq_ is null!" << std::endl;
            break;
        }
        size_t num_completions = recv_cq_->poll(wc_vec);
        total_polled += num_completions;
        
        if (num_completions > 0) {
            std::cout << "Receiver: Polled " << num_completions << " completions (total: " 
                      << total_polled << ")" << std::endl;
        }
        
        for (size_t i = 0; i < num_completions; ++i) {
            const auto& wc = wc_vec[i];
            
            // Only process receive completions (from our post_recv operations)
            // Send completions are handled by cq_poller on the send CQ
            if (wc.opcode != IBV_WC_RECV && wc.opcode != IBV_WC_RECV_RDMA_WITH_IMM) {
                std::cout << "Receiver: Skipping non-receive completion: opcode=" << wc.opcode << std::endl;
                continue;
            }
            
            // Check completion status
            if (wc.status != IBV_WC_SUCCESS) {
                std::cout << "Receiver: Completion error: status=" << wc.status 
                          << ", opcode=" << wc.opcode << std::endl;
                // Still repost receive even on error
                if (dummy_recv_mr_) {
                    post_single_receive();
                }
                continue;
            }
            
            // Check if this completion has an immediate value
            if (wc.wc_flags & IBV_WC_WITH_IMM) {
                total_with_imm++;
                uint32_t imm = wc.imm_data;
                
                // Decode immediate value to get packet index
                auto [msg_id, packet_idx] = decode_immediate(imm);
                
                std::cout << "Receiver: Received completion with IMM: msg_id=" << msg_id 
                          << ", packet_idx=" << packet_idx << ", imm=0x" << std::hex 
                          << imm << std::dec << std::endl;
                
                // Verify message ID matches
                if (msg_id != current_msg_id_ - 1) {
                    std::cout << "Receiver: Warning - message ID mismatch: expected " 
                              << (current_msg_id_ - 1) << ", got " << msg_id << std::endl;
                    // Still repost receive
                    if (dummy_recv_mr_) {
                        post_single_receive();
                    }
                    continue;
                }
                
                // Verify packet index is valid
                if (packet_idx >= total_packets_) {
                    std::cout << "Receiver: Warning - invalid packet index: " 
                              << packet_idx << " (max: " << total_packets_ << ")" << std::endl;
                    // Still repost receive
                    if (dummy_recv_mr_) {
                        post_single_receive();
                    }
                    continue;
                }
                
                // Get the bitmap entry index (packet_idx / 16)
                // Each bitmap entry represents 16 packets
                size_t bitmap_idx = packet_idx / 16;
                
                std::cout << "[COMPLETION] Processing packet " << packet_idx 
                          << ", bitmap_idx=" << bitmap_idx << std::endl;
                
                // Safety checks - ensure packet_bitmap_ is valid and index is in range
                std::cout << "[COMPLETION] Checking packet_bitmap_..." << std::endl;
                if (packet_bitmap_.empty()) {
                    std::cerr << "Receiver: FATAL - packet_bitmap_ is empty!" << std::endl;
                    if (dummy_recv_mr_) {
                        post_single_receive();
                    }
                    continue;
                }
                
                std::cout << "[COMPLETION] packet_bitmap_.size()=" << packet_bitmap_.size() << std::endl;
                if (bitmap_idx >= packet_bitmap_.size()) {
                    std::cerr << "Receiver: FATAL - bitmap_idx " << bitmap_idx 
                              << " >= packet_bitmap_.size() " << packet_bitmap_.size() 
                              << " (packet_idx=" << packet_idx << ")" << std::endl;
                    if (dummy_recv_mr_) {
                        post_single_receive();
                    }
                    continue;
                }
                
                std::cout << "[COMPLETION] Accessing packet_bitmap_[" << bitmap_idx << "]..." << std::endl;
                // Set the bit atomically using fetch_or
                uint16_t bit_mask = 1U << (packet_idx % 16);
                std::cout << "[COMPLETION] bit_mask=0x" << std::hex << bit_mask << std::dec << std::endl;
                uint16_t old_val = packet_bitmap_[bitmap_idx].fetch_or(bit_mask, std::memory_order_release);
                std::cout << "[COMPLETION] Successfully updated bitmap, old_val=0x" 
                          << std::hex << old_val << std::dec << std::endl;
                
                if ((old_val & bit_mask) == 0) {
                    // This is a new packet
                    packets_received_.fetch_add(1, std::memory_order_relaxed);
                    std::cout << "Receiver: Marked packet " << packet_idx << " (bitmap[" 
                              << bitmap_idx << "] = 0x" << std::hex 
                              << (old_val | bit_mask) << std::dec << ")" << std::endl;
                }
                
                // Repost a receive to replace the one we just consumed
                if (dummy_recv_mr_) {
                    post_single_receive();
                }
            } else {
                std::cout << "Receiver: Completion without IMM: opcode=" << wc.opcode 
                          << ", byte_len=" << wc.byte_len << std::endl;
                
                // Repost a receive even for non-IMM completions
                if (dummy_recv_mr_) {
                    post_single_receive();
                }
            }
        }
        
        // Check if all packets have been received
        size_t received_count = packets_received_.load(std::memory_order_acquire);
        if (received_count >= total_packets_) {
            std::cout << "Receiver: All " << total_packets_ << " packets received!" << std::endl;
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
    
    std::cout << "Receiver: Completion thread exiting (total polled: " << total_polled 
              << ", with IMM: " << total_with_imm << ")" << std::endl;
}

void RDMAReceiver::frontend_poller() {
    // Use std::cout with flush to ensure output appears immediately
    std::cout << "[FRONTEND] Frontend poller thread started" << std::flush << std::endl;
    std::cout << "[FRONTEND] Thread ID: " << std::this_thread::get_id() << std::flush << std::endl;
    
    // Add a try-catch to catch any exceptions
    try {
        std::cout << "[FRONTEND] Entered try block" << std::flush << std::endl;
        
        // Wait a bit to ensure packet_bitmap_ is initialized
        // Use a single sleep call instead of a loop to simplify
        std::cout << "[FRONTEND] About to sleep for 10ms..." << std::flush << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::cout << "[FRONTEND] After initial sleep" << std::flush << std::endl;
        
        // Check if we can access member variables safely - do this one at a time
        std::cout << "[FRONTEND] Checking stop_thread_..." << std::flush << std::endl;
        bool stop = stop_thread_.load(std::memory_order_acquire);
        std::cout << "[FRONTEND] stop_thread_=" << stop << std::flush << std::endl;
        
        std::cout << "[FRONTEND] Checking total_packets_..." << std::flush << std::endl;
        size_t total = total_packets_;
        std::cout << "[FRONTEND] total_packets_=" << total << std::flush << std::endl;
        
        std::cout << "[FRONTEND] Checking packet_bitmap_..." << std::flush << std::endl;
        size_t bmp_size = packet_bitmap_.size();
        std::cout << "[FRONTEND] packet_bitmap_.size()=" << bmp_size << std::flush << std::endl;
        
        std::cout << "[FRONTEND] Checking config_.chunk_size..." << std::flush << std::endl;
        size_t chunk_size = config_.chunk_size;
        std::cout << "[FRONTEND] config_.chunk_size=" << chunk_size << std::flush << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "[FRONTEND] Exception in frontend_poller: " << e.what() << std::endl;
        return;
    } catch (...) {
        std::cerr << "[FRONTEND] Unknown exception in frontend_poller!" << std::endl;
        return;
    }
    
    std::cout << "[FRONTEND] Entering main loop..." << std::flush << std::endl;
    int iteration = 0;
    while (!stop_thread_.load(std::memory_order_acquire)) {
        iteration++;
        if (iteration % 1000 == 0) {
            std::cout << "[FRONTEND] Iteration " << iteration << std::endl;
        }
        
        // Safety check
        std::cout << "[FRONTEND] Checking packet_bitmap_ and total_packets_" << std::endl;
        if (packet_bitmap_.empty() || total_packets_ == 0) {
            std::cout << "[FRONTEND] packet_bitmap_ empty or total_packets_=0, sleeping" << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        
        std::cout << "[FRONTEND] packet_bitmap_.size()=" << packet_bitmap_.size() 
                  << ", total_packets_=" << total_packets_ << std::endl;
        
        // Poll the packet bitmap and update chunk bitmap
        // Check each packet bitmap entry
        std::cout << "[FRONTEND] Starting loop over packet_bitmap_ entries" << std::endl;
        for (size_t i = 0; i < packet_bitmap_.size(); ++i) {
            if (i == 0 || (i % 10 == 0)) {
                std::cout << "[FRONTEND] Processing bitmap entry " << i << std::endl;
            }
            
            // Read the packet bitmap entry atomically
            std::cout << "[FRONTEND] Loading packet_mask from bitmap[" << i << "]" << std::endl;
            uint16_t packet_mask = packet_bitmap_[i].load(std::memory_order_acquire);
            std::cout << "[FRONTEND] Loaded packet_mask=0x" << std::hex << packet_mask << std::dec << std::endl;
            
            // Check if the mask (0xFFFF & packet_bitmap_[i]) indicates all bits are set
            // This means all 16 packets in this bitmap entry are received
            if ((packet_mask & 0xFFFF) == 0xFFFF) {
                std::cout << "[FRONTEND] All bits set in bitmap[" << i << "], checking chunks" << std::endl;
                
                // Calculate which packets this bitmap entry covers
                size_t first_packet = i * 16;
                size_t last_packet = std::min(first_packet + 15, total_packets_ - 1);
                std::cout << "[FRONTEND] first_packet=" << first_packet << ", last_packet=" << last_packet << std::endl;
                
                // For each chunk that overlaps with these packets, check if it's complete
                // and mark it atomically if so
                size_t first_chunk = first_packet / config_.chunk_size;
                size_t last_chunk = last_packet / config_.chunk_size;
                std::cout << "[FRONTEND] first_chunk=" << first_chunk << ", last_chunk=" << last_chunk 
                          << ", total_chunks_=" << total_chunks_ << std::endl;
                
                for (size_t chunk_idx = first_chunk; chunk_idx <= last_chunk && chunk_idx < total_chunks_; ++chunk_idx) {
                    std::cout << "[FRONTEND] Processing chunk_idx=" << chunk_idx << std::endl;
                    
                    // Check if this chunk is already marked
                    uint64_t chunk_bit = 1ULL << chunk_idx;
                    std::cout << "[FRONTEND] chunk_bit=0x" << std::hex << chunk_bit << std::dec << std::endl;
                    
                    uint64_t current_chunk_bitmap = chunk_bitmap_.load(std::memory_order_acquire);
                    std::cout << "[FRONTEND] current_chunk_bitmap=0x" << std::hex << current_chunk_bitmap << std::dec << std::endl;
                    
                    if (current_chunk_bitmap & chunk_bit) {
                        std::cout << "[FRONTEND] Chunk " << chunk_idx << " already marked, skipping" << std::endl;
                        continue; // Already marked
                    }
                    
                    // Check if all packets in this chunk are received
                    bool chunk_complete = true;
                    size_t chunk_start_packet = chunk_idx * config_.chunk_size;
                    size_t chunk_end_packet = std::min(chunk_start_packet + config_.chunk_size - 1, 
                                                       total_packets_ - 1);
                    std::cout << "[FRONTEND] Checking packets in chunk: start=" << chunk_start_packet 
                              << ", end=" << chunk_end_packet << std::endl;
                    
                    for (size_t p = chunk_start_packet; p <= chunk_end_packet; ++p) {
                        std::cout << "[FRONTEND] Checking packet " << p << std::endl;
                        size_t bmp_idx = p / 16;
                        size_t bit_pos = p % 16;
                        uint16_t bit_mask = 1U << bit_pos;
                        
                        std::cout << "[FRONTEND] packet " << p << ": bmp_idx=" << bmp_idx 
                                  << ", bit_pos=" << bit_pos << ", bit_mask=0x" << std::hex 
                                  << bit_mask << std::dec << std::endl;
                        
                        // Safety check
                        if (bmp_idx >= packet_bitmap_.size()) {
                            std::cout << "[FRONTEND] ERROR: bmp_idx " << bmp_idx 
                                      << " >= packet_bitmap_.size() " << packet_bitmap_.size() << std::endl;
                            chunk_complete = false;
                            break;
                        }
                        
                        std::cout << "[FRONTEND] Loading bitmap[" << bmp_idx << "]" << std::endl;
                        uint16_t bmp_val = packet_bitmap_[bmp_idx].load(std::memory_order_acquire);
                        std::cout << "[FRONTEND] bitmap[" << bmp_idx << "]=0x" << std::hex 
                                  << bmp_val << std::dec << std::endl;
                        
                        if ((bmp_val & bit_mask) == 0) {
                            std::cout << "[FRONTEND] Packet " << p << " not received yet" << std::endl;
                            chunk_complete = false;
                            break;
                        }
                        std::cout << "[FRONTEND] Packet " << p << " is received" << std::endl;
                    }
                    
                    // If chunk is complete, mark it atomically
                    if (chunk_complete) {
                        std::cout << "[FRONTEND] Chunk " << chunk_idx << " is complete, marking it" << std::endl;
                        chunk_bitmap_.fetch_or(chunk_bit, std::memory_order_release);
                        std::cout << "[FRONTEND] Chunk " << chunk_idx << " marked successfully" << std::endl;
                    } else {
                        std::cout << "[FRONTEND] Chunk " << chunk_idx << " is not complete yet" << std::endl;
                    }
                }
            } else {
                if (i == 0 || (i % 10 == 0)) {
                    std::cout << "[FRONTEND] Bitmap[" << i << "] not all bits set (mask=0x" 
                              << std::hex << packet_mask << std::dec << ")" << std::endl;
                }
            }
        }
        
        std::cout << "[FRONTEND] Finished loop over packet_bitmap_ entries" << std::endl;
        
        // Check if we should exit
        if (reception_complete_.load(std::memory_order_acquire)) {
            std::cout << "[FRONTEND] Reception complete, exiting" << std::endl;
            break;
        }
        
        // Small sleep to avoid busy-waiting
        std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
    
    std::cout << "[FRONTEND] Frontend poller thread exiting" << std::endl;
}

bool RDMAReceiver::is_complete() const {
    // Check if all chunks are complete by checking chunk bitmap
    uint64_t expected_mask = (total_chunks_ == 64) ? UINT64_MAX : ((1ULL << total_chunks_) - 1);
    uint64_t current_mask = chunk_bitmap_.load(std::memory_order_acquire);
    return (current_mask & expected_mask) == expected_mask;
}

} // namespace RDMA_EC
