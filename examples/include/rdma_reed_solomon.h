#pragma once

#include <rdmapp/rdmapp.h>
#include "acceptor.h"
#include "connector.h"

#include <cstdint>
#include <vector>
#include <memory>
#include <string>
#include <chrono>
#include <unordered_map>
#include <bitset>
#include <atomic>

namespace RDMA_EC {

// Erasure coding configuration
struct Config {
    int k = 8;                    // Number of data packets
    int m = 2;                    // Number of parity packets
    int packet_size = 1024;       // Size of each packet (bytes)
    int timeout_ms = 1000;        // Retransmission timeout
    int max_retries = 3;          // Maximum retry attempts
    bool enable_nack = true;      // Enable NACK-based retransmission
    
    int total_packets() const { return k + m; }
    int data_size() const { return k * packet_size; }
};

// Packet types for RDMA communication
enum class PacketType : uint8_t {
    DATA = 0x01,
    PARITY = 0x02,
    CONTROL = 0x03,
    METADATA = 0x04
};

// Control packet subtypes
enum class ControlType : uint8_t {
    ACK = 0x01,
    NACK = 0x02,
    COMPLETE = 0x03,
    READY = 0x04
};

// RDMA packet header structure
struct RDMAHeader {
    uint16_t magic;           // Magic number: 0xEC02
    uint8_t version;          // Protocol version
    uint8_t flags;            // Reserved flags
    uint32_t sequence;        // Packet sequence number
    PacketType type;          // Packet type
    uint32_t data_size;       // Size of data in this packet
    uint64_t remote_addr;     // Remote memory address for RDMA operations
    uint32_t rkey;            // Remote key for RDMA operations
    uint32_t checksum;        // Simple checksum
    
    static constexpr uint16_t MAGIC_NUMBER = 0xEC02;
    static constexpr uint8_t PROTOCOL_VERSION = 1;
};

// Complete RDMA packet structure
struct RDMAPacket {
    RDMAHeader header;
    std::vector<uint8_t> data;
    
    RDMAPacket() = default;
    RDMAPacket(uint32_t seq, PacketType type, const std::vector<uint8_t>& payload);
    
    // Serialize packet to bytes
    std::vector<uint8_t> serialize() const;
    
    // Deserialize packet from bytes
    static std::unique_ptr<RDMAPacket> deserialize(const uint8_t* data, size_t size);
    
    // Calculate simple checksum
    uint32_t calculate_checksum() const;
    
    // Validate packet
    bool is_valid() const;
};

// Reed-Solomon encoder/decoder (reused from UDP version)
class ReedSolomon {
public:
    ReedSolomon(int k, int m);
    ~ReedSolomon();
    
    // Encode data into k data packets + m parity packets
    std::vector<std::vector<uint8_t>> encode(const std::vector<uint8_t>& data) const;
    
    // Decode data from received packets (with bitmap indicating which packets arrived)
    std::vector<uint8_t> decode(const std::vector<std::vector<uint8_t>>& packets, 
                               const std::vector<bool>& received) const;
    
    int get_k() const { return k_; }
    int get_m() const { return m_; }
    int get_total() const { return k_ + m_; }

private:
    int k_, m_;
    void* rs_encoder_;  // Placeholder for actual RS implementation
};

// RDMA Reed Solomon Sender
class RDMASender {
public:
    RDMASender(std::shared_ptr<rdmapp::acceptor> acceptor, const Config& config = Config{});
    RDMASender(std::shared_ptr<rdmapp::qp> qp, const Config& config = Config{});
    ~RDMASender();
    
    // Send data with erasure coding over RDMA
    rdmapp::task<bool> send_data(const std::vector<uint8_t>& data);
    
    // Send individual packet
    rdmapp::task<bool> send_packet(const RDMAPacket& packet);
    
    // Handle incoming control packets (ACKs/NACKs)
    rdmapp::task<void> handle_control_packet(const RDMAPacket& packet);
    
    // Get statistics
    struct Stats {
        std::atomic<uint64_t> packets_sent{0};
        std::atomic<uint64_t> bytes_sent{0};
        std::atomic<uint64_t> retransmissions{0};
        std::atomic<uint64_t> acks_received{0};
        std::atomic<uint64_t> nacks_received{0};
        std::atomic<uint64_t> rdma_operations{0};
    };
    
    const Stats& get_stats() const { return stats_; }

private:
    std::shared_ptr<rdmapp::acceptor> acceptor_;
    Config config_;
    ReedSolomon rs_;
    Stats stats_;
    
    // Memory regions for each packet
    std::vector<std::shared_ptr<rdmapp::local_mr>> packet_mrs_;
    std::vector<std::vector<uint8_t>> packet_data_;
    
    // Current connection
    std::shared_ptr<rdmapp::qp> current_qp_;
    
    bool setup_memory_regions();
    void cleanup_memory_regions();
    rdmapp::task<void> send_packet_metadata(uint32_t seq, PacketType type, 
                                          const std::vector<uint8_t>& data);
    rdmapp::task<void> send_packet_data(uint32_t seq, const std::vector<uint8_t>& data);
};

// RDMA Reed Solomon Receiver
class RDMAReceiver {
public:
    RDMAReceiver(std::shared_ptr<rdmapp::connector> connector, const Config& config = Config{});
    RDMAReceiver(std::shared_ptr<rdmapp::qp> qp, const Config& config = Config{});
    ~RDMAReceiver();
    
    // Receive data with erasure coding over RDMA
    rdmapp::task<std::vector<uint8_t>> receive_data();
    
    // Receive single packet
    rdmapp::task<std::unique_ptr<RDMAPacket>> receive_packet();
    
    // Send control packet (ACK/NACK)
    rdmapp::task<bool> send_control_packet(ControlType type, const std::vector<uint32_t>& sequences);
    
    // Get statistics
    struct Stats {
        std::atomic<uint64_t> packets_received{0};
        std::atomic<uint64_t> bytes_received{0};
        std::atomic<uint64_t> packets_decoded{0};
        std::atomic<uint64_t> packets_lost{0};
        std::atomic<uint64_t> acks_sent{0};
        std::atomic<uint64_t> nacks_sent{0};
        std::atomic<uint64_t> rdma_operations{0};
    };
    
    const Stats& get_stats() const { return stats_; }

private:
    std::shared_ptr<rdmapp::connector> connector_;
    Config config_;
    ReedSolomon rs_;
    Stats stats_;
    
    // Current connection
    std::shared_ptr<rdmapp::qp> current_qp_;
    
    // Packet tracking
    std::vector<bool> received_bitmap_;
    std::vector<std::vector<uint8_t>> packet_buffer_;
    uint32_t current_sequence_;
    bool decoding_in_progress_;
    
    // Memory regions for receiving data
    std::vector<std::shared_ptr<rdmapp::local_mr>> receive_mrs_;
    std::vector<std::vector<uint8_t>> receive_buffers_;
    
    bool setup_memory_regions();
    void cleanup_memory_regions();
    void reset_tracking();
    std::vector<uint8_t> try_decode();
    rdmapp::task<void> send_nack_for_missing();
    rdmapp::task<void> fetch_packet_data(uint32_t seq, uint64_t remote_addr, uint32_t rkey);
};

// Utility functions
namespace Utils {
    // Simulate packet loss for testing
    bool should_drop_packet(double loss_rate = 0.1);
    
    // Generate random data for testing
    std::vector<uint8_t> generate_test_data(size_t size);
    
    // Calculate simple hash for data integrity
    uint32_t simple_hash(const uint8_t* data, size_t size);
    
    // Calculate packet checksum
    uint32_t calculate_checksum(const uint8_t* data, size_t size);
}

} // namespace RDMA_EC
