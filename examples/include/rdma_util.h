#pragma once

#include <cstdint>
#include <string>
#include <stdexcept>
#include <unordered_set>
#include <vector>
#include <infiniband/verbs.h>

namespace RDMA_EC {

// Default configuration values
constexpr size_t DEFAULT_MTU = 1024;
constexpr size_t DEFAULT_CHUNK_SIZE = 16;  // packets per chunk
constexpr size_t DEFAULT_BUFFER_SIZE = 1024 * 1024;  // 1MB
constexpr int DEFAULT_RECEIVER_TIMEOUT_SECONDS = 10;
constexpr bool DEFAULT_ENABLE_LOGGING = true;
constexpr const char *DEFAULT_LOGGING_LEVEL = "info";
constexpr size_t DEFAULT_MAX_IN_FLIGHT_REQUESTS = 1024;
constexpr bool DEFAULT_POST_PER_COMPLETION = false;
constexpr size_t DEFAULT_RX_DEPTH = 2048;
extern enum ibv_qp_type DEFAULT_RDMA_TRANSPORT;

// Configuration for RDMA transport
class Config {
public:
    size_t mtu = DEFAULT_MTU;
    size_t chunk_size = DEFAULT_CHUNK_SIZE;
    size_t buffer_size = DEFAULT_BUFFER_SIZE;
    int cpu_core_id = 2;  // -1 means no CPU pinning
    int receiver_timeout_seconds = DEFAULT_RECEIVER_TIMEOUT_SECONDS;
    enum ibv_qp_type transport_type = DEFAULT_RDMA_TRANSPORT;
    bool enable_logging = DEFAULT_ENABLE_LOGGING;
    std::string logging_level = DEFAULT_LOGGING_LEVEL;
    size_t max_in_flight_requests = DEFAULT_MAX_IN_FLIGHT_REQUESTS;
    bool post_per_completion = DEFAULT_POST_PER_COMPLETION; // If true, post a new receive immediately for each consumed WR
    size_t rx_depth = DEFAULT_RX_DEPTH;
    size_t num_concurrent_chunks = 1;
    size_t cq_batch_size = 32;
    bool enable_batched_sends = false;
    bool enable_batch_recvs = false; // when true, use batched recv posting and await tail
    bool enable_inline_sends = false;

    // Track which keys were present in the loaded file for validation
    std::unordered_set<std::string> seen_keys;


    bool load_from_file(const std::string& filepath);

    bool save_to_file(const std::string& filepath) const;

    // Print common config keys; derived classes can override to add/remove role-specific fields
    virtual void print() const;

    // Validate presence of required common keys.
    virtual void validate_common() const;
    // Utility: check a list of required keys against seen_keys
    void validate_required_keys(const std::vector<const char*> &keys, const char* ctx) const;

    // Source of truth for required common keys
    std::vector<const char*> required_common_keys() const;

private:
    std::string trim(const std::string& str) const;
    bool parse_line(const std::string& line);
};

// Dedicated configurations for role-specific overrides or future divergence.
// Currently inherit behavior from Config, but allow separate files and defaults
// for sender vs receiver without changing existing code paths.
class SenderConfig : public Config {
public:
    // Future: sender-specific defaults or fields can be placed here.
    // Inherits load/save/print from Config, so you can keep a distinct
    // sender config file with the same keys.
    void validate() const; // validates common + sender-specific requirements

    // Print common + sender-specific (e.g., buffer_size) keys
    void print() const override;

    // Sender-specific required keys
    std::vector<const char*> required_role_keys() const;
};

class ReceiverConfig : public Config {
public:
    // Future: receiver-specific defaults or fields can be placed here.
    // Inherits load/save/print from Config, enabling a separate receiver
    // config file with the same keys.
    void validate() const; // validates common + receiver-specific requirements

    // Print common keys and any receiver-specific ones (exclude buffer_size)
    void print() const override;

    // Receiver-specific required keys (none beyond common for now)
    std::vector<const char*> required_role_keys() const;
};

// Clear-To-Send message structure
struct CTSInfo {
    uint64_t remote_addr;
    uint32_t rkey;
    size_t buffer_size;
    size_t total_packets;
    uint8_t msg_id;
};

// Utility functions for immediate value encoding/decoding
// msg_id: 8 bits (upper 8 bits of uint32_t)
// packet_idx: 24 bits (lower 24 bits of uint32_t)
// Maximum packet_idx value: 2^24 - 1 = 16,777,215
inline uint32_t encode_immediate(uint8_t msg_id, uint32_t packet_idx) {
    return (static_cast<uint32_t>(msg_id) << 24) | (packet_idx & 0xFFFFFF);
}

inline std::pair<uint8_t, uint32_t> decode_immediate(uint32_t imm) {
    uint8_t msg_id = (imm >> 24) & 0xFF;
    uint32_t packet_idx = imm & 0xFFFFFF;
    return {msg_id, packet_idx};
}

// Calculate number of packets needed for a given size
inline size_t calculate_num_packets(size_t data_size, size_t mtu) {
    return (data_size + mtu - 1) / mtu;
}

// Calculate number of chunks for given packets
inline size_t calculate_num_chunks(size_t num_packets, size_t chunk_size) {
    return (num_packets + chunk_size - 1) / chunk_size;
}

} // namespace RDMA_EC
