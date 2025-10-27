#include "rdma_reed_solomon.h"
#include <iostream>
#include <cstring>
#include <algorithm>
#include <random>
#include <numeric>

namespace RDMA_EC {

// Simple Reed-Solomon implementation using XOR-based parity
// For production, use a proper library like libfec or Intel ISA-L
class SimpleReedSolomon {
public:
    SimpleReedSolomon(int k, int m) : k_(k), m_(m) {}
    
    std::vector<std::vector<uint8_t>> encode(const std::vector<uint8_t>& data) const {
        std::vector<std::vector<uint8_t>> packets(k_ + m_);
        
        // Split data into k packets
        size_t packet_size = data.size() / k_;
        for (int i = 0; i < k_; ++i) {
            size_t start = i * packet_size;
            size_t end = (i == k_ - 1) ? data.size() : (i + 1) * packet_size;
            packets[i].assign(data.begin() + start, data.begin() + end);
        }
        
        // Generate parity packets using XOR
        for (int p = 0; p < m_; ++p) {
            packets[k_ + p].resize(packet_size, 0);
            
            // XOR all data packets
            for (int i = 0; i < k_; ++i) {
                for (size_t j = 0; j < packets[k_ + p].size() && j < packets[i].size(); ++j) {
                    packets[k_ + p][j] ^= packets[i][j];
                }
            }
        }
        
        return packets;
    }
    
    std::vector<uint8_t> decode(const std::vector<std::vector<uint8_t>>& packets, 
                               const std::vector<bool>& received) const {
        if (packets.size() != static_cast<size_t>(k_ + m_) || received.size() != static_cast<size_t>(k_ + m_)) {
            return {};
        }
        
        // Count received packets
        int received_count = 0;
        for (bool r : received) {
            if (r) received_count++;
        }
        
        // Need at least k packets to decode
        if (received_count < k_) {
            return {};
        }
        
        // For this simple implementation, we'll just concatenate the first k received packets
        std::vector<uint8_t> result;
        int data_packets_found = 0;
        
        for (int i = 0; i < k_ && data_packets_found < k_; ++i) {
            if (received[i] && static_cast<size_t>(i) < packets.size()) {
                result.insert(result.end(), packets[i].begin(), packets[i].end());
                data_packets_found++;
            }
        }
        
        return result;
    }
    
private:
    int k_, m_;
};

// ReedSolomon implementation
ReedSolomon::ReedSolomon(int k, int m) : k_(k), m_(m) {
    rs_encoder_ = new SimpleReedSolomon(k, m);
}

ReedSolomon::~ReedSolomon() {
    delete static_cast<SimpleReedSolomon*>(rs_encoder_);
}

std::vector<std::vector<uint8_t>> ReedSolomon::encode(const std::vector<uint8_t>& data) const {
    return static_cast<SimpleReedSolomon*>(rs_encoder_)->encode(data);
}

std::vector<uint8_t> ReedSolomon::decode(const std::vector<std::vector<uint8_t>>& packets, 
                                        const std::vector<bool>& received) const {
    return static_cast<SimpleReedSolomon*>(rs_encoder_)->decode(packets, received);
}

// RDMAPacket implementation
RDMAPacket::RDMAPacket(uint32_t seq, PacketType type, const std::vector<uint8_t>& payload) 
    : data(payload) {
    header.magic = RDMAHeader::MAGIC_NUMBER;
    header.version = RDMAHeader::PROTOCOL_VERSION;
    header.flags = 0;
    header.sequence = seq;
    header.type = type;
    header.data_size = payload.size();
    header.remote_addr = 0;  // Will be set by sender
    header.rkey = 0;         // Will be set by sender
    header.checksum = 0;     // Will be calculated
    header.checksum = calculate_checksum();
}

std::vector<uint8_t> RDMAPacket::serialize() const {
    std::vector<uint8_t> result;
    result.reserve(sizeof(RDMAHeader) + data.size());
    
    // Serialize header
    const uint8_t* header_ptr = reinterpret_cast<const uint8_t*>(&header);
    result.insert(result.end(), header_ptr, header_ptr + sizeof(RDMAHeader));
    
    // Serialize data
    result.insert(result.end(), data.begin(), data.end());
    
    return result;
}

std::unique_ptr<RDMAPacket> RDMAPacket::deserialize(const uint8_t* data, size_t size) {
    if (size < sizeof(RDMAHeader)) {
        return nullptr;
    }
    
    auto packet = std::make_unique<RDMAPacket>();
    
    // Deserialize header
    std::memcpy(&packet->header, data, sizeof(RDMAHeader));
    
    // Validate magic number
    if (packet->header.magic != RDMAHeader::MAGIC_NUMBER) {
        return nullptr;
    }
    
    // Deserialize data
    size_t data_size = size - sizeof(RDMAHeader);
    if (data_size != packet->header.data_size) {
        return nullptr;
    }
    
    packet->data.assign(data + sizeof(RDMAHeader), data + size);
    
    // Validate checksum
    if (!packet->is_valid()) {
        return nullptr;
    }
    
    return packet;
}

uint32_t RDMAPacket::calculate_checksum() const {
    return Utils::calculate_checksum(data.data(), data.size());
}

bool RDMAPacket::is_valid() const {
    return header.magic == RDMAHeader::MAGIC_NUMBER &&
           header.version == RDMAHeader::PROTOCOL_VERSION &&
           header.checksum == calculate_checksum();
}

// RDMASender implementation
RDMASender::RDMASender(std::shared_ptr<rdmapp::acceptor> acceptor, const Config& config) 
    : acceptor_(acceptor), config_(config), rs_(config.k, config.m) {
    setup_memory_regions();
}

RDMASender::RDMASender(std::shared_ptr<rdmapp::qp> qp, const Config& config) 
    : config_(config), rs_(config.k, config.m), current_qp_(qp) {
    setup_memory_regions();
}

RDMASender::~RDMASender() {
    cleanup_memory_regions();
}

bool RDMASender::setup_memory_regions() {
    packet_mrs_.clear();
    packet_data_.clear();
    
    // Create memory regions for each packet (k data + m parity)
    int total_packets = config_.total_packets();
    packet_mrs_.reserve(total_packets);
    packet_data_.resize(total_packets);
    
    for (int i = 0; i < total_packets; ++i) {
        packet_data_[i].resize(config_.packet_size);
        // Memory regions will be registered when we have a QP
    }
    
    return true;
}

void RDMASender::cleanup_memory_regions() {
    packet_mrs_.clear();
    packet_data_.clear();
}

rdmapp::task<bool> RDMASender::send_data(const std::vector<uint8_t>& data) {
    try {
        // Encode data using Reed-Solomon
        auto encoded_packets = rs_.encode(data);
        
        if (encoded_packets.size() != static_cast<size_t>(config_.total_packets())) {
            std::cerr << "Error: Encoded packets size mismatch" << std::endl;
            co_return false;
        }
        
        // Copy encoded data to our packet buffers
        for (size_t i = 0; i < encoded_packets.size(); ++i) {
            if (encoded_packets[i].size() <= static_cast<size_t>(config_.packet_size)) {
                std::copy(encoded_packets[i].begin(), encoded_packets[i].end(), 
                         packet_data_[i].begin());
            }
        }
        
        // Register memory regions if we have a QP
        if (current_qp_) {
            packet_mrs_.clear();
            for (int i = 0; i < config_.total_packets(); ++i) {
                auto mr = std::make_shared<rdmapp::local_mr>(
                    current_qp_->pd_ptr()->reg_mr(packet_data_[i].data(), config_.packet_size));
                packet_mrs_.push_back(mr);
            }
        }
        
        // Send metadata for each packet
        for (int i = 0; i < config_.total_packets(); ++i) {
            PacketType type = (i < config_.k) ? PacketType::DATA : PacketType::PARITY;
            co_await send_packet_metadata(i, type, packet_data_[i]);
        }
        
        stats_.packets_sent += config_.total_packets();
        stats_.bytes_sent += data.size();
        
        co_return true;
    } catch (const std::exception& e) {
        std::cerr << "Error sending data: " << e.what() << std::endl;
        co_return false;
    }
}

rdmapp::task<void> RDMASender::send_packet_metadata(uint32_t seq, PacketType type, 
                                                   const std::vector<uint8_t>& data) {
    if (!current_qp_) {
        throw std::runtime_error("No active connection");
    }
    
    RDMAPacket packet(seq, type, data);
    
    // Set RDMA information if we have memory regions
    if (seq < packet_mrs_.size()) {
        packet.header.remote_addr = reinterpret_cast<uint64_t>(packet_mrs_[seq]->addr());
        packet.header.rkey = packet_mrs_[seq]->rkey();
    }
    
    auto serialized = packet.serialize();
    co_await current_qp_->send(serialized.data(), serialized.size());
    
    stats_.rdma_operations++;
}

rdmapp::task<bool> RDMASender::send_packet(const RDMAPacket& packet) {
    if (!current_qp_) {
        co_return false;
    }
    
    auto serialized = packet.serialize();
    co_await current_qp_->send(serialized.data(), serialized.size());
    
    stats_.packets_sent++;
    stats_.bytes_sent += packet.data.size();
    stats_.rdma_operations++;
    
    co_return true;
}

rdmapp::task<void> RDMASender::handle_control_packet(const RDMAPacket& packet) {
    if (packet.header.type != PacketType::CONTROL) {
        co_return;
    }
    
    // Parse control packet
    if (packet.data.size() < 1) {
        co_return;
    }
    
    ControlType control_type = static_cast<ControlType>(packet.data[0]);
    
    switch (control_type) {
        case ControlType::ACK:
            stats_.acks_received++;
            break;
        case ControlType::NACK:
            stats_.nacks_received++;
            // TODO: Implement retransmission logic
            break;
        case ControlType::COMPLETE:
            // Data transfer complete
            break;
        default:
            break;
    }
}

// RDMAReceiver implementation
RDMAReceiver::RDMAReceiver(std::shared_ptr<rdmapp::connector> connector, const Config& config) 
    : connector_(connector), config_(config), rs_(config.k, config.m) {
    setup_memory_regions();
    reset_tracking();
}

RDMAReceiver::RDMAReceiver(std::shared_ptr<rdmapp::qp> qp, const Config& config) 
    : config_(config), rs_(config.k, config.m), current_qp_(qp) {
    setup_memory_regions();
    reset_tracking();
}

RDMAReceiver::~RDMAReceiver() {
    cleanup_memory_regions();
}

bool RDMAReceiver::setup_memory_regions() {
    receive_mrs_.clear();
    receive_buffers_.clear();
    
    // Create memory regions for receiving data
    int total_packets = config_.total_packets();
    receive_mrs_.reserve(total_packets);
    receive_buffers_.resize(total_packets);
    
    for (int i = 0; i < total_packets; ++i) {
        receive_buffers_[i].resize(config_.packet_size);
        // Memory regions will be registered when we have a QP
    }
    
    return true;
}

void RDMAReceiver::cleanup_memory_regions() {
    receive_mrs_.clear();
    receive_buffers_.clear();
}

void RDMAReceiver::reset_tracking() {
    received_bitmap_.assign(config_.total_packets(), false);
    packet_buffer_.assign(config_.total_packets(), std::vector<uint8_t>());
    current_sequence_ = 0;
    decoding_in_progress_ = false;
}

rdmapp::task<std::vector<uint8_t>> RDMAReceiver::receive_data() {
    try {
        reset_tracking();
        
        // Register memory regions if we have a QP
        if (current_qp_) {
            receive_mrs_.clear();
            for (int i = 0; i < config_.total_packets(); ++i) {
                auto mr = std::make_shared<rdmapp::local_mr>(
                    current_qp_->pd_ptr()->reg_mr(receive_buffers_[i].data(), config_.packet_size));
                receive_mrs_.push_back(mr);
            }
        }
        
        // Receive metadata for all packets
        for (int i = 0; i < config_.total_packets(); ++i) {
            auto packet = co_await receive_packet();
            if (!packet) {
                std::cerr << "Failed to receive packet " << i << std::endl;
                co_return {};
            }
            
            // Process packet metadata
            if (packet->header.type == PacketType::DATA || packet->header.type == PacketType::PARITY) {
                uint32_t seq = packet->header.sequence;
                if (seq < static_cast<uint32_t>(config_.total_packets())) {
                    // Fetch actual data using RDMA Read
                    co_await fetch_packet_data(seq, packet->header.remote_addr, packet->header.rkey);
                    received_bitmap_[seq] = true;
                    stats_.packets_received++;
                    stats_.bytes_received += packet->header.data_size;
                }
            }
        }
        
        // Try to decode the data
        auto decoded_data = try_decode();
        if (!decoded_data.empty()) {
            stats_.packets_decoded++;
            co_return decoded_data;
        }
        
        co_return {};
    } catch (const std::exception& e) {
        std::cerr << "Error receiving data: " << e.what() << std::endl;
        co_return {};
    }
}

rdmapp::task<std::unique_ptr<RDMAPacket>> RDMAReceiver::receive_packet() {
    if (!current_qp_) {
        co_return nullptr;
    }
    
    // Receive header first
    uint8_t header_buffer[sizeof(RDMAHeader)];
    auto [n, _] = co_await current_qp_->recv(header_buffer, sizeof(header_buffer));
    
    if (n != sizeof(RDMAHeader)) {
        co_return nullptr;
    }
    
    // Parse header to get data size
    RDMAHeader header;
    std::memcpy(&header, header_buffer, sizeof(header));
    
    if (header.magic != RDMAHeader::MAGIC_NUMBER) {
        co_return nullptr;
    }
    
    // Receive data
    std::vector<uint8_t> data_buffer(header.data_size);
    if (header.data_size > 0) {
        co_await current_qp_->recv(data_buffer.data(), header.data_size);
    }
    
    // Combine header and data
    std::vector<uint8_t> full_packet(sizeof(header) + data_buffer.size());
    std::memcpy(full_packet.data(), &header, sizeof(header));
    std::memcpy(full_packet.data() + sizeof(header), data_buffer.data(), data_buffer.size());
    
    auto packet = RDMAPacket::deserialize(full_packet.data(), full_packet.size());
    if (packet) {
        stats_.rdma_operations++;
    }
    
    co_return packet;
}

rdmapp::task<void> RDMAReceiver::fetch_packet_data(uint32_t seq, uint64_t remote_addr, uint32_t rkey) {
    if (!current_qp_ || seq >= receive_mrs_.size()) {
        co_return;
    }
    
    // Use RDMA Read to fetch the actual data
    rdmapp::remote_mr remote_mr{reinterpret_cast<void*>(remote_addr), static_cast<uint32_t>(config_.packet_size), rkey};
    co_await current_qp_->read(remote_mr, receive_buffers_[seq].data(), config_.packet_size);
    
    // Copy to packet buffer
    packet_buffer_[seq] = receive_buffers_[seq];
    
    stats_.rdma_operations++;
}

std::vector<uint8_t> RDMAReceiver::try_decode() {
    // Count received packets
    int received_count = 0;
    for (bool r : received_bitmap_) {
        if (r) received_count++;
    }
    
    // Need at least k packets to decode
    if (received_count < config_.k) {
        return {};
    }
    
    // Try to decode using Reed-Solomon
    auto decoded = rs_.decode(packet_buffer_, received_bitmap_);
    
    if (!decoded.empty()) {
        stats_.packets_decoded++;
    }
    
    return decoded;
}

rdmapp::task<bool> RDMAReceiver::send_control_packet(ControlType type, const std::vector<uint32_t>& sequences) {
    if (!current_qp_) {
        co_return false;
    }
    
    // Create control packet
    std::vector<uint8_t> control_data;
    control_data.push_back(static_cast<uint8_t>(type));
    
    // Add sequence numbers
    for (uint32_t seq : sequences) {
        control_data.push_back((seq >> 24) & 0xFF);
        control_data.push_back((seq >> 16) & 0xFF);
        control_data.push_back((seq >> 8) & 0xFF);
        control_data.push_back(seq & 0xFF);
    }
    
    RDMAPacket packet(0, PacketType::CONTROL, control_data);
    auto serialized = packet.serialize();
    co_await current_qp_->send(serialized.data(), serialized.size());
    
    if (type == ControlType::ACK) {
        stats_.acks_sent++;
    } else if (type == ControlType::NACK) {
        stats_.nacks_sent++;
    }
    
    stats_.rdma_operations++;
    co_return true;
}

rdmapp::task<void> RDMAReceiver::send_nack_for_missing() {
    std::vector<uint32_t> missing_sequences;
    
    for (size_t i = 0; i < received_bitmap_.size(); ++i) {
        if (!received_bitmap_[i]) {
            missing_sequences.push_back(i);
        }
    }
    
    if (!missing_sequences.empty()) {
        co_await send_control_packet(ControlType::NACK, missing_sequences);
    }
}

// Utility functions
namespace Utils {
    bool should_drop_packet(double loss_rate) {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_real_distribution<> dis(0.0, 1.0);
        return dis(gen) < loss_rate;
    }
    
    std::vector<uint8_t> generate_test_data(size_t size) {
        std::vector<uint8_t> data(size);
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_int_distribution<uint8_t> dis(0, 255);
        
        for (size_t i = 0; i < size; ++i) {
            data[i] = dis(gen);
        }
        
        return data;
    }
    
    uint32_t simple_hash(const uint8_t* data, size_t size) {
        uint32_t hash = 0;
        for (size_t i = 0; i < size; ++i) {
            hash = hash * 31 + data[i];
        }
        return hash;
    }
    
    uint32_t calculate_checksum(const uint8_t* data, size_t size) {
        uint32_t checksum = 0;
        for (size_t i = 0; i < size; ++i) {
            checksum += data[i];
        }
        return checksum;
    }
}

} // namespace RDMA_EC
