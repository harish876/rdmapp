#include "rdma_reed_solomon.h"
#include "acceptor.h"
#include "connector.h"
#include <iostream>
#include <chrono>
#include <thread>
#include <signal.h>
#include <atomic>
#include <numeric>
#include <algorithm>

using namespace RDMA_EC;

std::atomic<bool> running{true};

void signal_handler(int signal) {
    if (signal == SIGINT) {
        std::cout << "\nShutting down..." << std::endl;
        running = false;
    }
}

rdmapp::task<void> server_task(std::shared_ptr<rdmapp::acceptor> acceptor, const Config& config) {
    std::cout << "RDMA Reed Solomon Server started" << std::endl;
    std::cout << "Configuration: k=" << config.k << ", m=" << config.m 
              << ", packet_size=" << config.packet_size << std::endl;
    
    auto sender = std::make_unique<RDMASender>(acceptor, config);
    
    while (running) {
        try {
            // Generate test data
            auto test_data = Utils::generate_test_data(config.data_size());
            std::cout << "Generated test data: " << test_data.size() << " bytes" << std::endl;
            
            // Send data with erasure coding
            auto start_time = std::chrono::high_resolution_clock::now();
            bool success = co_await sender->send_data(test_data);
            auto end_time = std::chrono::high_resolution_clock::now();
            
            if (success) {
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
                std::cout << "Data sent successfully in " << duration.count() << " microseconds" << std::endl;
                
                // Print statistics
                const auto& stats = sender->get_stats();
                std::cout << "Stats - Packets sent: " << stats.packets_sent.load()
                          << ", Bytes sent: " << stats.bytes_sent.load()
                          << ", RDMA operations: " << stats.rdma_operations.load() << std::endl;
            } else {
                std::cerr << "Failed to send data" << std::endl;
            }
            
            // Wait before next transmission
            std::this_thread::sleep_for(std::chrono::seconds(2));
            
        } catch (const std::exception& e) {
            std::cerr << "Server error: " << e.what() << std::endl;
            break;
        }
    }
    
    co_return;
}

rdmapp::task<void> client_task(std::shared_ptr<rdmapp::connector> connector, const Config& config) {
    std::cout << "RDMA Reed Solomon Client started" << std::endl;
    std::cout << "Configuration: k=" << config.k << ", m=" << config.m 
              << ", packet_size=" << config.packet_size << std::endl;
    
    auto receiver = std::make_unique<RDMAReceiver>(connector, config);
    
    while (running) {
        try {
            // Receive data with erasure coding
            auto start_time = std::chrono::high_resolution_clock::now();
            auto received_data = co_await receiver->receive_data();
            auto end_time = std::chrono::high_resolution_clock::now();
            
            if (!received_data.empty()) {
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
                std::cout << "Data received successfully in " << duration.count() << " microseconds" << std::endl;
                std::cout << "Received data size: " << received_data.size() << " bytes" << std::endl;
                
                // Verify data integrity
                uint32_t hash = Utils::simple_hash(received_data.data(), received_data.size());
                std::cout << "Data hash: 0x" << std::hex << hash << std::dec << std::endl;
                
                // Print statistics
                const auto& stats = receiver->get_stats();
                std::cout << "Stats - Packets received: " << stats.packets_received.load()
                          << ", Bytes received: " << stats.bytes_received.load()
                          << ", Packets decoded: " << stats.packets_decoded.load()
                          << ", RDMA operations: " << stats.rdma_operations.load() << std::endl;
            } else {
                std::cout << "No data received or decode failed" << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Client error: " << e.what() << std::endl;
            break;
        }
    }
    
    co_return;
}

rdmapp::task<void> performance_test(std::shared_ptr<rdmapp::acceptor> acceptor, 
                                   std::shared_ptr<rdmapp::connector> connector, 
                                   const Config& config) {
    std::cout << "Starting RDMA Reed Solomon Performance Test" << std::endl;
    
    // Establish connection
    auto qp = co_await connector->connect();
    std::cout << "Connection established" << std::endl;
    
    auto sender = std::make_unique<RDMASender>(qp, config);
    auto receiver = std::make_unique<RDMAReceiver>(qp, config);
    
    const int num_iterations = 100;
    std::vector<double> latencies;
    latencies.reserve(num_iterations);
    
    for (int i = 0; i < num_iterations && running; ++i) {
        try {
            // Generate test data
            auto test_data = Utils::generate_test_data(config.data_size());
            
            // Measure round-trip time
            auto start_time = std::chrono::high_resolution_clock::now();
            
            // Send data
            bool send_success = co_await sender->send_data(test_data);
            if (!send_success) {
                std::cerr << "Send failed at iteration " << i << std::endl;
                continue;
            }
            
            // Receive data
            auto received_data = co_await receiver->receive_data();
            auto end_time = std::chrono::high_resolution_clock::now();
            
            if (!received_data.empty() && received_data.size() == test_data.size()) {
                auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
                double latency_ms = duration.count() / 1e6;
                latencies.push_back(latency_ms);
                
                if (i % 10 == 0) {
                    std::cout << "Iteration " << i << ": " << latency_ms << " ms" << std::endl;
                }
            } else {
                std::cerr << "Receive failed at iteration " << i << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cerr << "Performance test error at iteration " << i << ": " << e.what() << std::endl;
        }
    }
    
    // Calculate statistics
    if (!latencies.empty()) {
        std::sort(latencies.begin(), latencies.end());
        double min_latency = latencies.front();
        double max_latency = latencies.back();
        double avg_latency = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
        double median_latency = latencies[latencies.size() / 2];
        
        std::cout << "\nPerformance Results:" << std::endl;
        std::cout << "Iterations: " << latencies.size() << std::endl;
        std::cout << "Min latency: " << min_latency << " ms" << std::endl;
        std::cout << "Max latency: " << max_latency << " ms" << std::endl;
        std::cout << "Avg latency: " << avg_latency << " ms" << std::endl;
        std::cout << "Median latency: " << median_latency << " ms" << std::endl;
        
        // Calculate throughput
        double total_data = latencies.size() * config.data_size();
        double total_time = std::accumulate(latencies.begin(), latencies.end(), 0.0) / 1000.0; // Convert to seconds
        double throughput_mbps = (total_data * 8) / (total_time * 1e6); // Convert to Mbps
        
        std::cout << "Throughput: " << throughput_mbps << " Mbps" << std::endl;
    }
    
    co_return;
}

int main(int argc, char* argv[]) {
    // Set up signal handling
    signal(SIGINT, signal_handler);
    
    if (argc < 2) {
        std::cout << "Usage:" << std::endl;
        std::cout << "  " << argv[0] << " server <port> [k] [m] [packet_size]" << std::endl;
        std::cout << "  " << argv[0] << " client <server_ip> <port> [k] [m] [packet_size]" << std::endl;
        std::cout << "  " << argv[0] << " perf <port> [k] [m] [packet_size]" << std::endl;
        std::cout << std::endl;
        std::cout << "Parameters:" << std::endl;
        std::cout << "  k: Number of data packets (default: 8)" << std::endl;
        std::cout << "  m: Number of parity packets (default: 2)" << std::endl;
        std::cout << "  packet_size: Size of each packet in bytes (default: 1024)" << std::endl;
        return 1;
    }
    
    // Parse configuration
    Config config;
    if (argc > 3) config.k = std::stoi(argv[3]);
    if (argc > 4) config.m = std::stoi(argv[4]);
    if (argc > 5) config.packet_size = std::stoi(argv[5]);
    
    std::string mode = argv[1];
    
    try {
        // Initialize RDMA components
        auto device = std::make_shared<rdmapp::device>(0, 1);
        auto pd = std::make_shared<rdmapp::pd>(device);
        auto cq = std::make_shared<rdmapp::cq>(device);
        auto cq_poller = std::make_shared<rdmapp::cq_poller>(cq);
        auto loop = rdmapp::socket::event_loop::new_loop();
        auto looper = std::thread([loop]() { loop->loop(); });
        
        if (mode == "server") {
            int port = std::stoi(argv[2]);
            auto acceptor = std::make_shared<rdmapp::acceptor>(loop, port, pd, cq);
            server_task(acceptor, config).detach();
            
        } else if (mode == "client") {
            std::string server_ip = argv[2];
            int port = std::stoi(argv[3]);
            auto connector = std::make_shared<rdmapp::connector>(loop, server_ip, port, pd, cq);
            client_task(connector, config).detach();
            
        } else if (mode == "perf") {
            int port = std::stoi(argv[2]);
            auto acceptor = std::make_shared<rdmapp::acceptor>(loop, port, pd, cq);
            auto connector = std::make_shared<rdmapp::connector>(loop, "127.0.0.1", port, pd, cq);
            performance_test(acceptor, connector, config).detach();
            
        } else {
            std::cerr << "Invalid mode: " << mode << std::endl;
            return 1;
        }
        
        // Wait for completion or signal
        while (running) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        
        // Cleanup
        loop->close();
        looper.join();
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
