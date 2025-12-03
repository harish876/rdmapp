#include "acceptor.h"
#include "connector.h"
#include "rdma_logger.h"
#include "rdma_receiver.h"
#include "rdma_sender.h"
#include "rdma_util.h"
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <infiniband/verbs.h>

#include <rdmapp/rdmapp.h>

using namespace RDMA_EC;

namespace {

constexpr const char *kDefaultConfigPath = "./examples/rdma.config";

enum class RunnerRole { Server, Client };

enum class ParseResult { Ok, Help, Error };

struct ProgramOptions {
  RunnerRole role;
  int port{8011}; // default port
  std::string host;
  std::optional<std::string> config_path;
  std::optional<std::string> log_level;
  int iterations{1};
  std::optional<std::string> metrics_output_path;
};

void print_usage(const char *program) {
  std::cout
      << "Usage:\n"
      << "  " << program
      << " server --port <port> [--config <path>] [--log-level <level>] "
         "[--iters <count>] [--metrics-out <path>]\n"
      << "  " << program
      << " client --host <host> --port <port> [--config <path>] [--log-level "
         "<level>] [--iters <count>] [--metrics-out <path>]\n"
      << "\nOptions:\n"
      << "  --config <path>   Path to configuration file (default: "
      << kDefaultConfigPath << ")\n"
      << "  --log-level <lvl> Logging level: debug, info, error (default from "
         "config)\n"
      << "  --iters <count>   Number of iterations to run (default: 1)\n"
      << "  --metrics-out <path>\n"
      << "                   Append per-iteration metrics to CSV file\n"
      << "  --help            Show this message and exit\n";
}

ParseResult parse_arguments(int argc, char *argv[], ProgramOptions &options) {
  if (argc < 2) {
    Logger::error() << "Missing role argument (server/client)";
    print_usage(argc > 0 ? argv[0] : "rdma_sdr_runner");
    return ParseResult::Error;
  }

  std::string role_arg = argv[1];
  if (role_arg == "--help" || role_arg == "-h") {
    print_usage(argc > 0 ? argv[0] : "rdma_sdr_runner");
    return ParseResult::Help;
  }

  if (role_arg == "server") {
    options.role = RunnerRole::Server;
  } else if (role_arg == "client") {
    options.role = RunnerRole::Client;
  } else {
    Logger::error() << "Unknown role: " << role_arg;
    print_usage(argv[0]);
    return ParseResult::Error;
  }

  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--port") {
      if (i + 1 >= argc) {
        Logger::error() << "--port expects an integer value";
        return ParseResult::Error;
      }
      try {
        options.port = std::stoi(argv[++i]);
      } catch (const std::exception &e) {
        Logger::error() << "Invalid port: " << argv[i] << " (" << e.what()
                        << ")";
        return ParseResult::Error;
      }
    } else if (arg == "--host") {
      if (i + 1 >= argc) {
        Logger::error() << "--host expects an address";
        return ParseResult::Error;
      }
      options.host = argv[++i];
    } else if (arg == "--config") {
      if (i + 1 >= argc) {
        Logger::error() << "--config expects a file path";
        return ParseResult::Error;
      }
      options.config_path = argv[++i];
    } else if (arg == "--log-level") {
      if (i + 1 >= argc) {
        Logger::error() << "--log-level expects a value (debug/info/error)";
        return ParseResult::Error;
      }
      std::string level = argv[++i];
      std::transform(level.begin(), level.end(), level.begin(),
                     [](unsigned char c) { return std::tolower(c); });
      if (level != "debug" && level != "info" && level != "error") {
        Logger::error() << "Unknown log level: " << level;
        return ParseResult::Error;
      }
      options.log_level = level;
    } else if (arg == "--iters") {
      if (i + 1 >= argc) {
        Logger::error() << "--iters expects a positive integer";
        return ParseResult::Error;
      }
      try {
        options.iterations = std::stoi(argv[++i]);
      } catch (const std::exception &e) {
        Logger::error() << "Invalid iterations value: " << argv[i] << " ("
                        << e.what() << ")";
        return ParseResult::Error;
      }
    } else if (arg == "--metrics-out") {
      if (i + 1 >= argc) {
        Logger::error() << "--metrics-out expects a file path";
        return ParseResult::Error;
      }
      options.metrics_output_path = argv[++i];
    } else if (arg == "--help" || arg == "-h") {
      print_usage(argv[0]);
      return ParseResult::Help;
    } else {
      Logger::error() << "Unknown argument: " << arg;
      print_usage(argv[0]);
      return ParseResult::Error;
    }
  }

  if (options.port <= 0 || options.port > 65535) {
    Logger::error() << "Port must be within 1-65535";
    return ParseResult::Error;
  }

  if (options.role == RunnerRole::Client && options.host.empty()) {
    Logger::error() << "Client mode requires --host";
    return ParseResult::Error;
  }

  if (options.iterations <= 0) {
    Logger::error() << "--iters must be greater than zero";
    return ParseResult::Error;
  }

  if (!options.config_path.has_value()) {
    options.config_path = std::string(kDefaultConfigPath);
  }

  return ParseResult::Ok;
}

} // namespace

struct TransferMetrics {
  size_t packets{0};
  size_t bytes{0};
  double duration_us{0.0};
};

bool append_metrics_csv(const std::string &path,
                        const std::vector<TransferMetrics> &metrics,
                        const std::string &role_label) {
  if (metrics.empty()) {
    return true;
  }

  bool write_header = true;
  {
    std::ifstream existing(path);
    if (existing.good()) {
      auto ch = existing.peek();
      if (ch != std::ifstream::traits_type::eof()) {
        write_header = false;
      }
    }
  }

  std::ofstream out(path, std::ios::app);
  if (!out) {
    Logger::error() << "Failed to open metrics output file: " << path;
    return false;
  }

  if (write_header) {
    out << "role,iteration,packets,bytes,duration_us,throughput_mbit_s,"
           "throughput_mb_s\n";
  }

  for (size_t i = 0; i < metrics.size(); ++i) {
    const auto &metric = metrics[i];
    const double seconds = metric.duration_us / 1'000'000.0;
    const double throughput_mbit_s =
        seconds > 0.0 ? (metric.bytes * 8.0) / seconds / 1'000'000.0 : 0.0;
    const double throughput_mb_s =
        seconds > 0.0 ? (metric.bytes / (1024.0 * 1024.0)) / seconds : 0.0;

    out << role_label << ',' << (i + 1) << ',' << metric.packets << ','
        << metric.bytes << ',' << metric.duration_us << ',' << throughput_mbit_s
        << ',' << throughput_mb_s << '\n';
  }

  Logger::info() << "Appended " << metrics.size() << " metrics entries to "
                 << path;
  return true;
}

void *allocate_test_data(size_t size) {
  void *buffer = nullptr;
  size_t page_size = sysconf(_SC_PAGESIZE);

  size_t aligned_size = ((size + page_size - 1) / page_size) * page_size;

  if (posix_memalign(&buffer, page_size, aligned_size)) {
    perror("posix_memalign");
    return nullptr;
  }

  uint8_t *data = static_cast<uint8_t *>(buffer);
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<uint8_t>((i * 7 + 42) % 256);
  }

  if (aligned_size > size) {
    memset(data + size, 0, aligned_size - size);
  }

  Logger::info() << "Allocated page-aligned buffer: size=" << size
                 << ", aligned_size=" << aligned_size
                 << ", page_size=" << page_size << ", addr=0x" << std::hex
                 << reinterpret_cast<uintptr_t>(buffer) << std::dec;

  return buffer;
}

std::vector<uint8_t> generate_test_data(size_t size) {
  std::vector<uint8_t> data(size);

  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<uint8_t>((i * 7 + 42) % 256);
  }

  return data;
}

bool verify_data(const std::vector<uint8_t> &sent,
                 const std::vector<uint8_t> &received) {
  if (sent.size() != received.size()) {
    Logger::error() << "Size mismatch: sent " << sent.size() << " vs received "
                    << received.size();
    return false;
  }

  size_t first_mismatch = SIZE_MAX;
  for (size_t i = 0; i < sent.size(); ++i) {
    if (sent[i] != received[i]) {
      if (first_mismatch == SIZE_MAX) {
        first_mismatch = i;
      }
    }
  }

  if (first_mismatch != SIZE_MAX) {
    Logger::error() << "Data mismatch at byte " << first_mismatch
                    << ": expected " << static_cast<int>(sent[first_mismatch])
                    << " got " << static_cast<int>(received[first_mismatch]);
    Logger::error() << "Showing first 10 mismatches:";
    size_t shown = 0;
    for (size_t i = first_mismatch; i < sent.size() && shown < 10; ++i) {
      if (sent[i] != received[i]) {
        Logger::error() << "  Byte " << i << ": expected "
                        << static_cast<int>(sent[i]) << " got "
                        << static_cast<int>(received[i]);
        shown++;
      }
    }
    return false;
  }

  return true;
}

int main(int argc, char *argv[]) {
  srand(42);

  ProgramOptions options;
  switch (parse_arguments(argc, argv, options)) {
  case ParseResult::Ok:
    break;
  case ParseResult::Help:
    return 0;
  case ParseResult::Error:
  default:
    return 1;
  }

  auto device = std::make_shared<rdmapp::device>(0, 1, 3);
  auto pd = std::make_shared<rdmapp::pd>(device);
  auto loop = rdmapp::socket::event_loop::new_loop();
  auto looper = std::thread([loop]() { loop->loop(); });

  try {
    Config config;

    if (options.config_path) {
      const std::string &config_file = *options.config_path;
      if (config.load_from_file(config_file)) {
        Logger::info() << "Loaded configuration from " << config_file;
        config.print();
      } else {
        Logger::info() << "Warning: Failed to load config file, using defaults";
      }
    }

    if (options.log_level) {
      config.logging_level =
          *options.log_level; // override the log level in config
    }

    Logger::set_enabled(config.enable_logging);
    Logger::set_level(Logger::level_from_string(config.logging_level));

    size_t buffer_size = config.buffer_size;
    const int total_iters = options.iterations;
    if (options.role == RunnerRole::Server) {
      int port = options.port;
      Logger::info() << "Starting as RECEIVER on port " << port;

      Config receiver_config = config;
      receiver_config.buffer_size = buffer_size * 2;

      auto send_cq = std::make_shared<rdmapp::cq>(device, config.rx_depth);
      auto recv_cq = std::make_shared<rdmapp::cq>(device, config.rx_depth);

      auto send_cq_poller = std::make_shared<rdmapp::cq_poller>(send_cq);

      auto acceptor = std::make_shared<rdmapp::acceptor>(
          loop, port, pd, recv_cq, send_cq, nullptr,
          receiver_config.transport_type);

      double total_duration_us = 0.0;
      size_t total_packets = 0;
      size_t total_bytes = 0;
      std::vector<TransferMetrics> iteration_metrics;
      iteration_metrics.reserve(total_iters);

      for (int iter = 0; iter < total_iters; ++iter) {
        rdmapp::task<TransferMetrics> receiver_task =
            [acceptor, recv_cq, buffer_size, receiver_config, iter,
             total_iters]() -> rdmapp::task<TransferMetrics> {
          RDMAReceiver receiver(acceptor, recv_cq, receiver_config);

          Logger::info() << "\n=== RECEIVER STARTING (iteration " << (iter + 1)
                         << "/" << total_iters << ") ===";
          Logger::info() << "Expecting " << buffer_size << " bytes";
          Logger::info() << "MTU: " << receiver_config.mtu
                         << ", Chunk size: " << receiver_config.chunk_size;
          Logger::info() << ", Transport Type: "
                         << ((receiver_config.transport_type == IBV_QPT_RC)
                                 ? "RC"
                                 : "UC");

          auto start_time = std::chrono::high_resolution_clock::now();

          co_await receiver.receive_data();

          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration_us =
              std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                    start_time)
                  .count();
          auto duration_ms = duration_us / 1000.0;

          std::cout << "=== RECEIVER COMPLETE (iteration " << (iter + 1)
                    << ") ===" << std::endl;
          std::cout << "Received " << receiver.get_packets_received()
                    << " packets, " << receiver.get_bytes_received() << " bytes"
                    << std::endl;
          std::cout << "Transfer time: " << duration_ms << " ms ("
                    << duration_us / 1000000.0 << " seconds)" << std::endl;
          if (duration_us > 0) {
            long long bytes = receiver.get_bytes_received();
            double mbits_per_sec = (bytes * 8.0) / duration_us;
            double mb_per_sec = (bytes * 1000.0) / duration_ms / 1024 / 1024;
            std::cout << "Throughput: " << mb_per_sec << " MB/s ("
                      << mbits_per_sec << " Mbit/sec)" << std::endl;
          }

          co_return TransferMetrics{receiver.get_packets_received(),
                                    receiver.get_bytes_received(),
                                    static_cast<double>(duration_us)};
        }();

        auto &future = receiver_task.get_future();
        receiver_task.detach();
        future.wait();
        TransferMetrics metrics = future.get();

        total_duration_us += metrics.duration_us;
        total_packets += metrics.packets;
        total_bytes += metrics.bytes;
        iteration_metrics.push_back(metrics);
      }

      if (total_iters > 1) {
        double avg_duration_us = total_duration_us / total_iters;
        double avg_seconds = avg_duration_us / 1'000'000.0;
        double avg_packets = static_cast<double>(total_packets) / total_iters;
        double avg_bytes = static_cast<double>(total_bytes) / total_iters;
        double avg_mbps = avg_seconds > 0
                              ? (avg_bytes * 8.0) / avg_seconds / 1'000'000.0
                              : 0.0;
        double avg_mb_s = avg_seconds > 0
                              ? (avg_bytes / (1024.0 * 1024.0)) / avg_seconds
                              : 0.0;

        std::cout << "\n=== RECEIVER AVERAGES OVER " << total_iters
                  << " ITERATIONS ===" << std::endl;
        std::cout << "Average packets: " << avg_packets << std::endl;
        std::cout << "Average bytes: " << avg_bytes << std::endl;
        std::cout << "Average duration: " << (avg_duration_us / 1000.0) << " ms"
                  << std::endl;
        std::cout << "Average throughput: " << avg_mb_s << " MB/s (" << avg_mbps
                  << " Mbit/sec)" << std::endl;
      }

      if (options.metrics_output_path) {
        if (!append_metrics_csv(*options.metrics_output_path, iteration_metrics,
                                "receiver")) {
          Logger::error() << "Failed to write receiver metrics to "
                          << *options.metrics_output_path;
        }
      }
    } else if (options.role == RunnerRole::Client) {
      std::string receiver_ip = options.host;
      int port = options.port;
      Logger::info() << "Starting as SENDER connecting to " << receiver_ip
                     << ":" << port;

      Config sender_config = config;
      sender_config.buffer_size = buffer_size * 2;

      auto send_cq = std::make_shared<rdmapp::cq>(device, config.rx_depth);
      auto recv_cq = std::make_shared<rdmapp::cq>(device, config.rx_depth);

      auto send_cq_poller = std::make_shared<rdmapp::cq_poller>(send_cq);
      auto recv_cq_poller = std::make_shared<rdmapp::cq_poller>(recv_cq);

      double total_duration_us = 0.0;
      size_t total_packets = 0;
      size_t total_bytes = 0;
      std::vector<TransferMetrics> iteration_metrics;
      iteration_metrics.reserve(total_iters);

      for (int iter = 0; iter < total_iters; ++iter) {
        auto connector = std::make_shared<rdmapp::connector>(
            loop, receiver_ip, port, pd, recv_cq, send_cq, nullptr,
            sender_config.transport_type);

        rdmapp::task<TransferMetrics> sender_task =
            [connector, buffer_size, sender_config, iter,
             total_iters]() -> rdmapp::task<TransferMetrics> {
          RDMASender sender(connector, sender_config);

          void *large_data_buffer = allocate_test_data(buffer_size);
          if (!large_data_buffer) {
            Logger::error() << "Failed to allocate page-aligned buffer";
            co_return TransferMetrics{};
          }

          std::unique_ptr<void, decltype(&free)> buffer_guard(large_data_buffer,
                                                              &free);

          Logger::info() << "\n=== SENDER STARTING (iteration " << (iter + 1)
                         << "/" << total_iters << ") ===";
          Logger::info() << "Sending " << buffer_size << " bytes";
          Logger::info() << "MTU: " << sender_config.mtu
                         << ", Chunk size: " << sender_config.chunk_size;

          auto start_time = std::chrono::high_resolution_clock::now();

          co_await sender.send_data(large_data_buffer, buffer_size);

          auto end_time = std::chrono::high_resolution_clock::now();
          auto duration_us =
              std::chrono::duration_cast<std::chrono::microseconds>(end_time -
                                                                    start_time)
                  .count();
          auto duration_ms = duration_us / 1000.0;

          std::cout << "=== SENDER COMPLETE (iteration " << (iter + 1)
                    << ") ===" << std::endl;
          std::cout << "Sent " << sender.get_packets_sent() << " packets, "
                    << sender.get_bytes_sent() << " bytes" << std::endl;
          std::cout << "Transfer time: " << duration_ms << " ms ("
                    << duration_us / 1000000.0 << " seconds)" << std::endl;
          if (duration_us > 0) {
            long long bytes = sender.get_bytes_sent();
            double mbits_per_sec = (bytes * 8.0) / duration_us;
            double mb_per_sec = (bytes * 1000.0) / duration_ms / 1024 / 1024;
            std::cout << "Throughput: " << mb_per_sec << " MB/s ("
                      << mbits_per_sec << " Mbit/sec)" << std::endl;
          }

          co_return TransferMetrics{sender.get_packets_sent(),
                                    sender.get_bytes_sent(),
                                    static_cast<double>(duration_us)};
        }();

        auto &future = sender_task.get_future();
        sender_task.detach();
        future.wait();
        TransferMetrics metrics = future.get();

        total_duration_us += metrics.duration_us;
        total_packets += metrics.packets;
        total_bytes += metrics.bytes;
        iteration_metrics.push_back(metrics);
      }

      if (total_iters > 1) {
        double avg_duration_us = total_duration_us / total_iters;
        double avg_seconds = avg_duration_us / 1'000'000.0;
        double avg_packets = static_cast<double>(total_packets) / total_iters;
        double avg_bytes = static_cast<double>(total_bytes) / total_iters;
        double avg_mbps = avg_seconds > 0
                              ? (avg_bytes * 8.0) / avg_seconds / 1'000'000.0
                              : 0.0;
        double avg_mb_s = avg_seconds > 0
                              ? (avg_bytes / (1024.0 * 1024.0)) / avg_seconds
                              : 0.0;

        std::cout << "\n=== SENDER AVERAGES OVER " << total_iters
                  << " ITERATIONS ===" << std::endl;
        std::cout << "Average packets: " << avg_packets << std::endl;
        std::cout << "Average bytes: " << avg_bytes << std::endl;
        std::cout << "Average duration: " << (avg_duration_us / 1000.0) << " ms"
                  << std::endl;
        std::cout << "Average throughput: " << avg_mb_s << " MB/s (" << avg_mbps
                  << " Mbit/sec)" << std::endl;
      }

      if (options.metrics_output_path) {
        if (!append_metrics_csv(*options.metrics_output_path, iteration_metrics,
                                "sender")) {
          Logger::error() << "Failed to write sender metrics to "
                          << *options.metrics_output_path;
        }
      }
    } else {
      Logger::error() << "Unsupported role configuration";
      return 1;
    }
  } catch (const std::exception &e) {
    Logger::error() << "Error: " << e.what();
    return 1;
  }

  loop->close();
  looper.join();

  return 0;
}