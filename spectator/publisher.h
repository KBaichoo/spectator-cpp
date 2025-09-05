#pragma once

#include "logger.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include <asio.hpp>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <atomic>
#include <vector>
#include <memory>

namespace spectator {

struct PublisherConfig {
  uint32_t bytes_to_buffer = 0;
  std::chrono::milliseconds flush_interval = std::chrono::milliseconds(60000);
  size_t max_buffer_size = 1024 * 1024;  // 1MB default
  size_t num_worker_threads = 2;
};

class SpectatordPublisher {
 public:
  explicit SpectatordPublisher(
      absl::string_view endpoint,
      uint32_t bytes_to_buffer = 0,
      std::chrono::milliseconds flush_interval = std::chrono::milliseconds(60000),
      std::shared_ptr<spdlog::logger> logger = DefaultLogger());
  
  explicit SpectatordPublisher(
      absl::string_view endpoint,
      const PublisherConfig& config,
      std::shared_ptr<spdlog::logger> logger = DefaultLogger());
  
  SpectatordPublisher(const SpectatordPublisher&) = delete;
  ~SpectatordPublisher();

  void send(std::string_view measurement);

 private:
  struct PerCpuBuffer {
    std::string buffer;
    std::chrono::steady_clock::time_point last_flush_time;
    std::mutex mutex;
  };

  struct WorkItem {
    std::string data;
    std::chrono::steady_clock::time_point timestamp;
  };

  void setup_nop_sender();
  void setup_unix_domain(absl::string_view path);
  void setup_udp(absl::string_view host_port);
  void local_reconnect(absl::string_view path);
  void udp_reconnect(const asio::ip::udp::endpoint& endpoint);
  
  void start_worker_threads();
  void stop_worker_threads();
  void worker_thread_loop();
  void flush_cpu_buffer(size_t cpu_id);
  void enqueue_work(std::string data);
  size_t get_cpu_id() const;
  
  std::shared_ptr<spdlog::logger> logger_;
  asio::io_context io_context_;
  asio::ip::udp::socket udp_socket_;
  asio::local::datagram_protocol::socket local_socket_;
  
  PublisherConfig config_;
  std::vector<std::unique_ptr<PerCpuBuffer>> cpu_buffers_;
  
  std::queue<WorkItem> work_queue_;
  std::mutex work_queue_mutex_;
  std::condition_variable work_cv_;
  std::atomic<bool> shutdown_requested_{false};
  std::vector<std::thread> worker_threads_;
  
  std::atomic<size_t> total_buffer_size_{0};
  
  enum class EndpointType { UDP, UNIX_DOMAIN, DISABLED } endpoint_type_;
  std::string endpoint_path_;
  asio::ip::udp::endpoint udp_endpoint_;
};

}  // namespace spectator
