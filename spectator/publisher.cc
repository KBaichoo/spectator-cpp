#include "publisher.h"
#include "logger.h"
#include <fmt/format.h>
#include <sched.h>
#include <unistd.h>

namespace spectator {

static const char NEW_LINE = '\n';

SpectatordPublisher::SpectatordPublisher(absl::string_view endpoint,
                                         uint32_t bytes_to_buffer,
                                         std::chrono::milliseconds flush_interval,
                                         std::shared_ptr<spdlog::logger> logger)
    : SpectatordPublisher(endpoint, 
                         PublisherConfig{bytes_to_buffer, flush_interval}, 
                         std::move(logger)) {}

SpectatordPublisher::SpectatordPublisher(absl::string_view endpoint,
                                         const PublisherConfig& config,
                                         std::shared_ptr<spdlog::logger> logger)
    : logger_(std::move(logger)),
      udp_socket_(io_context_),
      local_socket_(io_context_),
      config_(config) {
  
  // Initialize per-CPU buffers
  size_t num_cpus = std::thread::hardware_concurrency();
  if (num_cpus == 0) num_cpus = 4;  // fallback
  
  cpu_buffers_.reserve(num_cpus);
  for (size_t i = 0; i < num_cpus; ++i) {
    auto buffer = std::make_unique<PerCpuBuffer>();
    buffer->buffer.reserve(config_.bytes_to_buffer + 1024);
    buffer->last_flush_time = std::chrono::steady_clock::now();
    cpu_buffers_.push_back(std::move(buffer));
  }
  
  if (absl::StartsWith(endpoint, "unix:")) {
    setup_unix_domain(endpoint.substr(5));
  } else if (absl::StartsWith(endpoint, "udp:")) {
    auto pos = 4;
    if (endpoint.substr(pos, 2) == "//") {
      pos += 2;
    }
    setup_udp(endpoint.substr(pos));
  } else if (endpoint != "disabled") {
    logger_->warn(
        "Unknown endpoint: '{}'. Expecting: 'unix:/path/to/socket'"
        " or 'udp:hostname:port' - Will not send metrics",
        std::string(endpoint));
    setup_nop_sender();
  }
  
  start_worker_threads();
}

SpectatordPublisher::~SpectatordPublisher() {
  // Flush all remaining CPU buffers before shutdown
  for (size_t i = 0; i < cpu_buffers_.size(); ++i) {
    std::lock_guard<std::mutex> lock(cpu_buffers_[i]->mutex);
    flush_cpu_buffer(i);
  }
  
  stop_worker_threads();
}

void SpectatordPublisher::setup_nop_sender() {
  endpoint_type_ = EndpointType::DISABLED;
}

void SpectatordPublisher::local_reconnect(absl::string_view path) {
  using endpoint_t = asio::local::datagram_protocol::endpoint;
  try {
    if (local_socket_.is_open()) {
      local_socket_.close();
    }
    local_socket_.open();
    local_socket_.connect(endpoint_t(std::string(path)));
  } catch (std::exception& e) {
    logger_->warn("Unable to connect to {}: {}", std::string(path), e.what());
  }
}

void SpectatordPublisher::setup_unix_domain(absl::string_view path) {
  endpoint_type_ = EndpointType::UNIX_DOMAIN;
  endpoint_path_ = std::string(path);
  local_reconnect(path);
}

inline asio::ip::udp::endpoint resolve_host_port(
    asio::io_context& io_context,  // NOLINT
    absl::string_view host_port) {
  using asio::ip::udp;
  udp::resolver resolver{io_context};

  auto end_host = host_port.find(':');
  if (end_host == std::string_view::npos) {
    auto err = fmt::format(
        "Unable to parse udp endpoint: '{}'. Expecting hostname:port",
        std::string(host_port));
    throw std::runtime_error(err);
  }

  auto host = host_port.substr(0, end_host);
  auto port = host_port.substr(end_host + 1);
  return *resolver.resolve(udp::v6(), std::string(host), std::string(port));
}

void SpectatordPublisher::udp_reconnect(
    const asio::ip::udp::endpoint& endpoint) {
  try {
    if (udp_socket_.is_open()) {
      udp_socket_.close();
    }
    udp_socket_.open(asio::ip::udp::v6());
    udp_socket_.connect(endpoint);
  } catch (std::exception& e) {
    logger_->warn("Unable to connect to {}: {}", endpoint.address().to_string(),
                  endpoint.port());
  }
}

void SpectatordPublisher::setup_udp(absl::string_view host_port) {
  endpoint_type_ = EndpointType::UDP;
  udp_endpoint_ = resolve_host_port(io_context_, host_port);
  udp_reconnect(udp_endpoint_);
}

void SpectatordPublisher::send(std::string_view measurement) {
  if (endpoint_type_ == EndpointType::DISABLED) {
    logger_->trace("{}", measurement);
    return;
  }
  
  size_t cpu_id = get_cpu_id();
  auto& cpu_buffer = cpu_buffers_[cpu_id];
  
  {
    std::lock_guard<std::mutex> lock(cpu_buffer->mutex);
    
    // Check if adding this measurement would exceed the max buffer size
    if (total_buffer_size_.load() + measurement.size() > config_.max_buffer_size) {
      logger_->warn("Buffer size limit exceeded, dropping measurement");
      return;
    }
    
    cpu_buffer->buffer.append(measurement);
    cpu_buffer->buffer.push_back(NEW_LINE);
    total_buffer_size_.fetch_add(measurement.size() + 1);
    
    const auto now = std::chrono::steady_clock::now();
    const bool should_flush = cpu_buffer->buffer.length() >= config_.bytes_to_buffer ||
                             now - cpu_buffer->last_flush_time >= config_.flush_interval;
    
    if (should_flush) {
      flush_cpu_buffer(cpu_id);
    }
  }
}

size_t SpectatordPublisher::get_cpu_id() const {
  int cpu = sched_getcpu();
  if (cpu < 0) {
    // Fallback to thread-local storage or simple hash
    static thread_local size_t tls_cpu_id = std::hash<std::thread::id>{}(std::this_thread::get_id()) % cpu_buffers_.size();
    return tls_cpu_id;
  }
  return static_cast<size_t>(cpu) % cpu_buffers_.size();
}

void SpectatordPublisher::flush_cpu_buffer(size_t cpu_id) {
  auto& cpu_buffer = cpu_buffers_[cpu_id];
  
  if (!cpu_buffer->buffer.empty()) {
    size_t buffer_size = cpu_buffer->buffer.size();
    
    // Remove trailing newline for cleaner output
    if (cpu_buffer->buffer.back() == NEW_LINE) {
      cpu_buffer->buffer.pop_back();
    }
    
    enqueue_work(std::move(cpu_buffer->buffer));
    
    total_buffer_size_.fetch_sub(buffer_size);
    cpu_buffer->buffer.clear();
    cpu_buffer->buffer.reserve(config_.bytes_to_buffer + 1024);
    cpu_buffer->last_flush_time = std::chrono::steady_clock::now();
  }
}

void SpectatordPublisher::enqueue_work(std::string data) {
  {
    std::lock_guard<std::mutex> lock(work_queue_mutex_);
    work_queue_.emplace(WorkItem{std::move(data), std::chrono::steady_clock::now()});
  }
  work_cv_.notify_one();
}

void SpectatordPublisher::start_worker_threads() {
  if (endpoint_type_ == EndpointType::DISABLED) {
    return;
  }
  
  worker_threads_.reserve(config_.num_worker_threads);
  for (size_t i = 0; i < config_.num_worker_threads; ++i) {
    worker_threads_.emplace_back(&SpectatordPublisher::worker_thread_loop, this);
  }
}

void SpectatordPublisher::stop_worker_threads() {
  shutdown_requested_.store(true);
  work_cv_.notify_all();
  
  for (auto& thread : worker_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  worker_threads_.clear();
}

void SpectatordPublisher::worker_thread_loop() {
  while (!shutdown_requested_.load()) {
    std::unique_lock<std::mutex> lock(work_queue_mutex_);
    work_cv_.wait(lock, [this] { return !work_queue_.empty() || shutdown_requested_.load(); });
    
    if (shutdown_requested_.load()) {
      break;
    }
    
    if (work_queue_.empty()) {
      continue;
    }
    
    WorkItem item = std::move(work_queue_.front());
    work_queue_.pop();
    lock.unlock();
    
    // Perform the actual socket I/O
    switch (endpoint_type_) {
      case EndpointType::UNIX_DOMAIN:
        for (int i = 0; i < 3; ++i) {
          try {
            auto sent_bytes = local_socket_.send(asio::buffer(item.data));
            logger_->trace("Sent (local): {} bytes", sent_bytes);
            break;
          } catch (std::exception& e) {
            local_reconnect(endpoint_path_);
            logger_->warn("Unable to send data - attempt {}/3 ({})", i + 1, e.what());
          }
        }
        break;
        
      case EndpointType::UDP:
        for (int i = 0; i < 3; ++i) {
          try {
            udp_socket_.send(asio::buffer(item.data));
            logger_->trace("Sent (udp): {} bytes", item.data.size());
            break;
          } catch (std::exception& e) {
            logger_->warn("Unable to send data - attempt {}/3 ({})", i + 1, e.what());
            udp_reconnect(udp_endpoint_);
          }
        }
        break;
        
      case EndpointType::DISABLED:
        break;
    }
  }
}

}  // namespace spectator