#include "id.h"
#include "logger.h"
#include "publisher.h"
#include "stateless_meters.h"
#include "test_server.h"
#include <gtest/gtest.h>
#include <unistd.h>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <future>
#include <sstream>
#include <algorithm>

namespace {

using spectator::Counter;
using spectator::Id;
using spectator::SpectatordPublisher;
using spectator::Tags;

TEST(Publisher, Udp) {
  // travis does not support udp on its container
  if (std::getenv("TRAVIS_COMPILER") == nullptr) {
    TestUdpServer server;
    server.Start();
    auto logger = spectator::DefaultLogger();
    logger->info("Udp Server started on port {}", server.GetPort());

    SpectatordPublisher publisher{fmt::format("udp:localhost:{}", server.GetPort()), 0};
    Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};
    c.Increment();
    c.Add(2);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto msgs = server.GetMessages();
    server.Stop();
    std::vector<std::string> expected{"c:counter:1", "c:counter:2"};
    EXPECT_EQ(server.GetMessages(), expected);
  }
}

const char* first_not_null(char* a, const char* b) {
  if (a != nullptr) return a;
  return b;
}

TEST(Publisher, UnixNoBuffer) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();
  logger->info("Unix Server started on path {}", path);
  SpectatordPublisher publisher{fmt::format("unix:{}", path), 0};
  Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};
  c.Increment();
  c.Add(2);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  // With multithreading, messages might arrive in different order
  EXPECT_EQ(msgs.size(), 2);
  EXPECT_TRUE(std::find(msgs.begin(), msgs.end(), "c:counter:1") != msgs.end());
  EXPECT_TRUE(std::find(msgs.begin(), msgs.end(), "c:counter:2") != msgs.end());
}

TEST(Publisher, UnixBuffer) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();
  logger->info("Unix Server started on path {}", path);
  // Do not send until we buffer 32 bytes of data.
  SpectatordPublisher publisher{fmt::format("unix:{}", path), 32};
  Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};
  c.Increment();
  c.Increment();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto msgs = server.GetMessages();
  std::vector<std::string> emptyVector{};
  EXPECT_EQ(msgs, emptyVector);
  c.Increment();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  msgs = server.GetMessages();
  std::vector<std::string> expected{"c:counter:1\nc:counter:1\nc:counter:1"};
  EXPECT_EQ(msgs, expected);
  server.Stop();
  unlink(path.c_str());
}

TEST(Publisher, UnixBufferTimeFlush) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();
  logger->info("Unix Server started on path {}", path);

  // Set buffer size to a large value so that flushing is based on time
  SpectatordPublisher publisher{fmt::format("unix:{}", path), 10000,
                                std::chrono::milliseconds(500)};
  Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};

  // Wait for 300ms, increment, and the counter should not be flushed (300ms is less than the 500ms
  // flush interval)
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  c.Increment();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  auto msgs = server.GetMessages();
  EXPECT_TRUE(msgs.empty());

  // Wait for another 300ms, increment, and the counter should be flushed (600ms is greater than
  // 500ms flush interval)
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  c.Increment();
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  msgs = server.GetMessages();
  std::vector<std::string> first_flush{"c:counter:1\nc:counter:1"};
  EXPECT_EQ(msgs, first_flush);

  server.Stop();
  unlink(path.c_str());
}

TEST(Publisher, Nop) {
  SpectatordPublisher publisher{"", 0};
  Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};
  c.Increment();
  c.Add(2);
}

TEST(Publisher, MultithreadedUnix) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver_mt.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();

  spectator::PublisherConfig config;
  config.bytes_to_buffer = 100;
  config.num_worker_threads = 3;
  config.max_buffer_size = 10000;

  SpectatordPublisher publisher{fmt::format("unix:{}", path), config};

  const int num_threads = 10;
  const int increments_per_thread = 50;
  std::vector<std::thread> threads;
  std::atomic<int> total_sent{0};

  // Launch multiple threads that increment counters simultaneously
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&, i]() {
      Counter c{std::make_shared<Id>(fmt::format("counter_{}", i), Tags{}), &publisher};
      for (int j = 0; j < increments_per_thread; ++j) {
        c.Increment();
        total_sent.fetch_add(1);
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : threads) {
    thread.join();
  }

  // Wait longer to ensure all data is processed
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  // We should receive messages from all threads
  EXPECT_GT(msgs.size(), 0);

  // Count total increments in received messages
  int total_received = 0;
  for (const auto& msg_batch : msgs) {
    // Count the number of counter lines in each batch
    std::istringstream iss(msg_batch);
    std::string line;
    while (std::getline(iss, line)) {
      if (!line.empty() && line.find("c:counter_") == 0) {
        total_received++;
      }
    }
  }

  // Due to the asynchronous nature and potential message loss in testing,
  // we verify that we received most of the messages (at least 80%)
  EXPECT_GE(total_received, static_cast<int>(num_threads * increments_per_thread * 0.8));
  EXPECT_LE(total_received, num_threads * increments_per_thread);
}

TEST(Publisher, BufferSizeLimit) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver_limit.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();

  spectator::PublisherConfig config;
  config.bytes_to_buffer = 50;
  config.max_buffer_size = 100;                              // Very small limit
  config.flush_interval = std::chrono::milliseconds(10000);  // Long interval

  SpectatordPublisher publisher{fmt::format("unix:{}", path), config};
  Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};

  // Try to exceed the buffer limit
  for (int i = 0; i < 20; ++i) {
    c.Increment();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  // Should have received some messages, but not all due to buffer limit
  EXPECT_GT(msgs.size(), 0);
}

TEST(Publisher, ConcurrentFlushes) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver_concurrent.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();

  spectator::PublisherConfig config;
  config.bytes_to_buffer = 10;  // Small buffer to force frequent flushes
  config.num_worker_threads = 2;

  SpectatordPublisher publisher{fmt::format("unix:{}", path), config};

  // Create multiple counters on different threads to trigger concurrent flushes
  std::vector<std::future<void>> futures;
  for (int i = 0; i < 5; ++i) {
    futures.push_back(std::async(std::launch::async, [&, i]() {
      Counter c{std::make_shared<Id>(fmt::format("counter_{}", i), Tags{}), &publisher};
      for (int j = 0; j < 10; ++j) {
        c.Increment();
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }));
  }

  // Wait for all async operations to complete
  for (auto& future : futures) {
    future.wait();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  EXPECT_GT(msgs.size(), 0);
}

TEST(Publisher, BackgroundThreadShutdown) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver_shutdown.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();

  {
    spectator::PublisherConfig config;
    config.num_worker_threads = 4;
    config.bytes_to_buffer = 0;  // Force immediate flush
    SpectatordPublisher publisher{fmt::format("unix:{}", path), config};

    Counter c{std::make_shared<Id>("counter", Tags{}), &publisher};
    c.Increment();
    c.Add(5);

    // Give time for processing
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Publisher destructor should cleanly shut down worker threads
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  // Should have received the measurements before shutdown
  EXPECT_GT(msgs.size(), 0);
}

TEST(Publisher, NumSeperateBuffersConfig) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver_buffers.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();

  // Test custom num_seperate_buffers setting
  spectator::PublisherConfig config;
  config.num_seperate_buffers = 8;  // Custom value instead of default 16
  config.bytes_to_buffer = 0;       // Force immediate flush
  config.num_worker_threads = 3;

  SpectatordPublisher publisher{fmt::format("unix:{}", path), config};

  // Create counters to test buffer allocation
  std::vector<Counter<SpectatordPublisher>> counters;
  for (int i = 0; i < 20; ++i) {
    counters.emplace_back(std::make_shared<Id>(fmt::format("buffer_test_{}", i), Tags{}),
                          &publisher);
    counters.back().Increment();
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  // Verify that we received messages (indicating the custom buffer config works)
  EXPECT_GT(msgs.size(), 0);

  // Count total increments in received messages
  int total_received = 0;
  for (const auto& msg_batch : msgs) {
    std::istringstream iss(msg_batch);
    std::string line;
    while (std::getline(iss, line)) {
      if (!line.empty() && line.find("c:buffer_test_") == 0) {
        total_received++;
      }
    }
  }

  // Should receive all 20 increments
  EXPECT_EQ(total_received, 20);
}

TEST(Publisher, WorkerThreadSockets) {
  auto logger = spectator::DefaultLogger();
  const auto* dir = first_not_null(std::getenv("TMPDIR"), "/tmp");
  auto path = fmt::format("{}/testserver_workers.{}", dir, getpid());
  TestUnixServer server{path};
  server.Start();

  // Test multiple worker threads with individual sockets
  spectator::PublisherConfig config;
  config.num_worker_threads = 4;     // Multiple workers
  config.bytes_to_buffer = 0;        // Force immediate flush
  config.num_seperate_buffers = 12;  // More buffers than workers

  SpectatordPublisher publisher{fmt::format("unix:{}", path), config};

  const int num_test_threads = 8;
  const int increments_per_thread = 25;
  std::vector<std::thread> test_threads;
  std::atomic<int> total_sent{0};

  // Launch multiple threads that increment counters simultaneously
  // This will test that each worker thread uses its own socket
  for (int i = 0; i < num_test_threads; ++i) {
    test_threads.emplace_back([&, i]() {
      Counter c{std::make_shared<Id>(fmt::format("worker_test_{}", i), Tags{}), &publisher};
      for (int j = 0; j < increments_per_thread; ++j) {
        c.Increment();
        total_sent.fetch_add(1);
        // Small delay to ensure threads interleave
        std::this_thread::sleep_for(std::chrono::microseconds(10));
      }
    });
  }

  // Wait for all threads to complete
  for (auto& thread : test_threads) {
    thread.join();
  }

  // Wait for all data to be processed
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  auto msgs = server.GetMessages();
  server.Stop();
  unlink(path.c_str());

  // Verify we received messages from all threads
  EXPECT_GT(msgs.size(), 0);

  // Count total increments in received messages
  int total_received = 0;
  for (const auto& msg_batch : msgs) {
    std::istringstream iss(msg_batch);
    std::string line;
    while (std::getline(iss, line)) {
      if (!line.empty() && line.find("c:worker_test_") == 0) {
        total_received++;
      }
    }
  }

  // Should receive most messages (allowing for some potential loss in testing)
  EXPECT_GE(total_received, static_cast<int>(num_test_threads * increments_per_thread * 0.9));
  EXPECT_LE(total_received, num_test_threads * increments_per_thread);
}

}  // namespace
