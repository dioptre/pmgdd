/**
 * @file   BatchManager.h
 *
 * @section LICENSE
 *
 * The MIT License
 *
 * @copyright Copyright (c) 2024 Intel Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 */

#pragma once

#include <memory>
#include <vector>
#include <queue>
#include <unordered_set>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <thread>
#include <future>

#include "../schema/pmgd.capnp.h"
#include <capnp/message.h>
#include "../include/exception.h"

namespace PMGD {

class BatchManager {
public:
    enum class BatchMode {
        IMMEDIATE,      // Execute operations immediately
        DEFERRED,       // Collect operations and execute later
        AUTO_FLUSH,     // Auto-flush when batch size reached
        TIME_BASED      // Flush based on time intervals
    };

    enum class BatchPriority {
        LOW = 0,
        NORMAL = 1,
        HIGH = 2,
        CRITICAL = 3
    };

    struct BatchConfig {
        BatchMode mode = BatchMode::AUTO_FLUSH;
        size_t max_batch_size = 1000;
        std::chrono::milliseconds flush_interval{100};
        size_t max_memory_usage = SIZE_64MB;
        bool enable_compression = true;
        bool enable_deduplication = true;
        BatchPriority default_priority = BatchPriority::NORMAL;
    };

    struct BatchStats {
        std::atomic<uint64_t> operations_batched{0};
        std::atomic<uint64_t> batches_flushed{0};
        std::atomic<uint64_t> operations_deduplicated{0};
        std::atomic<uint64_t> bytes_saved_compression{0};
        std::atomic<uint64_t> total_flush_time_us{0};
        std::atomic<uint64_t> current_memory_usage{0};
    };

    struct BatchOperation {
        uint64_t id;
        BatchPriority priority;
        std::chrono::system_clock::time_point timestamp;
        capnp::MallocMessageBuilder message_builder;
        Schema::Operation::Builder operation;
        size_t estimated_size;
        
        BatchOperation(BatchPriority prio = BatchPriority::NORMAL);
    };

    class Batch {
    private:
        std::vector<std::unique_ptr<BatchOperation>> _operations;
        std::unordered_set<std::string> _operation_hashes;
        std::mutex _batch_mutex;
        std::atomic<size_t> _estimated_size{0};
        BatchPriority _priority;
        
    public:
        Batch(BatchPriority priority = BatchPriority::NORMAL);
        
        bool add_operation(std::unique_ptr<BatchOperation> op, bool deduplicate = true);
        size_t size() const { return _operations.size(); }
        size_t estimated_memory_size() const { return _estimated_size.load(); }
        BatchPriority priority() const { return _priority; }
        
        const std::vector<std::unique_ptr<BatchOperation>>& operations() const { return _operations; }
        void clear();
        
        std::string hash_operation(const BatchOperation& op) const;
    };

private:
    BatchConfig _config;
    BatchStats _stats;
    
    std::queue<std::shared_ptr<Batch>> _batch_queue;
    std::unordered_map<BatchPriority, std::shared_ptr<Batch>> _active_batches;
    
    std::mutex _queue_mutex;
    std::condition_variable _flush_condition;
    std::atomic<bool> _stop_flush_thread{false};
    std::thread _flush_thread;
    
    std::atomic<uint64_t> _next_operation_id{1};
    
    void flush_thread_worker();
    void flush_batch(std::shared_ptr<Batch> batch);
    void compress_batch_data(std::shared_ptr<Batch> batch);
    bool should_auto_flush() const;
    
public:
    BatchManager(const BatchConfig& config = BatchConfig{});
    ~BatchManager();

    // Operation queuing
    uint64_t queue_create_node(const std::string& tag, BatchPriority priority = BatchPriority::NORMAL);
    uint64_t queue_create_edge(uint64_t src, uint64_t dst, const std::string& tag, BatchPriority priority = BatchPriority::NORMAL);
    uint64_t queue_delete_node(uint64_t node_id, BatchPriority priority = BatchPriority::NORMAL);
    uint64_t queue_delete_edge(uint64_t edge_id, BatchPriority priority = BatchPriority::NORMAL);
    uint64_t queue_set_property(uint64_t target_id, bool is_node, const std::string& key, 
                               const Schema::Property::Value::Reader& value, BatchPriority priority = BatchPriority::NORMAL);

    // Batch control
    void flush_all_batches();
    void flush_batch_by_priority(BatchPriority priority);
    void flush_if_needed();
    
    std::future<void> flush_async();
    void wait_for_flush();
    
    // Configuration
    void set_config(const BatchConfig& config);
    const BatchConfig& get_config() const { return _config; }
    
    // Statistics and monitoring
    const BatchStats& get_stats() const { return _stats; }
    size_t get_queue_size() const;
    size_t get_pending_operations() const;
    std::string get_performance_report() const;
    
    // Advanced features
    void set_batch_mode(BatchMode mode);
    void enable_compression(bool enable) { _config.enable_compression = enable; }
    void enable_deduplication(bool enable) { _config.enable_deduplication = enable; }
    
    void pause_auto_flush();
    void resume_auto_flush();
    
    // Callback for actual database operations
    std::function<void(const std::vector<std::unique_ptr<BatchOperation>>&)> operation_executor;
};

// Helper classes for specific batch operation types
class NodeBatchBuilder {
public:
    struct NodeBatchOp {
        std::string tag;
        std::vector<std::pair<std::string, Schema::Property::Value::Reader>> properties;
        BatchPriority priority = BatchPriority::NORMAL;
    };

private:
    std::vector<NodeBatchOp> _nodes;
    BatchManager* _batch_manager;

public:
    NodeBatchBuilder(BatchManager* manager) : _batch_manager(manager) {}
    
    NodeBatchBuilder& add_node(const std::string& tag, BatchPriority priority = BatchPriority::NORMAL);
    NodeBatchBuilder& with_property(const std::string& key, bool value);
    NodeBatchBuilder& with_property(const std::string& key, int64_t value);
    NodeBatchBuilder& with_property(const std::string& key, const std::string& value);
    NodeBatchBuilder& with_priority(BatchPriority priority);
    
    std::vector<uint64_t> execute();
};

class EdgeBatchBuilder {
public:
    struct EdgeBatchOp {
        uint64_t src;
        uint64_t dst;
        std::string tag;
        std::vector<std::pair<std::string, Schema::Property::Value::Reader>> properties;
        BatchPriority priority = BatchPriority::NORMAL;
    };

private:
    std::vector<EdgeBatchOp> _edges;
    BatchManager* _batch_manager;

public:
    EdgeBatchBuilder(BatchManager* manager) : _batch_manager(manager) {}
    
    EdgeBatchBuilder& add_edge(uint64_t src, uint64_t dst, const std::string& tag, BatchPriority priority = BatchPriority::NORMAL);
    EdgeBatchBuilder& with_property(const std::string& key, bool value);
    EdgeBatchBuilder& with_property(const std::string& key, int64_t value);
    EdgeBatchBuilder& with_property(const std::string& key, const std::string& value);
    EdgeBatchBuilder& with_priority(BatchPriority priority);
    
    std::vector<uint64_t> execute();
};

} // namespace PMGD