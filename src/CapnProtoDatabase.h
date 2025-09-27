/**
 * @file   CapnProtoDatabase.h
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
#include <unordered_map>
#include <string>
#include <queue>
#include <mutex>
#include <atomic>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

#include "../schema/pmgd.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <capnp/serialize-packed.h>
#include "../include/exception.h"
#include "GraphConfig.h"
#include "RangeSet.h"

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/vm_map.h>
#endif

namespace PMGD {
    class CapnProtoDatabase {
    public:
        struct MmapRegion {
            void* addr;
            size_t size;
            int fd;
            std::string filename;
            bool writable;
            
            MmapRegion(const std::string& path, size_t size, bool create, bool writable);
            ~MmapRegion();
            
            void flush(size_t offset = 0, size_t length = 0);
            void sync();
        };

        struct BatchContext {
            std::vector<Schema::Operation::Builder> operations;
            std::atomic<size_t> operation_count{0};
            std::mutex batch_mutex;
            bool auto_flush;
            size_t max_batch_size;
            
            BatchContext(size_t max_size = 1000, bool auto_flush = true) 
                : auto_flush(auto_flush), max_batch_size(max_size) {}
        };

        struct DatabaseStats {
            std::atomic<uint64_t> nodes_created{0};
            std::atomic<uint64_t> edges_created{0};
            std::atomic<uint64_t> transactions_committed{0};
            std::atomic<uint64_t> batch_operations{0};
            std::atomic<uint64_t> sync_operations{0};
        };

    private:
        std::string _db_path;
        std::unique_ptr<MmapRegion> _main_region;
        std::unique_ptr<MmapRegion> _index_region;
        std::unique_ptr<MmapRegion> _transaction_region;
        
        capnp::MallocMessageBuilder _message_builder;
        Schema::Database::Builder _db_builder;
        
        std::unique_ptr<BatchContext> _batch_context;
        DatabaseStats _stats;
        
        bool _read_only;
        std::atomic<uint64_t> _next_node_id{1};
        std::atomic<uint64_t> _next_edge_id{1};
        std::atomic<uint64_t> _next_transaction_id{1};
        
        std::mutex _write_mutex;
        RangeSet _pending_commits;

        // Darwin-specific mmap optimizations
        #ifdef __APPLE__
        void setup_darwin_mmap_optimizations();
        void* create_darwin_mmap(const std::string& path, size_t size, bool create, bool writable);
        #endif

        void initialize_database(bool create);
        void load_from_legacy_format(const std::string& legacy_path);
        void migrate_legacy_files();
        
        Schema::Node::Builder create_node_builder(uint64_t id, const std::string& tag);
        Schema::Edge::Builder create_edge_builder(uint64_t id, const std::string& tag, uint64_t src, uint64_t dst);
        
        void flush_batch_if_needed();
        void commit_batch();
        
    public:
        CapnProtoDatabase(const std::string& db_path, bool create = false, bool read_only = false);
        ~CapnProtoDatabase();

        // Node operations with batching support
        uint64_t create_node(const std::string& tag);
        uint64_t create_node_batch(const std::string& tag);
        bool delete_node(uint64_t node_id);
        bool delete_node_batch(uint64_t node_id);
        Schema::Node::Reader get_node(uint64_t node_id);
        
        // Edge operations with batching support  
        uint64_t create_edge(uint64_t src, uint64_t dst, const std::string& tag);
        uint64_t create_edge_batch(uint64_t src, uint64_t dst, const std::string& tag);
        bool delete_edge(uint64_t edge_id);
        bool delete_edge_batch(uint64_t edge_id);
        Schema::Edge::Reader get_edge(uint64_t edge_id);
        
        // Property operations
        void set_node_property(uint64_t node_id, const std::string& key, const Schema::Property::Value::Reader& value);
        void set_edge_property(uint64_t edge_id, const std::string& key, const Schema::Property::Value::Reader& value);
        void set_node_property_batch(uint64_t node_id, const std::string& key, const Schema::Property::Value::Reader& value);
        void set_edge_property_batch(uint64_t edge_id, const std::string& key, const Schema::Property::Value::Reader& value);
        
        // Transaction support
        uint64_t begin_transaction();
        void commit_transaction(uint64_t tx_id);
        void rollback_transaction(uint64_t tx_id);
        
        // Batch operations
        void begin_batch(size_t max_size = 1000);
        void commit_batch();
        void rollback_batch();
        size_t get_batch_size() const { return _batch_context->operation_count.load(); }
        
        // Persistence and sync
        void sync(bool force = false);
        void flush_all_regions();
        void checkpoint();
        
        // Compatibility with old format
        void import_legacy_database(const std::string& legacy_db_path);
        void export_to_legacy_format(const std::string& output_path);
        
        // Statistics and monitoring
        const DatabaseStats& get_stats() const { return _stats; }
        size_t get_memory_usage() const;
        std::string get_database_info() const;
        
        // Darwin/Mac specific optimizations
        void enable_mac_optimizations();
        void configure_memory_pressure_handling();
        
        // Debugging and diagnostics
        void validate_database_integrity();
        void dump_database_structure(const std::string& output_file);
    };
    
    // Helper functions for property value conversion
    namespace PropertyHelpers {
        Schema::Property::Value::Builder create_bool_value(capnp::MessageBuilder& builder, bool value);
        Schema::Property::Value::Builder create_int_value(capnp::MessageBuilder& builder, int64_t value);
        Schema::Property::Value::Builder create_float_value(capnp::MessageBuilder& builder, double value);
        Schema::Property::Value::Builder create_string_value(capnp::MessageBuilder& builder, const std::string& value);
        Schema::Property::Value::Builder create_time_value(capnp::MessageBuilder& builder, uint64_t timestamp);
        Schema::Property::Value::Builder create_blob_value(capnp::MessageBuilder& builder, const void* data, size_t size);
    }
}