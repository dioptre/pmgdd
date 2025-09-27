/**
 * @file   CapnProtoGraphImpl.h
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
#include <unordered_map>
#include <string>
#include <mutex>

#include "CapnProtoDatabase.h"
#include "BatchManager.h"
#include "GraphImpl.h"
#include "TransactionManager.h"
#include "IndexManager.h"
#include "StringTable.h"
#include "os.h"
#include "lock.h"

namespace PMGD {

class CapnProtoGraphImpl {
public:
    struct LegacyCompatibilityLayer {
        std::unordered_map<uint64_t, void*> node_ptr_cache;
        std::unordered_map<uint64_t, void*> edge_ptr_cache;
        std::mutex cache_mutex;
        
        void* get_node_ptr(uint64_t node_id);
        void* get_edge_ptr(uint64_t edge_id);
        void invalidate_cache();
    };

    struct MigrationStats {
        std::atomic<uint64_t> nodes_migrated{0};
        std::atomic<uint64_t> edges_migrated{0};
        std::atomic<uint64_t> properties_migrated{0};
        std::atomic<uint64_t> indices_migrated{0};
        std::chrono::system_clock::time_point migration_start;
        std::chrono::system_clock::time_point migration_end;
        std::vector<std::string> migration_errors;
    };

private:
    std::unique_ptr<CapnProtoDatabase> _capnp_db;
    std::unique_ptr<BatchManager> _batch_manager;
    
    // Legacy compatibility
    std::unique_ptr<LegacyCompatibilityLayer> _legacy_layer;
    std::unique_ptr<MigrationStats> _migration_stats;
    
    // Configuration
    std::string _db_path;
    bool _read_only;
    bool _enable_batching;
    bool _legacy_compatibility_mode;
    
    // Synchronization
    mutable std::shared_mutex _db_mutex;
    StripedLock _node_locks;
    StripedLock _edge_locks;
    StripedLock _index_locks;
    
    // Migration and compatibility methods
    void initialize_legacy_compatibility();
    void migrate_from_legacy_database();
    void convert_legacy_node_format(const void* legacy_node_data);
    void convert_legacy_edge_format(const void* legacy_edge_data);
    void convert_legacy_index_format(const void* legacy_index_data);
    
    // Transaction support
    struct TransactionContext {
        uint64_t tx_id;
        std::vector<uint64_t> created_nodes;
        std::vector<uint64_t> created_edges;
        std::vector<std::pair<uint64_t, std::string>> modified_properties;
        bool auto_commit;
    };
    
    std::unordered_map<std::thread::id, std::unique_ptr<TransactionContext>> _active_transactions;
    std::mutex _transaction_mutex;
    
public:
    CapnProtoGraphImpl(const char* name, int options, const Graph::Config* config);
    ~CapnProtoGraphImpl();

    // Node operations - compatible with existing PMGD API
    Node& create_node(const std::string& tag = "");
    Node& create_node_batch(const std::string& tag = "");
    Node* find_node(uint64_t node_id);
    const Node* find_node(uint64_t node_id) const;
    bool delete_node(Node& node);
    bool delete_node_batch(Node& node);
    
    // Edge operations - compatible with existing PMGD API  
    Edge& create_edge(Node& src, Node& dst, const std::string& tag = "");
    Edge& create_edge_batch(Node& src, Node& dst, const std::string& tag = "");
    Edge* find_edge(uint64_t edge_id);
    const Edge* find_edge(uint64_t edge_id) const;
    bool delete_edge(Edge& edge);
    bool delete_edge_batch(Edge& edge);
    
    // Property operations
    void set_property(Node& node, const std::string& key, const Property& value);
    void set_property(Edge& edge, const std::string& key, const Property& value);
    void set_property_batch(Node& node, const std::string& key, const Property& value);
    void set_property_batch(Edge& edge, const std::string& key, const Property& value);
    
    Property get_property(const Node& node, const std::string& key) const;
    Property get_property(const Edge& edge, const std::string& key) const;
    
    void remove_property(Node& node, const std::string& key);
    void remove_property(Edge& edge, const std::string& key);
    
    // Iterator support for compatibility
    class NodeIterator;
    class EdgeIterator;
    
    NodeIterator nodes_begin();
    NodeIterator nodes_end();
    EdgeIterator edges_begin();
    EdgeIterator edges_end();
    
    // Transaction management
    void begin_transaction();
    void commit_transaction();
    void rollback_transaction();
    bool in_transaction() const;
    
    // Batch operations
    void begin_batch();
    void commit_batch();
    void rollback_batch();
    void set_batch_mode(BatchManager::BatchMode mode);
    
    // Index operations
    void create_node_index(const std::string& tag, const std::string& property = "");
    void create_edge_index(const std::string& tag, const std::string& property = "");
    void drop_index(const std::string& index_name);
    
    std::vector<Node*> find_nodes_by_tag(const std::string& tag);
    std::vector<Edge*> find_edges_by_tag(const std::string& tag);
    std::vector<Node*> find_nodes_by_property(const std::string& key, const Property& value);
    std::vector<Edge*> find_edges_by_property(const std::string& key, const Property& value);
    
    // Persistence and synchronization
    void sync();
    void checkpoint();
    void flush();
    
    // Legacy compatibility
    void enable_legacy_compatibility(bool enable = true);
    bool is_legacy_compatible() const { return _legacy_compatibility_mode; }
    
    // Migration utilities
    void migrate_from_legacy(const std::string& legacy_db_path);
    void export_to_legacy_format(const std::string& output_path);
    const MigrationStats& get_migration_stats() const { return *_migration_stats; }
    
    // Statistics and monitoring
    size_t get_node_count() const;
    size_t get_edge_count() const;
    size_t get_memory_usage() const;
    std::string get_database_info() const;
    
    // Access to underlying components for compatibility
    CapnProtoDatabase* get_capnp_database() { return _capnp_db.get(); }
    BatchManager* get_batch_manager() { return _batch_manager.get(); }
    
    // Darwin/Mac optimizations
    void enable_mac_optimizations();
    
    // Debugging and validation
    void validate_database();
    void dump_database_stats(const std::string& output_file);
};

// Iterator implementations for API compatibility
class CapnProtoGraphImpl::NodeIterator {
private:
    CapnProtoGraphImpl* _graph;
    uint64_t _current_index;
    std::vector<uint64_t> _node_ids;
    
public:
    NodeIterator(CapnProtoGraphImpl* graph, uint64_t index = 0);
    
    Node* operator*();
    NodeIterator& operator++();
    NodeIterator operator++(int);
    bool operator==(const NodeIterator& other) const;
    bool operator!=(const NodeIterator& other) const;
};

class CapnProtoGraphImpl::EdgeIterator {
private:
    CapnProtoGraphImpl* _graph;
    uint64_t _current_index;
    std::vector<uint64_t> _edge_ids;
    
public:
    EdgeIterator(CapnProtoGraphImpl* graph, uint64_t index = 0);
    
    Edge* operator*();
    EdgeIterator& operator++();
    EdgeIterator operator++(int);
    bool operator==(const EdgeIterator& other) const;
    bool operator!=(const EdgeIterator& other) const;
};

// Factory function to replace the old GraphImpl
std::unique_ptr<CapnProtoGraphImpl> create_capnproto_graph(const char* name, int options, const Graph::Config* config = nullptr);

// Conversion utilities between old PMGD types and CapnProto
namespace CapnProtoConverters {
    Schema::Property::Value::Builder property_to_capnp(capnp::MessageBuilder& builder, const Property& prop);
    Property capnp_to_property(const Schema::Property::Value::Reader& capnp_prop);
    
    std::string node_to_capnp_tag(const Node& node);
    std::string edge_to_capnp_tag(const Edge& edge);
}

} // namespace PMGD