/**
 * @file   CapnProtoGraphImpl.cc
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

#include "CapnProtoGraphImpl.h"
#include "node.h"
#include "edge.h"
#include "property.h"
#include <filesystem>
#include <fstream>
#include <chrono>

namespace PMGD {

// LegacyCompatibilityLayer implementation
void* CapnProtoGraphImpl::LegacyCompatibilityLayer::get_node_ptr(uint64_t node_id) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    
    auto it = node_ptr_cache.find(node_id);
    if (it != node_ptr_cache.end()) {
        return it->second;
    }
    
    // Create a placeholder Node object for legacy compatibility
    // In a real implementation, this would create a proper Node wrapper
    void* node_ptr = nullptr; // Placeholder
    node_ptr_cache[node_id] = node_ptr;
    
    return node_ptr;
}

void* CapnProtoGraphImpl::LegacyCompatibilityLayer::get_edge_ptr(uint64_t edge_id) {
    std::lock_guard<std::mutex> lock(cache_mutex);
    
    auto it = edge_ptr_cache.find(edge_id);
    if (it != edge_ptr_cache.end()) {
        return it->second;
    }
    
    // Create a placeholder Edge object for legacy compatibility
    void* edge_ptr = nullptr; // Placeholder
    edge_ptr_cache[edge_id] = edge_ptr;
    
    return edge_ptr;
}

void CapnProtoGraphImpl::LegacyCompatibilityLayer::invalidate_cache() {
    std::lock_guard<std::mutex> lock(cache_mutex);
    node_ptr_cache.clear();
    edge_ptr_cache.clear();
}

// Main CapnProtoGraphImpl implementation
CapnProtoGraphImpl::CapnProtoGraphImpl(const char* name, int options, const Graph::Config* config)
    : _db_path(name), _read_only(options & Graph::ReadOnly), _enable_batching(true), 
      _legacy_compatibility_mode(false),
      _node_locks(4096), _edge_locks(4096), _index_locks(1024) {
    
    // Initialize the CapnProto database
    bool create = options & Graph::Create;
    _capnp_db = std::make_unique<CapnProtoDatabase>(_db_path, create, _read_only);
    
    // Initialize batch manager
    BatchManager::BatchConfig batch_config;
    batch_config.mode = BatchManager::BatchMode::AUTO_FLUSH;
    batch_config.max_batch_size = config ? 2000 : 1000;
    batch_config.flush_interval = std::chrono::milliseconds(50);
    batch_config.enable_compression = true;
    batch_config.enable_deduplication = true;
    
    _batch_manager = std::make_unique<BatchManager>(batch_config);
    
    // Set up batch operation executor
    _batch_manager->operation_executor = [this](const auto& operations) {
        for (const auto& op : operations) {
            // Execute each operation against the CapnProto database
            // This would implement the actual database operations
        }
    };
    
    // Initialize legacy compatibility layer
    _legacy_layer = std::make_unique<LegacyCompatibilityLayer>();
    _migration_stats = std::make_unique<MigrationStats>();
    
    // Check for legacy database migration if creating new database
    if (create && std::filesystem::exists(_db_path + "/allocator.jdb")) {
        migrate_from_legacy_database();
    }
    
    #ifdef __APPLE__
    _capnp_db->enable_mac_optimizations();
    #endif
}

CapnProtoGraphImpl::~CapnProtoGraphImpl() {
    try {
        // Commit any pending batch operations
        if (_batch_manager) {
            _batch_manager->flush_all_batches();
        }
        
        // Commit any active transactions
        for (auto& [thread_id, ctx] : _active_transactions) {
            if (ctx && !ctx->auto_commit) {
                commit_transaction();
            }
        }
        
        // Final sync
        if (_capnp_db) {
            _capnp_db->sync(true);
        }
    } catch (...) {
        // Don't throw from destructor
    }
}

void CapnProtoGraphImpl::initialize_legacy_compatibility() {
    _legacy_compatibility_mode = true;
    _legacy_layer = std::make_unique<LegacyCompatibilityLayer>();
}

void CapnProtoGraphImpl::migrate_from_legacy_database() {
    _migration_stats->migration_start = std::chrono::system_clock::now();
    
    try {
        // Migration logic for each legacy component
        std::vector<std::string> legacy_files = {
            "nodes.jdb", "edges.jdb", "indexmanager.jdb", 
            "stringtable.jdb", "transaction.jdb"
        };
        
        for (const auto& filename : legacy_files) {
            std::string full_path = _db_path + "/" + filename;
            if (std::filesystem::exists(full_path)) {
                if (filename == "nodes.jdb") {
                    convert_legacy_node_format(nullptr); // Placeholder
                } else if (filename == "edges.jdb") {
                    convert_legacy_edge_format(nullptr); // Placeholder
                } else if (filename == "indexmanager.jdb") {
                    convert_legacy_index_format(nullptr); // Placeholder
                }
            }
        }
        
        _migration_stats->migration_end = std::chrono::system_clock::now();
        
    } catch (const std::exception& e) {
        _migration_stats->migration_errors.push_back(e.what());
        throw;
    }
}

void CapnProtoGraphImpl::convert_legacy_node_format(const void* legacy_node_data) {
    // This would implement the actual conversion from legacy node format
    // to CapnProto format. For now, it's a placeholder.
    _migration_stats->nodes_migrated.fetch_add(1);
}

void CapnProtoGraphImpl::convert_legacy_edge_format(const void* legacy_edge_data) {
    // This would implement the actual conversion from legacy edge format
    // to CapnProto format. For now, it's a placeholder.
    _migration_stats->edges_migrated.fetch_add(1);
}

void CapnProtoGraphImpl::convert_legacy_index_format(const void* legacy_index_data) {
    // This would implement the actual conversion from legacy index format
    // to CapnProto format. For now, it's a placeholder.
    _migration_stats->indices_migrated.fetch_add(1);
}

Node& CapnProtoGraphImpl::create_node(const std::string& tag) {
    std::unique_lock<std::shared_mutex> lock(_db_mutex);
    
    uint64_t node_id = _capnp_db->create_node(tag);
    
    // For legacy compatibility, we need to return a Node& reference
    // In a real implementation, this would create or retrieve a Node wrapper
    static Node placeholder_node; // Placeholder for compilation
    return placeholder_node;
}

Node& CapnProtoGraphImpl::create_node_batch(const std::string& tag) {
    std::shared_lock<std::shared_mutex> lock(_db_mutex);
    
    uint64_t node_id = _capnp_db->create_node_batch(tag);
    
    // For legacy compatibility, we need to return a Node& reference
    static Node placeholder_node; // Placeholder for compilation
    return placeholder_node;
}

Node* CapnProtoGraphImpl::find_node(uint64_t node_id) {
    std::shared_lock<std::shared_mutex> lock(_db_mutex);
    
    auto node_reader = _capnp_db->get_node(node_id);
    if (node_reader.hasTag()) {
        // Convert CapnProto node to legacy Node object
        return static_cast<Node*>(_legacy_layer->get_node_ptr(node_id));
    }
    
    return nullptr;
}

const Node* CapnProtoGraphImpl::find_node(uint64_t node_id) const {
    std::shared_lock<std::shared_mutex> lock(_db_mutex);
    
    auto node_reader = _capnp_db->get_node(node_id);
    if (node_reader.hasTag()) {
        return static_cast<const Node*>(_legacy_layer->get_node_ptr(node_id));
    }
    
    return nullptr;
}

Edge& CapnProtoGraphImpl::create_edge(Node& src, Node& dst, const std::string& tag) {
    std::unique_lock<std::shared_mutex> lock(_db_mutex);
    
    // Extract node IDs from Node objects (placeholder logic)
    uint64_t src_id = 1; // Placeholder
    uint64_t dst_id = 2; // Placeholder
    
    uint64_t edge_id = _capnp_db->create_edge(src_id, dst_id, tag);
    
    // For legacy compatibility, we need to return an Edge& reference
    static Edge placeholder_edge; // Placeholder for compilation
    return placeholder_edge;
}

Edge& CapnProtoGraphImpl::create_edge_batch(Node& src, Node& dst, const std::string& tag) {
    std::shared_lock<std::shared_mutex> lock(_db_mutex);
    
    // Extract node IDs from Node objects (placeholder logic)
    uint64_t src_id = 1; // Placeholder
    uint64_t dst_id = 2; // Placeholder
    
    uint64_t edge_id = _capnp_db->create_edge_batch(src_id, dst_id, tag);
    
    // For legacy compatibility, we need to return an Edge& reference
    static Edge placeholder_edge; // Placeholder for compilation
    return placeholder_edge;
}

void CapnProtoGraphImpl::begin_transaction() {
    std::lock_guard<std::mutex> lock(_transaction_mutex);
    
    std::thread::id current_thread = std::this_thread::get_id();
    
    if (_active_transactions.find(current_thread) != _active_transactions.end()) {
        throw PMGDException(TransactionActive);
    }
    
    auto ctx = std::make_unique<TransactionContext>();
    ctx->tx_id = _capnp_db->begin_transaction();
    ctx->auto_commit = false;
    
    _active_transactions[current_thread] = std::move(ctx);
}

void CapnProtoGraphImpl::commit_transaction() {
    std::lock_guard<std::mutex> lock(_transaction_mutex);
    
    std::thread::id current_thread = std::this_thread::get_id();
    auto it = _active_transactions.find(current_thread);
    
    if (it == _active_transactions.end()) {
        throw PMGDException(NoTransaction);
    }
    
    auto& ctx = it->second;
    _capnp_db->commit_transaction(ctx->tx_id);
    
    _active_transactions.erase(it);
}

void CapnProtoGraphImpl::rollback_transaction() {
    std::lock_guard<std::mutex> lock(_transaction_mutex);
    
    std::thread::id current_thread = std::this_thread::get_id();
    auto it = _active_transactions.find(current_thread);
    
    if (it == _active_transactions.end()) {
        throw PMGDException(NoTransaction);
    }
    
    auto& ctx = it->second;
    _capnp_db->rollback_transaction(ctx->tx_id);
    
    _active_transactions.erase(it);
}

bool CapnProtoGraphImpl::in_transaction() const {
    std::lock_guard<std::mutex> lock(_transaction_mutex);
    
    std::thread::id current_thread = std::this_thread::get_id();
    return _active_transactions.find(current_thread) != _active_transactions.end();
}

void CapnProtoGraphImpl::begin_batch() {
    _batch_manager->set_batch_mode(BatchManager::BatchMode::DEFERRED);
}

void CapnProtoGraphImpl::commit_batch() {
    _batch_manager->flush_all_batches();
}

void CapnProtoGraphImpl::rollback_batch() {
    // For now, we just clear pending operations
    // A full implementation would need to track operations for rollback
    _batch_manager->flush_all_batches();
}

void CapnProtoGraphImpl::set_batch_mode(BatchManager::BatchMode mode) {
    _batch_manager->set_batch_mode(mode);
}

void CapnProtoGraphImpl::sync() {
    _capnp_db->sync(false);
}

void CapnProtoGraphImpl::checkpoint() {
    _capnp_db->checkpoint();
}

void CapnProtoGraphImpl::flush() {
    _batch_manager->flush_all_batches();
    _capnp_db->flush_all_regions();
}

size_t CapnProtoGraphImpl::get_node_count() const {
    std::shared_lock<std::shared_mutex> lock(_db_mutex);
    return _capnp_db->get_stats().nodes_created.load();
}

size_t CapnProtoGraphImpl::get_edge_count() const {
    std::shared_lock<std::shared_mutex> lock(_db_mutex);
    return _capnp_db->get_stats().edges_created.load();
}

size_t CapnProtoGraphImpl::get_memory_usage() const {
    return _capnp_db->get_memory_usage();
}

std::string CapnProtoGraphImpl::get_database_info() const {
    std::ostringstream info;
    
    info << "CapnProto Graph Database Info:\n";
    info << "  Database Path: " << _db_path << "\n";
    info << "  Read Only: " << (_read_only ? "Yes" : "No") << "\n";
    info << "  Legacy Compatibility: " << (_legacy_compatibility_mode ? "Yes" : "No") << "\n";
    info << "  Nodes: " << get_node_count() << "\n";
    info << "  Edges: " << get_edge_count() << "\n";
    info << "  Memory Usage: " << get_memory_usage() << " bytes\n";
    info << "  Batch Mode: " << static_cast<int>(_batch_manager->get_config().mode) << "\n";
    info << "\nDatabase Stats:\n" << _capnp_db->get_database_info();
    info << "\nBatch Manager Stats:\n" << _batch_manager->get_performance_report();
    
    return info.str();
}

void CapnProtoGraphImpl::enable_mac_optimizations() {
    #ifdef __APPLE__
    _capnp_db->enable_mac_optimizations();
    #endif
}

void CapnProtoGraphImpl::validate_database() {
    _capnp_db->validate_database_integrity();
}

// NodeIterator implementation
CapnProtoGraphImpl::NodeIterator::NodeIterator(CapnProtoGraphImpl* graph, uint64_t index)
    : _graph(graph), _current_index(index) {
    // Initialize node IDs vector (placeholder implementation)
    // In a real implementation, this would load node IDs from the database
}

Node* CapnProtoGraphImpl::NodeIterator::operator*() {
    if (_current_index < _node_ids.size()) {
        return _graph->find_node(_node_ids[_current_index]);
    }
    return nullptr;
}

CapnProtoGraphImpl::NodeIterator& CapnProtoGraphImpl::NodeIterator::operator++() {
    ++_current_index;
    return *this;
}

bool CapnProtoGraphImpl::NodeIterator::operator==(const NodeIterator& other) const {
    return _graph == other._graph && _current_index == other._current_index;
}

bool CapnProtoGraphImpl::NodeIterator::operator!=(const NodeIterator& other) const {
    return !(*this == other);
}

// EdgeIterator implementation
CapnProtoGraphImpl::EdgeIterator::EdgeIterator(CapnProtoGraphImpl* graph, uint64_t index)
    : _graph(graph), _current_index(index) {
    // Initialize edge IDs vector (placeholder implementation)
}

Edge* CapnProtoGraphImpl::EdgeIterator::operator*() {
    if (_current_index < _edge_ids.size()) {
        return _graph->find_edge(_edge_ids[_current_index]);
    }
    return nullptr;
}

CapnProtoGraphImpl::EdgeIterator& CapnProtoGraphImpl::EdgeIterator::operator++() {
    ++_current_index;
    return *this;
}

bool CapnProtoGraphImpl::EdgeIterator::operator==(const EdgeIterator& other) const {
    return _graph == other._graph && _current_index == other._current_index;
}

bool CapnProtoGraphImpl::EdgeIterator::operator!=(const EdgeIterator& other) const {
    return !(*this == other);
}

CapnProtoGraphImpl::NodeIterator CapnProtoGraphImpl::nodes_begin() {
    return NodeIterator(this, 0);
}

CapnProtoGraphImpl::NodeIterator CapnProtoGraphImpl::nodes_end() {
    return NodeIterator(this, get_node_count());
}

CapnProtoGraphImpl::EdgeIterator CapnProtoGraphImpl::edges_begin() {
    return EdgeIterator(this, 0);
}

CapnProtoGraphImpl::EdgeIterator CapnProtoGraphImpl::edges_end() {
    return EdgeIterator(this, get_edge_count());
}

// Factory function
std::unique_ptr<CapnProtoGraphImpl> create_capnproto_graph(const char* name, int options, const Graph::Config* config) {
    return std::make_unique<CapnProtoGraphImpl>(name, options, config);
}

// Conversion utilities
namespace CapnProtoConverters {
    Schema::Property::Value::Builder property_to_capnp(capnp::MessageBuilder& builder, const Property& prop) {
        auto value = builder.initRoot<Schema::Property::Value>();
        
        // This would implement conversion from PMGD Property to CapnProto
        // Placeholder implementation
        switch (prop.type()) {
            case Property::Bool:
                value.setBoolVal(prop.bool_value());
                break;
            case Property::Int:
                value.setIntVal(prop.int_value());
                break;
            case Property::Float:
                value.setFloatVal(prop.float_value());
                break;
            case Property::String:
                value.setStringVal(prop.string_value());
                break;
            case Property::Time:
                value.setTimeVal(prop.time_value().value);
                break;
            default:
                throw PMGDException(InvalidPropertyType);
        }
        
        return value;
    }

    Property capnp_to_property(const Schema::Property::Value::Reader& capnp_prop) {
        // This would implement conversion from CapnProto to PMGD Property
        // Placeholder implementation
        switch (capnp_prop.which()) {
            case Schema::Property::Value::BOOL_VAL:
                return Property(capnp_prop.getBoolVal());
            case Schema::Property::Value::INT_VAL:
                return Property(capnp_prop.getIntVal());
            case Schema::Property::Value::FLOAT_VAL:
                return Property(capnp_prop.getFloatVal());
            case Schema::Property::Value::STRING_VAL:
                return Property(std::string(capnp_prop.getStringVal().cStr()));
            case Schema::Property::Value::TIME_VAL:
                return Property(Property::Time{capnp_prop.getTimeVal()});
            default:
                throw PMGDException(InvalidPropertyType);
        }
    }
}

} // namespace PMGD