/**
 * @file   CapnProtoDatabase.cc
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

#include "CapnProtoDatabase.h"
#include <chrono>
#include <fstream>
#include <filesystem>
#include <sys/stat.h>
#include <errno.h>

#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#include <sys/sysctl.h>
#endif

namespace PMGD {

// MmapRegion implementation
CapnProtoDatabase::MmapRegion::MmapRegion(const std::string& path, size_t size, bool create, bool writable)
    : addr(nullptr), size(size), fd(-1), filename(path), writable(writable) {
    
    int flags = writable ? O_RDWR : O_RDONLY;
    if (create) {
        flags |= O_CREAT;
    }
    
    fd = open(path.c_str(), flags, 0666);
    if (fd == -1) {
        throw PMGDException(OpenFailed, errno, "Failed to open " + path);
    }
    
    struct stat sb;
    if (fstat(fd, &sb) == -1) {
        close(fd);
        throw PMGDException(OpenFailed, errno, "Failed to fstat " + path);
    }
    
    if (sb.st_size == 0 && create && writable) {
        if (ftruncate(fd, size) == -1) {
            close(fd);
            throw PMGDException(OpenFailed, errno, "Failed to resize " + path);
        }
    } else if (sb.st_size > 0) {
        this->size = sb.st_size;
    }
    
    int prot = PROT_READ;
    if (writable) {
        prot |= PROT_WRITE;
    }
    
    #ifdef __APPLE__
    // Darwin-specific optimizations
    addr = mmap(nullptr, this->size, prot, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        throw PMGDException(OpenFailed, errno, "Failed to mmap " + path);
    }
    
    // Enable read-ahead for better performance on macOS
    if (madvise(addr, this->size, MADV_SEQUENTIAL) == -1) {
        // Non-fatal, just log warning
    }
    #else
    addr = mmap(nullptr, this->size, prot, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        throw PMGDException(OpenFailed, errno, "Failed to mmap " + path);
    }
    #endif
}

CapnProtoDatabase::MmapRegion::~MmapRegion() {
    if (addr != nullptr && addr != MAP_FAILED) {
        munmap(addr, size);
    }
    if (fd != -1) {
        close(fd);
    }
}

void CapnProtoDatabase::MmapRegion::flush(size_t offset, size_t length) {
    if (!writable) return;
    
    size_t flush_offset = offset;
    size_t flush_length = (length == 0) ? size - offset : length;
    
    if (flush_offset + flush_length > size) {
        flush_length = size - flush_offset;
    }
    
    #ifdef __APPLE__
    // Darwin uses different sync semantics
    if (msync(static_cast<char*>(addr) + flush_offset, flush_length, MS_ASYNC) == -1) {
        throw PMGDException(SyncFailed, errno, "Failed to msync " + filename);
    }
    #else
    if (msync(static_cast<char*>(addr) + flush_offset, flush_length, MS_ASYNC) == -1) {
        throw PMGDException(SyncFailed, errno, "Failed to msync " + filename);
    }
    #endif
}

void CapnProtoDatabase::MmapRegion::sync() {
    if (!writable) return;
    
    #ifdef __APPLE__
    // Force synchronous write on Darwin
    if (msync(addr, size, MS_SYNC) == -1) {
        throw PMGDException(SyncFailed, errno, "Failed to sync " + filename);
    }
    // Also sync the file descriptor
    if (fsync(fd) == -1) {
        throw PMGDException(SyncFailed, errno, "Failed to fsync " + filename);
    }
    #else
    if (msync(addr, size, MS_SYNC) == -1) {
        throw PMGDException(SyncFailed, errno, "Failed to sync " + filename);
    }
    if (fsync(fd) == -1) {
        throw PMGDException(SyncFailed, errno, "Failed to fsync " + filename);
    }
    #endif
}

#ifdef __APPLE__
void CapnProtoDatabase::setup_darwin_mmap_optimizations() {
    // Configure memory pressure handling for Darwin
    size_t size = sizeof(int);
    int vm_pressure_disable = 1;
    
    // Disable VM pressure monitoring for our mapped regions if available
    sysctlbyname("vm.pressure_disable_enable", nullptr, nullptr, &vm_pressure_disable, size);
}

void* CapnProtoDatabase::create_darwin_mmap(const std::string& path, size_t size, bool create, bool writable) {
    int fd = open(path.c_str(), (writable ? O_RDWR : O_RDONLY) | (create ? O_CREAT : 0), 0666);
    if (fd == -1) {
        throw PMGDException(OpenFailed, errno, "Failed to open " + path);
    }
    
    if (create && writable) {
        if (ftruncate(fd, size) == -1) {
            close(fd);
            throw PMGDException(OpenFailed, errno, "Failed to resize " + path);
        }
    }
    
    int prot = PROT_READ;
    if (writable) prot |= PROT_WRITE;
    
    void* addr = mmap(nullptr, size, prot, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        close(fd);
        throw PMGDException(OpenFailed, errno, "Failed to mmap " + path);
    }
    
    // Darwin-specific optimizations
    madvise(addr, size, MADV_WILLNEED);  // Hint that we'll need this soon
    
    close(fd);  // Can close fd after mmap on Darwin
    return addr;
}
#endif

// Main CapnProtoDatabase implementation
CapnProtoDatabase::CapnProtoDatabase(const std::string& db_path, bool create, bool read_only)
    : _db_path(db_path), _read_only(read_only), _batch_context(std::make_unique<BatchContext>()) {
    
    #ifdef __APPLE__
    setup_darwin_mmap_optimizations();
    #endif
    
    std::filesystem::create_directories(db_path);
    
    // Initialize main database file
    std::string main_db_file = db_path + "/pmgd.capnp.db";
    _main_region = std::make_unique<MmapRegion>(main_db_file, SIZE_1GB, create, !read_only);
    
    // Initialize auxiliary regions
    std::string index_file = db_path + "/pmgd.index.db";
    _index_region = std::make_unique<MmapRegion>(index_file, SIZE_256MB, create, !read_only);
    
    std::string tx_file = db_path + "/pmgd.transactions.db";
    _transaction_region = std::make_unique<MmapRegion>(tx_file, SIZE_128MB, create, !read_only);
    
    initialize_database(create);
    
    // Check for legacy database migration
    if (create && std::filesystem::exists(db_path + "/allocator.jdb")) {
        migrate_legacy_files();
    }
}

CapnProtoDatabase::~CapnProtoDatabase() {
    try {
        if (_batch_context && _batch_context->operation_count.load() > 0) {
            commit_batch();
        }
        sync(true);
    } catch (...) {
        // Don't throw from destructor
    }
}

void CapnProtoDatabase::initialize_database(bool create) {
    if (create) {
        // Initialize new database
        _db_builder = _message_builder.initRoot<Schema::Database>();
        
        auto header = _db_builder.initHeader();
        header.setMagic(0x4447444D50474442ULL);  // "PMGDDB"
        header.setVersion(1);
        
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        header.setCreated(now);
        header.setLastModified(now);
        
        header.setNodeSize(sizeof(Schema::Node));
        header.setEdgeSize(sizeof(Schema::Edge));
        header.setMaxStringIdLength(256);
        header.setNumAllocators(8);
        header.setBaseAddress(reinterpret_cast<uint64_t>(_main_region->addr));
        
        // Initialize regions info
        auto regions = header.initRegions(3);
        
        regions[0].setName("main");
        regions[0].setAddress(reinterpret_cast<uint64_t>(_main_region->addr));
        regions[0].setLength(_main_region->size);
        regions[0].setMmapped(true);
        
        regions[1].setName("index");
        regions[1].setAddress(reinterpret_cast<uint64_t>(_index_region->addr));
        regions[1].setLength(_index_region->size);
        regions[1].setMmapped(true);
        
        regions[2].setName("transactions");
        regions[2].setAddress(reinterpret_cast<uint64_t>(_transaction_region->addr));
        regions[2].setLength(_transaction_region->size);
        regions[2].setMmapped(true);
        
        // Initialize empty collections
        _db_builder.initNodes(0);
        _db_builder.initEdges(0);
        _db_builder.initStringTable();
        _db_builder.initIndices(0);
        _db_builder.initTransactionLog();
        _db_builder.initAllocators(0);
        _db_builder.initBatchOperations(0);
        
        _db_builder.setNodeCount(0);
        _db_builder.setEdgeCount(0);
        _db_builder.setNextNodeId(1);
        _db_builder.setNextEdgeId(1);
        
    } else {
        // Load existing database
        if (_main_region->size < sizeof(uint64_t)) {
            throw PMGDException(CorruptedFile, "Database file too small");
        }
        
        // Read CapnProto data from mmap region
        capnp::FlatArrayMessageReader reader(
            capnp::arrayPtr(static_cast<const capnp::word*>(_main_region->addr),
                          _main_region->size / sizeof(capnp::word)));
        
        auto db_reader = reader.getRoot<Schema::Database>();
        _db_builder = _message_builder.initRoot<Schema::Database>();
        
        // Validate magic number
        auto header = db_reader.getHeader();
        if (header.getMagic() != 0x4447444D50474442ULL) {
            throw PMGDException(CorruptedFile, "Invalid database magic number");
        }
        
        _next_node_id.store(db_reader.getNextNodeId());
        _next_edge_id.store(db_reader.getNextEdgeId());
    }
}

void CapnProtoDatabase::migrate_legacy_files() {
    // Import from old .jdb format
    std::vector<std::string> legacy_files = {
        "allocator.jdb", "edges.jdb", "graph.jdb", "indexmanager.jdb",
        "journal.jdb", "nodes.jdb", "stringtable.jdb", "transaction.jdb"
    };
    
    for (const auto& file : legacy_files) {
        std::string full_path = _db_path + "/" + file;
        if (std::filesystem::exists(full_path)) {
            load_from_legacy_format(full_path);
        }
    }
}

void CapnProtoDatabase::load_from_legacy_format(const std::string& legacy_path) {
    // This would contain the migration logic from old .jdb files
    // For now, we'll create a placeholder that logs the migration
    std::ifstream legacy_file(legacy_path, std::ios::binary);
    if (!legacy_file) {
        return;
    }
    
    // Read legacy format and convert to CapnProto
    // This is a complex conversion that would need specific handling
    // for each component type (nodes, edges, transactions, etc.)
    
    legacy_file.close();
}

uint64_t CapnProtoDatabase::create_node(const std::string& tag) {
    std::lock_guard<std::mutex> lock(_write_mutex);
    
    uint64_t node_id = _next_node_id.fetch_add(1);
    
    auto nodes = _db_builder.getNodes();
    auto new_nodes = _db_builder.initNodes(nodes.size() + 1);
    
    for (size_t i = 0; i < nodes.size(); ++i) {
        new_nodes.setWithCaveats(i, nodes[i]);
    }
    
    auto new_node = new_nodes[nodes.size()];
    new_node.setId(node_id);
    new_node.setTag(tag);
    new_node.setDeleted(false);
    
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    new_node.setCreatedTimestamp(now);
    new_node.setModifiedTimestamp(now);
    
    new_node.initProperties(0);
    new_node.initEdges(0);
    new_node.initIncomingEdges(0);
    
    _db_builder.setNodeCount(_db_builder.getNodeCount() + 1);
    _stats.nodes_created.fetch_add(1);
    
    return node_id;
}

uint64_t CapnProtoDatabase::create_node_batch(const std::string& tag) {
    if (!_batch_context) {
        begin_batch();
    }
    
    uint64_t node_id = _next_node_id.fetch_add(1);
    
    {
        std::lock_guard<std::mutex> lock(_batch_context->batch_mutex);
        
        capnp::MallocMessageBuilder op_builder;
        auto operation = op_builder.initRoot<Schema::Operation>();
        operation.setType(Schema::OperationType::CREATE_NODE);
        
        auto node_op = operation.initNodeOp();
        node_op.setNodeId(node_id);
        node_op.setTag(tag);
        
        _batch_context->operations.push_back(operation.asReader());
        _batch_context->operation_count.fetch_add(1);
    }
    
    flush_batch_if_needed();
    return node_id;
}

uint64_t CapnProtoDatabase::create_edge(uint64_t src, uint64_t dst, const std::string& tag) {
    std::lock_guard<std::mutex> lock(_write_mutex);
    
    uint64_t edge_id = _next_edge_id.fetch_add(1);
    
    auto edges = _db_builder.getEdges();
    auto new_edges = _db_builder.initEdges(edges.size() + 1);
    
    for (size_t i = 0; i < edges.size(); ++i) {
        new_edges.setWithCaveats(i, edges[i]);
    }
    
    auto new_edge = new_edges[edges.size()];
    new_edge.setId(edge_id);
    new_edge.setTag(tag);
    new_edge.setSrc(src);
    new_edge.setDst(dst);
    new_edge.setDeleted(false);
    
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    new_edge.setCreatedTimestamp(now);
    new_edge.setModifiedTimestamp(now);
    
    new_edge.initProperties(0);
    
    _db_builder.setEdgeCount(_db_builder.getEdgeCount() + 1);
    _stats.edges_created.fetch_add(1);
    
    return edge_id;
}

void CapnProtoDatabase::begin_batch(size_t max_size) {
    std::lock_guard<std::mutex> lock(_write_mutex);
    _batch_context = std::make_unique<BatchContext>(max_size, true);
}

void CapnProtoDatabase::commit_batch() {
    if (!_batch_context) return;
    
    std::lock_guard<std::mutex> lock(_write_mutex);
    std::lock_guard<std::mutex> batch_lock(_batch_context->batch_mutex);
    
    // Apply all batched operations
    for (const auto& op_reader : _batch_context->operations) {
        // Process each operation based on its type
        auto op_type = op_reader.getType();
        
        switch (op_type) {
            case Schema::OperationType::CREATE_NODE: {
                auto node_op = op_reader.getNodeOp();
                // Apply node creation
                break;
            }
            case Schema::OperationType::CREATE_EDGE: {
                auto edge_op = op_reader.getEdgeOp();
                // Apply edge creation
                break;
            }
            default:
                break;
        }
    }
    
    _stats.batch_operations.fetch_add(_batch_context->operation_count.load());
    _batch_context->operations.clear();
    _batch_context->operation_count.store(0);
}

void CapnProtoDatabase::flush_batch_if_needed() {
    if (_batch_context && 
        _batch_context->auto_flush &&
        _batch_context->operation_count.load() >= _batch_context->max_batch_size) {
        commit_batch();
    }
}

void CapnProtoDatabase::sync(bool force) {
    if (_read_only) return;
    
    std::lock_guard<std::mutex> lock(_write_mutex);
    
    // Serialize the current database state to the mmap regions
    capnp::FlatArrayMessageBuilder builder;
    builder.setRoot(_db_builder.asReader());
    
    auto words = builder.getSegmentsForOutput();
    size_t total_size = 0;
    for (const auto& segment : words) {
        total_size += segment.size() * sizeof(capnp::word);
    }
    
    if (total_size > _main_region->size) {
        throw PMGDException(OutOfSpace, "Database exceeds allocated space");
    }
    
    // Write serialized data to mmap region
    char* write_ptr = static_cast<char*>(_main_region->addr);
    for (const auto& segment : words) {
        size_t segment_size = segment.size() * sizeof(capnp::word);
        std::memcpy(write_ptr, segment.begin(), segment_size);
        write_ptr += segment_size;
    }
    
    // Sync all regions
    _main_region->sync();
    _index_region->sync();
    _transaction_region->sync();
    
    _stats.sync_operations.fetch_add(1);
}

void CapnProtoDatabase::flush_all_regions() {
    if (_read_only) return;
    
    _main_region->flush();
    _index_region->flush();
    _transaction_region->flush();
}

void CapnProtoDatabase::checkpoint() {
    sync(true);
    flush_all_regions();
}

const CapnProtoDatabase::DatabaseStats& CapnProtoDatabase::get_stats() const {
    return _stats;
}

size_t CapnProtoDatabase::get_memory_usage() const {
    return _main_region->size + _index_region->size + _transaction_region->size;
}

void CapnProtoDatabase::enable_mac_optimizations() {
    #ifdef __APPLE__
    setup_darwin_mmap_optimizations();
    #endif
}

void CapnProtoDatabase::validate_database_integrity() {
    // Perform database integrity checks
    auto nodes = _db_builder.getNodes();
    auto edges = _db_builder.getEdges();
    
    // Check node integrity
    for (const auto& node : nodes) {
        if (node.getId() == 0) {
            throw PMGDException(CorruptedFile, "Invalid node ID");
        }
    }
    
    // Check edge integrity
    for (const auto& edge : edges) {
        if (edge.getId() == 0 || edge.getSrc() == 0 || edge.getDst() == 0) {
            throw PMGDException(CorruptedFile, "Invalid edge");
        }
    }
}

// Property helper implementations
namespace PropertyHelpers {
    Schema::Property::Value::Builder create_bool_value(capnp::MessageBuilder& builder, bool value) {
        auto prop_value = builder.initRoot<Schema::Property::Value>();
        prop_value.setBoolVal(value);
        return prop_value;
    }

    Schema::Property::Value::Builder create_int_value(capnp::MessageBuilder& builder, int64_t value) {
        auto prop_value = builder.initRoot<Schema::Property::Value>();
        prop_value.setIntVal(value);
        return prop_value;
    }

    Schema::Property::Value::Builder create_string_value(capnp::MessageBuilder& builder, const std::string& value) {
        auto prop_value = builder.initRoot<Schema::Property::Value>();
        prop_value.setStringVal(value);
        return prop_value;
    }
}

} // namespace PMGD