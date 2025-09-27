/**
 * @file   ZoneManager.cc
 *
 * Implementation of hierarchical zone-based database with WAL and cloud support
 */

#include "ZoneManager.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <ctime>
#include <openssl/sha.h>

namespace PMGD {

// Zone implementation
ZoneManager::Zone::Zone(const ZoneId& id, const ZoneConfig& config, bool create) 
    : _zone_id(id), _config(config) {
    
    _zone_path = get_zone_path(id);
    _wal_path = get_wal_path(id);
    
    if (create) {
        std::filesystem::create_directories(std::filesystem::path(_zone_path).parent_path());
        std::filesystem::create_directories(std::filesystem::path(_wal_path).parent_path());
    }
    
    initialize_zone(create);
    setup_wal_writer();
}

ZoneManager::Zone::~Zone() {
    _stop_wal_writer.store(true);
    _wal_condition.notify_all();
    
    if (_wal_writer_thread && _wal_writer_thread->joinable()) {
        _wal_writer_thread->join();
    }
    
    flush_wal();
    sync();
}

void ZoneManager::Zone::initialize_zone(bool create) {
    _message = std::make_unique<capnp::MallocMessageBuilder>();
    
    if (create) {
        // Create new zone
        switch (_config.type) {
            case Zones::ZoneType::NODES: {
                auto node_zone = _message->initRoot<Zones::NodeZone>();
                _header = node_zone.initHeader();
                node_zone.initNodes(0);
                node_zone.initNodeIndex(0);
                break;
            }
            case Zones::ZoneType::EDGES: {
                auto edge_zone = _message->initRoot<Zones::EdgeZone>();
                _header = edge_zone.initHeader();
                edge_zone.initEdges(0);
                edge_zone.initEdgeIndex(0);
                break;
            }
            case Zones::ZoneType::INDICES: {
                auto index_zone = _message->initRoot<Zones::IndexZone>();
                _header = index_zone.initHeader();
                index_zone.initIndices(0);
                index_zone.initBloomFilters(0);
                break;
            }
            case Zones::ZoneType::TRANSACTIONS: {
                auto tx_zone = _message->initRoot<Zones::TransactionZone>();
                _header = tx_zone.initHeader();
                tx_zone.initTransactions(0);
                tx_zone.initCheckpoints(0);
                break;
            }
            default:
                throw std::runtime_error("Unsupported zone type");
        }
        
        // Initialize header
        _header.setZoneId(_zone_id);
        _header.setZoneType(_config.type);
        _header.setMagic(0x5A4F4E4544420000ULL);  // "ZONEDB"
        _header.setVersion(1);
        
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        _header.setCreated(now);
        _header.setLastModified(now);
        
        _header.setStorageBackend(static_cast<Zones::StorageBackend>(_config.backend));
        _header.setWalEnabled(true);
        _header.setWalRotationSize(_config.wal_rotation_size);
        
    } else {
        // Load existing zone
        load_zone_from_storage();
    }
}

void ZoneManager::Zone::load_zone_from_storage() {
    if (_config.backend == StorageBackend::FILESYSTEM) {
        std::ifstream file(_zone_path, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Cannot open zone file: " + _zone_path);
        }
        
        auto stream = kj::std::StdInputStream(file);
        
        capnp::ReaderOptions options;
        options.traversalLimitInWords = 1000000000;  // 8GB limit
        
        capnp::InputStreamMessageReader reader(stream, options);
        
        // Determine zone type and load accordingly
        // This is a simplified approach - real implementation would detect type
        auto node_zone = reader.getRoot<Zones::NodeZone>();
        _header = _message->initRoot<Zones::NodeZone>().initHeader();
        
    } else {
        // Load from cloud storage
        download_from_cloud();
        load_zone_from_storage();  // Recursive call after download
    }
}

void ZoneManager::Zone::setup_wal_writer() {
    _wal_writer_thread = std::make_unique<std::thread>(&Zone::wal_writer_worker, this);
}

void ZoneManager::Zone::wal_writer_worker() {
    while (!_stop_wal_writer.load()) {
        std::unique_lock<std::mutex> lock(_wal_mutex);
        
        _wal_condition.wait_for(lock, std::chrono::milliseconds(100), [this] {
            return !_wal_queue.empty() || _stop_wal_writer.load();
        });
        
        if (_stop_wal_writer.load()) break;
        
        // Process WAL entries
        std::vector<Zones::WALEntry::Reader> entries_to_write;
        while (!_wal_queue.empty()) {
            entries_to_write.push_back(_wal_queue.front());
            _wal_queue.pop();
        }
        
        lock.unlock();
        
        if (!entries_to_write.empty()) {
            write_wal_batch(entries_to_write);
        }
    }
}

void ZoneManager::Zone::write_wal_entry(Zones::WALOperation operation, const void* data, size_t size) {
    capnp::MallocMessageBuilder wal_message;
    auto wal_entry = wal_message.initRoot<Zones::WALEntry>();
    
    wal_entry.setLsn(_current_lsn.fetch_add(1));
    wal_entry.setTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
    wal_entry.setOperation(operation);
    
    if (data && size > 0) {
        auto data_array = wal_entry.initData(size);
        std::memcpy(data_array.begin(), data, size);
    }
    
    // Calculate checksum
    uint32_t checksum = calculate_checksum(data, size);
    wal_entry.setChecksum(checksum);
    
    {
        std::lock_guard<std::mutex> lock(_wal_mutex);
        _wal_queue.push(wal_entry.asReader());
    }
    
    _wal_condition.notify_one();
}

uint64_t ZoneManager::Zone::insert_node(const Schema::Node::Reader& node) {
    std::unique_lock<std::shared_mutex> lock(_zone_mutex);
    
    if (_config.type != Zones::ZoneType::NODES && _config.type != Zones::ZoneType::MIXED) {
        throw std::runtime_error("Cannot insert node into " + _zone_id + " - wrong zone type");
    }
    
    // Write to WAL first
    capnp::MallocMessageBuilder node_message;
    node_message.setRoot(node);
    auto serialized = capnp::messageToFlatArray(node_message);
    
    write_wal_entry(Zones::WALOperation::INSERT_NODE, 
                   serialized.begin(), 
                   serialized.size() * sizeof(capnp::word));
    
    // Add to zone data
    auto node_zone = _message->getRoot<Zones::NodeZone>();
    auto nodes = node_zone.getNodes();
    auto new_nodes = node_zone.initNodes(nodes.size() + 1);
    
    for (size_t i = 0; i < nodes.size(); ++i) {
        new_nodes.setWithCaveats(i, nodes[i]);
    }
    
    new_nodes.setWithCaveats(nodes.size(), node);
    
    return node.getId();
}

// ZoneManager implementation
ZoneManager::ZoneManager(const std::string& base_path, const std::string& cluster_id)
    : _base_path(base_path), _cluster_id(cluster_id) {
    
    std::filesystem::create_directories(base_path);
    
    // Initialize default configurations
    _default_zone_config.type = Zones::ZoneType::MIXED;
    _default_zone_config.backend = StorageBackend::FILESYSTEM;
    _default_zone_config.max_size = SIZE_1GB;
    _default_zone_config.enable_compression = true;
    
    _wal_config.buffer_size = SIZE_16MB;
    _wal_config.flush_interval = std::chrono::milliseconds(100);
    _wal_config.sync_on_commit = true;
    
    initialize_cluster();
    discover_zones();
    start_background_services();
}

ZoneManager::~ZoneManager() {
    _shutdown.store(true);
    
    // Stop background services
    if (_health_monitor.joinable()) {
        _health_monitor.join();
    }
    if (_replication_manager.joinable()) {
        _replication_manager.join();
    }
    
    // Sync all zones
    for (auto& [zone_id, zone] : _zones) {
        zone->sync();
    }
}

ZoneManager::ZoneId ZoneManager::create_zone(Zones::ZoneType type, const ZoneConfig& config) {
    std::unique_lock<std::shared_mutex> lock(_manager_mutex);
    
    ZoneId zone_id = generate_zone_id(type);
    
    ZoneConfig zone_config = config;
    if (zone_config.backend == StorageBackend::FILESYSTEM && zone_config.endpoint.empty()) {
        zone_config.endpoint = _base_path + "/" + zone_id;
    }
    
    auto zone = std::make_unique<Zone>(zone_id, zone_config, true);
    _zones[zone_id] = std::move(zone);
    _zones_by_type[type].push_back(zone_id);
    
    return zone_id;
}

ZoneManager::Zone* ZoneManager::get_zone(const ZoneId& zone_id) {
    std::shared_lock<std::shared_mutex> lock(_manager_mutex);
    
    auto it = _zones.find(zone_id);
    return (it != _zones.end()) ? it->second.get() : nullptr;
}

uint64_t ZoneManager::create_node(const std::string& tag, const ZoneId& preferred_zone) {
    ZoneId target_zone = preferred_zone;
    
    if (target_zone.empty()) {
        // Find or create a node zone
        auto node_zones = list_zones_by_type(Zones::ZoneType::NODES);
        if (node_zones.empty()) {
            target_zone = create_zone(Zones::ZoneType::NODES);
        } else {
            target_zone = node_zones[0];  // Use first available node zone
        }
    }
    
    Zone* zone = get_zone(target_zone);
    if (!zone) {
        throw std::runtime_error("Zone not found: " + target_zone);
    }
    
    // Create node data
    capnp::MallocMessageBuilder node_message;
    auto node = node_message.initRoot<Schema::Node>();
    
    static std::atomic<uint64_t> next_node_id{1};
    uint64_t node_id = next_node_id.fetch_add(1);
    
    node.setId(node_id);
    node.setTag(tag);
    node.setDeleted(false);
    
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    node.setCreatedTimestamp(now);
    node.setModifiedTimestamp(now);
    
    return zone->insert_node(node.asReader());
}

void ZoneManager::Zone::recover_from_directory_scan() {
    std::cout << "🔄 Recovering zones from directory scan..." << std::endl;
    
    // Scan base directory for zone directories
    for (const auto& entry : std::filesystem::directory_iterator(_base_path)) {
        if (entry.is_directory()) {
            std::string zone_id = entry.path().filename().string();
            
            // Look for zone files in directory
            std::string zone_file = entry.path() / "zone.capnp";
            if (std::filesystem::exists(zone_file)) {
                std::cout << "   📁 Found zone: " << zone_id << std::endl;
                
                try {
                    // Detect zone type from file
                    ZoneConfig config = _default_zone_config;
                    config.endpoint = entry.path().string();
                    
                    auto zone = std::make_unique<Zone>(zone_id, config, false);
                    _zones[zone_id] = std::move(zone);
                    
                    // Recover from WAL if available
                    recover_zone_from_wal(zone_id);
                    
                } catch (const std::exception& e) {
                    std::cout << "   ⚠️  Failed to recover zone " << zone_id << ": " << e.what() << std::endl;
                }
            }
        }
    }
    
    std::cout << "   ✅ Recovery completed: " << _zones.size() << " zones recovered" << std::endl;
}

void ZoneManager::Zone::recover_zone_from_wal(const ZoneId& zone_id) {
    std::string wal_dir = get_wal_path(zone_id);
    
    if (!std::filesystem::exists(wal_dir)) {
        return;  // No WAL to recover from
    }
    
    std::cout << "   📝 Recovering WAL for zone: " << zone_id << std::endl;
    
    // Find all WAL files and sort by sequence
    std::vector<std::pair<WALSequence, std::string>> wal_files;
    
    for (const auto& entry : std::filesystem::directory_iterator(wal_dir)) {
        if (entry.path().extension() == ".wal") {
            std::string filename = entry.path().filename().string();
            
            // Extract sequence number from filename (e.g., "wal_000001.wal")
            if (filename.find("wal_") == 0) {
                std::string seq_str = filename.substr(4, 6);  // "000001"
                WALSequence sequence = std::stoull(seq_str);
                wal_files.emplace_back(sequence, entry.path().string());
            }
        }
    }
    
    // Sort by sequence and replay
    std::sort(wal_files.begin(), wal_files.end());
    
    for (const auto& [sequence, wal_path] : wal_files) {
        replay_wal_file(wal_path);
    }
    
    std::cout << "     ✅ Replayed " << wal_files.size() << " WAL files" << std::endl;
}

void ZoneManager::Zone::replay_wal_file(const std::string& wal_path) {
    std::ifstream file(wal_path, std::ios::binary);
    if (!file) {
        throw std::runtime_error("Cannot open WAL file: " + wal_path);
    }
    
    auto stream = kj::std::StdInputStream(file);
    capnp::InputStreamMessageReader reader(stream);
    auto wal = reader.getRoot<Zones::ZoneWAL>();
    
    auto entries = wal.getEntries();
    for (const auto& entry : entries) {
        replay_wal_entry(entry);
    }
}

void ZoneManager::Zone::replay_wal_entry(const Zones::WALEntry::Reader& entry) {
    auto operation = entry.getOperation();
    auto data = entry.getData();
    
    switch (operation) {
        case Zones::WALOperation::INSERT_NODE: {
            // Deserialize node data and insert
            capnp::FlatArrayMessageReader node_reader(
                capnp::arrayPtr(reinterpret_cast<const capnp::word*>(data.begin()),
                              data.size() / sizeof(capnp::word)));
            auto node = node_reader.getRoot<Schema::Node>();
            insert_node_direct(node);
            break;
        }
        case Zones::WALOperation::INSERT_EDGE: {
            capnp::FlatArrayMessageReader edge_reader(
                capnp::arrayPtr(reinterpret_cast<const capnp::word*>(data.begin()),
                              data.size() / sizeof(capnp::word)));
            auto edge = edge_reader.getRoot<Schema::Edge>();
            insert_edge_direct(edge);
            break;
        }
        default:
            // Handle other operations
            break;
    }
}

// S3 Adapter implementation
bool ZoneManager::S3Adapter::upload_file(const std::string& local_path, const std::string& remote_key) {
    // This would use AWS SDK to upload to S3
    // For now, placeholder implementation
    std::cout << "📤 Uploading " << local_path << " to S3://" << _bucket << "/" << remote_key << std::endl;
    return true;
}

bool ZoneManager::S3Adapter::download_file(const std::string& remote_key, const std::string& local_path) {
    // This would use AWS SDK to download from S3
    std::cout << "📥 Downloading S3://" << _bucket << "/" << remote_key << " to " << local_path << std::endl;
    return true;
}

std::vector<std::string> ZoneManager::S3Adapter::list_files(const std::string& prefix) {
    // This would list S3 objects with prefix
    std::cout << "📋 Listing S3://" << _bucket << "/" << prefix << "*" << std::endl;
    return {};  // Placeholder
}

// Zone helpers
namespace ZoneHelpers {
    ZoneManager::ZoneId compute_zone_for_node(uint64_t node_id, const std::vector<ZoneManager::ZoneId>& available_zones) {
        if (available_zones.empty()) {
            return "";
        }
        
        // Simple hash-based distribution
        size_t zone_index = node_id % available_zones.size();
        return available_zones[zone_index];
    }
    
    ZoneManager::ZoneId compute_zone_for_edge(uint64_t src, uint64_t dst, const std::vector<ZoneManager::ZoneId>& available_zones) {
        if (available_zones.empty()) {
            return "";
        }
        
        // Hash based on both source and destination
        size_t combined_hash = std::hash<uint64_t>{}(src) ^ (std::hash<uint64_t>{}(dst) << 1);
        size_t zone_index = combined_hash % available_zones.size();
        return available_zones[zone_index];
    }
    
    std::string zone_to_s3_key(const ZoneManager::ZoneId& zone_id, const std::string& file_type) {
        return "zones/" + zone_id + "/" + file_type + ".capnp";
    }
    
    std::string wal_to_s3_key(const ZoneManager::ZoneId& zone_id, ZoneManager::WALSequence sequence) {
        char seq_str[16];
        snprintf(seq_str, sizeof(seq_str), "%06llu", sequence);
        return "zones/" + zone_id + "/wal/wal_" + seq_str + ".wal";
    }
}

ZoneManager::ZoneId ZoneManager::generate_zone_id(Zones::ZoneType type) {
    const char* type_prefix[] = {"nodes", "edges", "indices", "tx", "strings", "alloc", "mixed", "custom"};
    
    uint64_t sequence = _next_zone_sequence.fetch_add(1);
    uint64_t timestamp = std::chrono::system_clock::now().time_since_epoch().count();
    
    std::ostringstream oss;
    oss << type_prefix[static_cast<int>(type)] << "_" 
        << timestamp << "_" << sequence;
    
    return oss.str();
}

std::string ZoneManager::get_zone_path(const ZoneId& zone_id) const {
    return _base_path + "/" + zone_id + "/zone.capnp";
}

std::string ZoneManager::get_wal_path(const ZoneId& zone_id) const {
    return _base_path + "/" + zone_id + "/wal";
}

void ZoneManager::setup_s3_backend(const std::string& bucket, const std::string& region,
                                  const std::string& access_key, const std::string& secret_key) {
    auto s3_adapter = std::make_unique<S3Adapter>(bucket, region);
    _cloud_adapters[StorageBackend::S3] = std::move(s3_adapter);
    
    std::cout << "✅ S3 backend configured: " << bucket << " (" << region << ")" << std::endl;
}

void ZoneManager::sync_zones_to_cloud() {
    std::shared_lock<std::shared_mutex> lock(_manager_mutex);
    
    for (const auto& [zone_id, zone] : _zones) {
        if (zone->config().backend == StorageBackend::S3 || 
            zone->config().backend == StorageBackend::HYBRID) {
            
            zone->upload_to_cloud();
        }
    }
}

ZoneManager::ClusterStats ZoneManager::get_cluster_stats() const {
    std::shared_lock<std::shared_mutex> lock(_manager_mutex);
    
    ClusterStats stats = {};
    stats.total_zones = _zones.size();
    
    for (const auto& [zone_id, zone] : _zones) {
        if (zone->get_health() == Zones::ZoneHealth::HEALTHY) {
            stats.healthy_zones++;
        }
        
        stats.total_storage_bytes += zone->get_memory_usage();
        
        // Add zone-specific stats based on type
        if (zone->config().type == Zones::ZoneType::NODES) {
            auto node_stats = zone->get_node_stats();
            stats.total_nodes_across_zones += node_stats.getTotalNodes();
        } else if (zone->config().type == Zones::ZoneType::EDGES) {
            auto edge_stats = zone->get_edge_stats();
            stats.total_edges_across_zones += edge_stats.getTotalEdges();
        }
    }
    
    if (stats.total_zones > 0) {
        stats.average_zone_utilization = static_cast<double>(stats.healthy_zones) / stats.total_zones;
    }
    
    return stats;
}

} // namespace PMGD