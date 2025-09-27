/**
 * @file   ZoneManager.h
 *
 * Hierarchical Zone-Based Database Manager
 * Supports distributed storage with per-zone WAL and S3/cloud backends
 */

#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <atomic>
#include <thread>
#include <queue>
#include <functional>

#include "../schema/zones.capnp.h"
#include "../schema/pmgd.capnp.h"
#include <capnp/message.h>

namespace PMGD {

class ZoneManager {
public:
    using ZoneId = std::string;
    using WALSequence = uint64_t;
    
    enum class StorageBackend {
        FILESYSTEM,
        S3,
        GCS,
        AZURE,
        MEMORY,
        HYBRID
    };
    
    struct ZoneConfig {
        Zones::ZoneType type;
        StorageBackend backend;
        std::string endpoint;           // Path or S3 bucket
        size_t max_size = SIZE_1GB;
        size_t wal_rotation_size = SIZE_128MB;
        uint32_t replication_factor = 1;
        bool enable_compression = true;
        bool enable_encryption = false;
        std::string credentials_path;
    };
    
    struct WALConfig {
        size_t buffer_size = SIZE_16MB;
        std::chrono::milliseconds flush_interval{100};
        bool sync_on_commit = true;
        bool enable_compression = true;
        uint32_t retention_days = 7;
    };

    class Zone {
    private:
        ZoneId _zone_id;
        ZoneConfig _config;
        std::unique_ptr<capnp::MallocMessageBuilder> _message;
        Zones::ZoneHeader::Builder _header;
        
        std::string _zone_path;
        std::string _wal_path;
        std::atomic<WALSequence> _current_wal_sequence{1};
        std::atomic<uint64_t> _current_lsn{1};
        
        mutable std::shared_mutex _zone_mutex;
        std::unique_ptr<std::thread> _wal_writer_thread;
        std::queue<Zones::WALEntry::Reader> _wal_queue;
        std::mutex _wal_mutex;
        std::condition_variable _wal_condition;
        std::atomic<bool> _stop_wal_writer{false};
        
        void initialize_zone(bool create);
        void setup_wal_writer();
        void wal_writer_worker();
        void rotate_wal_if_needed();
        
    public:
        Zone(const ZoneId& id, const ZoneConfig& config, bool create = false);
        ~Zone();
        
        // Basic operations
        uint64_t insert_node(const Schema::Node::Reader& node);
        uint64_t insert_edge(const Schema::Edge::Reader& edge);
        bool update_node(uint64_t node_id, const Schema::Node::Reader& node);
        bool update_edge(uint64_t edge_id, const Schema::Edge::Reader& edge);
        bool delete_node(uint64_t node_id);
        bool delete_edge(uint64_t edge_id);
        
        Schema::Node::Reader get_node(uint64_t node_id) const;
        Schema::Edge::Reader get_edge(uint64_t edge_id) const;
        
        // WAL operations
        void write_wal_entry(Zones::WALOperation operation, const void* data, size_t size);
        void flush_wal();
        void rotate_wal();
        std::vector<std::string> get_wal_files() const;
        
        // Zone management
        void sync();
        void checkpoint();
        void compact();
        void split_zone(const ZoneId& new_zone_id, std::function<bool(uint64_t)> predicate);
        void merge_with_zone(const ZoneId& other_zone_id);
        
        // Statistics and health
        Zones::NodeZoneStats get_node_stats() const;
        Zones::EdgeZoneStats get_edge_stats() const;
        Zones::ZoneHealth get_health() const;
        size_t get_memory_usage() const;
        
        // Storage backend operations
        void upload_to_cloud();
        void download_from_cloud();
        void replicate_to_peers();
        
        const ZoneId& zone_id() const { return _zone_id; }
        const ZoneConfig& config() const { return _config; }
    };

private:
    std::string _base_path;
    std::string _cluster_id;
    std::unordered_map<ZoneId, std::unique_ptr<Zone>> _zones;
    std::unordered_map<Zones::ZoneType, std::vector<ZoneId>> _zones_by_type;
    
    mutable std::shared_mutex _manager_mutex;
    std::atomic<uint64_t> _next_zone_sequence{1};
    
    // Distributed coordination
    std::vector<std::string> _coordinator_endpoints;
    std::string _local_endpoint;
    std::thread _health_monitor;
    std::thread _replication_manager;
    std::atomic<bool> _shutdown{false};
    
    // Cloud storage abstraction
    class CloudStorageAdapter {
    public:
        virtual ~CloudStorageAdapter() = default;
        virtual bool upload_file(const std::string& local_path, const std::string& remote_key) = 0;
        virtual bool download_file(const std::string& remote_key, const std::string& local_path) = 0;
        virtual std::vector<std::string> list_files(const std::string& prefix) = 0;
        virtual bool delete_file(const std::string& remote_key) = 0;
    };
    
    class S3Adapter : public CloudStorageAdapter {
        std::string _bucket;
        std::string _region;
        std::string _access_key;
        std::string _secret_key;
        
    public:
        S3Adapter(const std::string& bucket, const std::string& region);
        bool upload_file(const std::string& local_path, const std::string& remote_key) override;
        bool download_file(const std::string& remote_key, const std::string& local_path) override;
        std::vector<std::string> list_files(const std::string& prefix) override;
        bool delete_file(const std::string& remote_key) override;
    };
    
    std::unordered_map<StorageBackend, std::unique_ptr<CloudStorageAdapter>> _cloud_adapters;
    
    void initialize_cluster();
    void discover_zones();
    void start_background_services();
    void health_monitor_worker();
    void replication_manager_worker();
    
    ZoneId generate_zone_id(Zones::ZoneType type);
    std::string get_zone_path(const ZoneId& zone_id) const;
    std::string get_wal_path(const ZoneId& zone_id) const;

public:
    ZoneManager(const std::string& base_path, const std::string& cluster_id = "default");
    ~ZoneManager();
    
    // Zone lifecycle
    ZoneId create_zone(Zones::ZoneType type, const ZoneConfig& config = {});
    bool delete_zone(const ZoneId& zone_id);
    Zone* get_zone(const ZoneId& zone_id);
    const Zone* get_zone(const ZoneId& zone_id) const;
    
    std::vector<ZoneId> list_zones() const;
    std::vector<ZoneId> list_zones_by_type(Zones::ZoneType type) const;
    
    // High-level operations that route to appropriate zones
    uint64_t create_node(const std::string& tag, const ZoneId& preferred_zone = "");
    uint64_t create_edge(uint64_t src, uint64_t dst, const std::string& tag, const ZoneId& preferred_zone = "");
    
    Schema::Node::Reader get_node(uint64_t node_id);
    Schema::Edge::Reader get_edge(uint64_t edge_id);
    
    bool delete_node(uint64_t node_id);
    bool delete_edge(uint64_t edge_id);
    
    // Batch operations across zones
    void begin_distributed_batch();
    void commit_distributed_batch();
    void rollback_distributed_batch();
    
    // Failure recovery
    void recover_from_directory_scan();
    void recover_zone_from_wal(const ZoneId& zone_id);
    void rebuild_zone_from_peers(const ZoneId& zone_id);
    
    // Cloud/distributed operations
    void setup_s3_backend(const std::string& bucket, const std::string& region, 
                         const std::string& access_key, const std::string& secret_key);
    void setup_gcs_backend(const std::string& bucket, const std::string& credentials_path);
    
    void sync_zones_to_cloud();
    void download_zones_from_cloud();
    void replicate_zones_across_cluster();
    
    // Monitoring and management
    void start_cluster_coordinator();
    void join_cluster(const std::vector<std::string>& coordinator_endpoints);
    void leave_cluster();
    
    std::vector<Zones::ZoneLocation> discover_peer_zones();
    void broadcast_zone_update(const ZoneId& zone_id);
    
    // Administrative operations
    void rebalance_zones();
    void compact_all_zones();
    void vacuum_old_wals();
    void optimize_zone_placement();
    
    // Statistics and monitoring
    struct ClusterStats {
        size_t total_zones;
        size_t healthy_zones;
        size_t total_nodes_across_zones;
        size_t total_edges_across_zones;
        size_t total_wal_entries;
        double average_zone_utilization;
        uint64_t total_storage_bytes;
        uint64_t total_cloud_storage_bytes;
    };
    
    ClusterStats get_cluster_stats() const;
    std::string get_cluster_topology_json() const;
    
    // Configuration
    void set_default_zone_config(const ZoneConfig& config);
    void set_wal_config(const WALConfig& config);
    
    ZoneConfig _default_zone_config;
    WALConfig _wal_config;
};

// Helper functions for zone-based operations
namespace ZoneHelpers {
    ZoneManager::ZoneId compute_zone_for_node(uint64_t node_id, const std::vector<ZoneManager::ZoneId>& available_zones);
    ZoneManager::ZoneId compute_zone_for_edge(uint64_t src, uint64_t dst, const std::vector<ZoneManager::ZoneId>& available_zones);
    
    std::string serialize_wal_operation(Zones::WALOperation op, const void* data, size_t size);
    bool deserialize_wal_operation(const std::string& serialized, Zones::WALOperation& op, std::vector<uint8_t>& data);
    
    // S3 path utilities
    std::string zone_to_s3_key(const ZoneManager::ZoneId& zone_id, const std::string& file_type);
    std::string wal_to_s3_key(const ZoneManager::ZoneId& zone_id, ZoneManager::WALSequence sequence);
}

} // namespace PMGD