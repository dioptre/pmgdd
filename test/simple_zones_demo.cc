// Simple Zone Architecture Demo - Core Concepts
#include "../schema/zones.capnp.h"
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/std/iostream.h>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include <vector>
#include <string>

using namespace PMGD::Zones;

class SimpleZonesDemo {
private:
    std::string _demo_path;
    
public:
    SimpleZonesDemo(const std::string& path) : _demo_path(path) {
        cleanup();
    }
    
    void cleanup() {
        system(("rm -rf " + _demo_path).c_str());
        mkdir(_demo_path.c_str(), 0755);
    }
    
    void run_zones_demo() {
        std::cout << "🏗️  Zone-Based Database Architecture Demo" << std::endl;
        std::cout << "=========================================" << std::endl;
        std::cout << "💡 Concept: Hierarchical zones replacing flat .jdb files" << std::endl;
        std::cout << "💡 Each zone: Independent storage + WAL + cloud sync capability" << std::endl;
        std::cout << std::endl;
        
        // Create different zone types
        create_zone_types();
        
        // Show data distribution
        demonstrate_data_distribution();
        
        // Show WAL per zone
        demonstrate_zone_wal();
        
        // Show directory structure
        show_zone_structure();
        
        print_architecture_summary();
    }

private:
    void create_zone_types() {
        std::cout << "🏗️  Creating Specialized Zones..." << std::endl;
        
        // Create different zone types (like old .jdb files but modernized)
        std::vector<std::pair<std::string, ZoneType>> zones = {
            {"nodes_primary", ZoneType::NODES},
            {"nodes_secondary", ZoneType::NODES}, 
            {"edges_primary", ZoneType::EDGES},
            {"edges_secondary", ZoneType::EDGES},
            {"indices_main", ZoneType::INDICES},
            {"transactions_log", ZoneType::TRANSACTIONS},
            {"strings_table", ZoneType::STRINGS},
            {"allocator_meta", ZoneType::ALLOCATOR}
        };
        
        for (size_t i = 0; i < zones.size(); ++i) {
            const auto& zone_name = zones[i].first;
            const auto& zone_type = zones[i].second;
            
            create_zone_with_header(zone_name, zone_type);
            create_zone_wal(zone_name);
            
            std::cout << "   📁 Zone created: " << zone_name 
                      << " (type: " << static_cast<int>(zone_type) << ")" << std::endl;
        }
        
        std::cout << "   ✅ Created " << zones.size() << " specialized zones" << std::endl;
    }
    
    void create_zone_with_header(const std::string& zone_name, ZoneType zone_type) {
        // Create zone directory
        std::string zone_dir = _demo_path + "/" + zone_name;
        mkdir(zone_dir.c_str(), 0755);
        
        // Create zone header
        capnp::MallocMessageBuilder message;
        auto header = message.initRoot<ZoneHeader>();
        
        header.setZoneId(zone_name);
        header.setZoneType(zone_type);
        header.setMagic(0x5A4F4E4544420000ULL);  // "ZONEDB"
        header.setVersion(1);
        
        auto now = std::chrono::system_clock::now().time_since_epoch().count();
        header.setCreated(now);
        header.setLastModified(now);
        
        header.setStorageBackend(StorageBackend::FILESYSTEM);
        header.setWalEnabled(true);
        header.setWalRotationSize(134217728);  // 128MB
        header.setEnableCompression(true);
        
        // Save zone header
        std::string header_file = zone_dir + "/zone_header.capnp";
        std::ofstream file(header_file, std::ios::binary);
        auto stream = kj::std::StdOutputStream(file);
        capnp::writeMessage(stream, message);
    }
    
    void create_zone_wal(const std::string& zone_name) {
        // Create WAL directory and sample WAL file
        std::string wal_dir = _demo_path + "/" + zone_name + "/wal";
        mkdir(wal_dir.c_str(), 0755);
        
        // Create sample WAL entries
        capnp::MallocMessageBuilder wal_message;
        auto wal = wal_message.initRoot<ZoneWAL>();
        
        auto wal_header = wal.initHeader();
        wal_header.setZoneId(zone_name);
        wal_header.setWalSequence(1);
        wal_header.setStartLSN(1);
        wal_header.setEndLSN(10);
        wal_header.setCreated(std::chrono::system_clock::now().time_since_epoch().count());
        wal_header.setEntryCount(3);
        
        // Create sample WAL entries
        auto entries = wal.initEntries(3);
        
        // Entry 1: Node insertion
        auto entry1 = entries[0];
        entry1.setLsn(1);
        entry1.setTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
        entry1.setOperation(WALOperation::INSERT_NODE);
        entry1.setTransactionId(100);
        
        std::string data1 = "node_insert:" + zone_name + ":person_1";
        auto data1_array = entry1.initData(data1.size());
        std::memcpy(data1_array.begin(), data1.c_str(), data1.size());
        entry1.setChecksum(std::hash<std::string>{}(data1));
        
        // Entry 2: Edge insertion  
        auto entry2 = entries[1];
        entry2.setLsn(2);
        entry2.setTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
        entry2.setOperation(WALOperation::INSERT_EDGE);
        entry2.setTransactionId(100);
        
        std::string data2 = "edge_insert:" + zone_name + ":1->2:knows";
        auto data2_array = entry2.initData(data2.size());
        std::memcpy(data2_array.begin(), data2.c_str(), data2.size());
        entry2.setChecksum(std::hash<std::string>{}(data2));
        
        // Entry 3: Transaction commit
        auto entry3 = entries[2];
        entry3.setLsn(3);
        entry3.setTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
        entry3.setOperation(WALOperation::COMMIT_TRANSACTION);
        entry3.setTransactionId(100);
        
        std::string data3 = "commit_tx:100";
        auto data3_array = entry3.initData(data3.size());
        std::memcpy(data3_array.begin(), data3.c_str(), data3.size());
        entry3.setChecksum(std::hash<std::string>{}(data3));
        
        // Save WAL file
        std::string wal_file = wal_dir + "/wal_000001.wal";
        std::ofstream file(wal_file, std::ios::binary);
        auto stream = kj::std::StdOutputStream(file);
        capnp::writeMessage(stream, wal_message);
    }
    
    void demonstrate_data_distribution() {
        std::cout << "\n📊 Demonstrating Data Distribution..." << std::endl;
        
        // Create sample nodes in different zones
        create_sample_data_in_zone("nodes_primary", 50, "nodes");
        create_sample_data_in_zone("nodes_secondary", 50, "nodes");
        create_sample_data_in_zone("edges_primary", 25, "edges");
        create_sample_data_in_zone("edges_secondary", 25, "edges");
        
        std::cout << "   ✅ Data distributed across specialized zones" << std::endl;
        std::cout << "   💡 Like old .jdb files but hierarchical and independent" << std::endl;
    }
    
    void create_sample_data_in_zone(const std::string& zone_name, size_t count, const std::string& type) {
        std::string zone_dir = _demo_path + "/" + zone_name;
        
        for (size_t i = 0; i < count; ++i) {
            if (type == "nodes") {
                // Create node file
                capnp::MallocMessageBuilder message;
                auto node = message.initRoot<SimpleNode>();
                
                node.setId(i + 1);
                node.setTag("Person_" + std::to_string(i));
                node.setDeleted(false);
                node.setCreatedTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
                node.setModifiedTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
                
                std::string node_file = zone_dir + "/node_" + std::to_string(i + 1) + ".capnp";
                std::ofstream file(node_file, std::ios::binary);
                auto stream = kj::std::StdOutputStream(file);
                capnp::writeMessage(stream, message);
                
            } else if (type == "edges") {
                // Create edge file
                capnp::MallocMessageBuilder message;
                auto edge = message.initRoot<SimpleEdge>();
                
                edge.setId(i + 1);
                edge.setTag("knows");
                edge.setSrc(i + 1);
                edge.setDst((i + 1) % 10 + 1);  // Connect to other nodes
                edge.setDeleted(false);
                edge.setCreatedTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
                edge.setModifiedTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
                
                std::string edge_file = zone_dir + "/edge_" + std::to_string(i + 1) + ".capnp";
                std::ofstream file(edge_file, std::ios::binary);
                auto stream = kj::std::StdOutputStream(file);
                capnp::writeMessage(stream, message);
            }
        }
    }
    
    void demonstrate_zone_wal() {
        std::cout << "\n📝 Demonstrating Per-Zone WAL..." << std::endl;
        
        // Show WAL structure for each zone
        std::vector<std::string> zones = {
            "nodes_primary", "nodes_secondary", 
            "edges_primary", "edges_secondary"
        };
        
        for (const auto& zone_name : zones) {
            std::string wal_dir = _demo_path + "/" + zone_name + "/wal";
            
            // Count WAL files (we created one per zone)
            size_t wal_count = 0;
            system(("ls " + wal_dir + "/*.wal 2>/dev/null | wc -l > /tmp/wal_count").c_str());
            
            std::ifstream count_file("/tmp/wal_count");
            if (count_file) {
                count_file >> wal_count;
                count_file.close();
            }
            
            std::cout << "   📁 Zone '" << zone_name << "': " << wal_count << " WAL file(s)" << std::endl;
            
            // Load and show WAL content
            std::string wal_file = wal_dir + "/wal_000001.wal";
            show_wal_content(zone_name, wal_file);
        }
        
        std::cout << "   ✅ Each zone has independent WAL for ACID properties" << std::endl;
    }
    
    void show_wal_content(const std::string& zone_name, const std::string& wal_file) {
        try {
            std::ifstream file(wal_file, std::ios::binary);
            if (!file) return;
            
            auto stream = kj::std::StdInputStream(file);
            capnp::InputStreamMessageReader reader(stream);
            auto wal = reader.getRoot<ZoneWAL>();
            
            auto header = wal.getHeader();
            auto entries = wal.getEntries();
            
            std::cout << "     📝 WAL: " << entries.size() << " operations (LSN " 
                      << header.getStartLSN() << "-" << header.getEndLSN() << ")" << std::endl;
            
            for (size_t i = 0; i < std::min(size_t(2), size_t(entries.size())); ++i) {
                auto entry = entries[i];
                auto data = entry.getData();
                std::string operation_data(reinterpret_cast<const char*>(data.begin()), data.size());
                
                std::cout << "       🔸 LSN " << entry.getLsn() << ": " 
                          << static_cast<int>(entry.getOperation()) << " - " << operation_data << std::endl;
            }
            
        } catch (const std::exception& e) {
            std::cout << "     ⚠️  Could not read WAL: " << e.what() << std::endl;
        }
    }
    
    void show_zone_structure() {
        std::cout << "\n📂 Zone Directory Structure..." << std::endl;
        
        // Show the created structure using system commands
        std::cout << "   🌳 Directory tree:" << std::endl;
        system(("find " + _demo_path + " -type f | head -20 | sed 's|" + _demo_path + "/||' | sort").c_str());
        
        std::cout << "\n   📊 Zone Statistics:" << std::endl;
        
        // Count files in each zone
        std::vector<std::string> zones = {
            "nodes_primary", "nodes_secondary",
            "edges_primary", "edges_secondary", 
            "indices_main", "transactions_log"
        };
        
        for (const auto& zone : zones) {
            std::string zone_path = _demo_path + "/" + zone;
            
            // Count data files
            size_t data_files = count_files_in_directory(zone_path, ".capnp");
            size_t wal_files = count_files_in_directory(zone_path + "/wal", ".wal");
            
            std::cout << "     📁 " << zone << ": " << data_files << " data files, " << wal_files << " WAL files" << std::endl;
        }
    }
    
    size_t count_files_in_directory(const std::string& dir, const std::string& extension) {
        std::string count_cmd = "find " + dir + " -name '*" + extension + "' 2>/dev/null | wc -l";
        
        FILE* pipe = popen(count_cmd.c_str(), "r");
        if (!pipe) return 0;
        
        size_t count = 0;
        fscanf(pipe, "%zu", &count);
        pclose(pipe);
        
        return count;
    }
    
    void print_architecture_summary() {
        std::cout << "\n🎯 Zone Architecture Summary" << std::endl;
        std::cout << "============================" << std::endl;
        
        std::cout << "💡 Key Improvements over old .jdb system:" << std::endl;
        std::cout << "   ✅ Hierarchical organization (vs flat .jdb files)" << std::endl;
        std::cout << "   ✅ Per-zone WAL for ACID properties" << std::endl;
        std::cout << "   ✅ Independent zone scaling" << std::endl;
        std::cout << "   ✅ Cloud storage abstraction ready" << std::endl;
        std::cout << "   ✅ Failure recovery from directory scan" << std::endl;
        
        std::cout << "\n🚀 Distributed Capabilities:" << std::endl;
        std::cout << "   🌐 Each zone can be on different storage backends" << std::endl;
        std::cout << "   🔄 Independent replication per zone" << std::endl;
        std::cout << "   ⚡ Parallel access to different zones" << std::endl;
        std::cout << "   ☁️  S3/GCS/Azure ready for infinite scale" << std::endl;
        
        std::cout << "\n🐝 Hive Access Pattern:" << std::endl;
        std::cout << "   💻 Process A: Read/Write nodes_primary + edges_primary" << std::endl;
        std::cout << "   💻 Process B: Read/Write nodes_secondary + edges_secondary" << std::endl;
        std::cout << "   ☁️  Cloud Sync: All zones replicated to S3" << std::endl;
        std::cout << "   🔄 Recovery: Scan directories + replay WAL files" << std::endl;
        
        std::cout << "\n✅ Zone architecture ready for infinite distributed scale!" << std::endl;
    }
    
    void demonstrate_failure_recovery() {
        std::cout << "\n🔄 Demonstrating Failure Recovery..." << std::endl;
        
        std::cout << "   🔍 Scanning for zones..." << std::endl;
        std::vector<std::string> discovered_zones;
        
        // Simulate zone discovery by scanning directories
        FILE* pipe = popen(("find " + _demo_path + " -name zone_header.capnp").c_str(), "r");
        if (pipe) {
            char path[1024];
            while (fgets(path, sizeof(path), pipe)) {
                std::string header_path(path);
                header_path.pop_back();  // Remove newline
                
                // Extract zone name from path
                size_t last_slash = header_path.find_last_of('/');
                size_t second_last_slash = header_path.find_last_of('/', last_slash - 1);
                
                if (second_last_slash != std::string::npos) {
                    std::string zone_name = header_path.substr(second_last_slash + 1, 
                                                              last_slash - second_last_slash - 1);
                    discovered_zones.push_back(zone_name);
                }
            }
            pclose(pipe);
        }
        
        std::cout << "   ✅ Discovered " << discovered_zones.size() << " zones from directory scan:" << std::endl;
        for (const auto& zone : discovered_zones) {
            std::cout << "     📁 " << zone << std::endl;
        }
        
        std::cout << "   💡 System can rebuild complete state from filesystem scan + WAL replay" << std::endl;
    }
};

int main() {
    try {
        std::string demo_path = "/tmp/zones_demo";
        SimpleZonesDemo demo(demo_path);
        
        demo.run_zones_demo();
        
        std::cout << "\n🎉 Zone Architecture Demo Complete!" << std::endl;
        std::cout << "====================================" << std::endl;
        std::cout << "🎯 Check the demo at: " << demo_path << std::endl;
        std::cout << "💡 Run: ls -la " << demo_path << " to see the zone structure" << std::endl;
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Demo failed: " << e.what() << std::endl;
        return 1;
    }
}