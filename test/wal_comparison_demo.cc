// WAL Design Comparison Demo
// Shows: Zone-level WAL vs Object-type-level WAL
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

class WALComparisonDemo {
private:
    std::string _demo_path;
    
public:
    WALComparisonDemo(const std::string& path) : _demo_path(path) {
        cleanup();
    }
    
    void cleanup() {
        system(("rm -rf " + _demo_path).c_str());
        mkdir(_demo_path.c_str(), 0755);
    }
    
    void run_wal_comparison() {
        std::cout << "📝 WAL Design Comparison Demo" << std::endl;
        std::cout << "============================" << std::endl;
        std::cout << "🤔 Question: WAL per zone vs WAL per object type?" << std::endl;
        std::cout << std::endl;
        
        // Approach 1: Zone-level WAL (current)
        demonstrate_zone_level_wal();
        
        // Approach 2: Object-type-level WAL (your idea)
        demonstrate_object_type_wal();
        
        // Compare both approaches
        compare_wal_approaches();
    }

private:
    void demonstrate_zone_level_wal() {
        std::cout << "📝 Approach 1: Zone-Level WAL (Current Design)" << std::endl;
        std::cout << "==============================================" << std::endl;
        
        std::string zone_path = _demo_path + "/zone_level_example";
        mkdir(zone_path.c_str(), 0755);
        
        std::string wal_dir = zone_path + "/wal";
        mkdir(wal_dir.c_str(), 0755);
        
        // Create ONE WAL file for ALL operations in this zone
        capnp::MallocMessageBuilder wal_message;
        auto wal = wal_message.initRoot<ZoneWAL>();
        
        auto wal_header = wal.initHeader();
        wal_header.setZoneId("mixed_zone");
        wal_header.setWalSequence(1);
        wal_header.setStartLSN(1);
        wal_header.setEndLSN(6);
        wal_header.setEntryCount(6);
        
        auto entries = wal.initEntries(6);
        uint64_t lsn = 1;
        
        // Mixed operations in ONE WAL
        create_wal_entry(entries[0], lsn++, WALOperation::INSERT_NODE, "node_1:Person");
        create_wal_entry(entries[1], lsn++, WALOperation::INSERT_NODE, "node_2:Company");
        create_wal_entry(entries[2], lsn++, WALOperation::INSERT_EDGE, "edge_1:1->2:works_at");
        create_wal_entry(entries[3], lsn++, WALOperation::CREATE_INDEX, "index_person_tag");
        create_wal_entry(entries[4], lsn++, WALOperation::BEGIN_TRANSACTION, "tx_100");
        create_wal_entry(entries[5], lsn++, WALOperation::COMMIT_TRANSACTION, "tx_100");
        
        // Save the single WAL file
        std::string wal_file = wal_dir + "/mixed_operations.wal";
        std::ofstream file(wal_file, std::ios::binary);
        auto stream = kj::std::StdOutputStream(file);
        capnp::writeMessage(stream, wal_message);
        
        std::cout << "   📁 Structure:" << std::endl;
        std::cout << "     zone_level_example/" << std::endl;
        std::cout << "     └── wal/" << std::endl;
        std::cout << "         └── mixed_operations.wal  # ALL operations here" << std::endl;
        
        std::cout << "   📊 WAL contains: 6 mixed operations (nodes, edges, indices, transactions)" << std::endl;
        std::cout << "   ✅ Simple: One WAL stream for entire zone" << std::endl;
        std::cout << "   ❌ Harder to: Replay only specific object types" << std::endl;
    }
    
    void demonstrate_object_type_wal() {
        std::cout << "\n📝 Approach 2: Object-Type-Level WAL (Your Idea)" << std::endl;
        std::cout << "================================================" << std::endl;
        
        std::string zone_path = _demo_path + "/object_type_example";
        mkdir(zone_path.c_str(), 0755);
        
        // Create separate directories for each object type
        std::vector<std::string> object_types = {"nodes", "edges", "properties", "indices", "transactions"};
        
        for (const auto& obj_type : object_types) {
            std::string obj_dir = zone_path + "/" + obj_type;
            std::string wal_dir = obj_dir + "/wal";
            mkdir(obj_dir.c_str(), 0755);
            mkdir(wal_dir.c_str(), 0755);
            
            // Create specialized WAL for this object type
            create_object_type_wal(obj_type, wal_dir);
        }
        
        std::cout << "   📁 Structure:" << std::endl;
        std::cout << "     object_type_example/" << std::endl;
        for (const auto& obj_type : object_types) {
            std::cout << "     ├── " << obj_type << "/" << std::endl;
            std::cout << "     │   ├── wal/" << obj_type << "_operations.wal  # Only " << obj_type << " ops" << std::endl;
            std::cout << "     │   └── " << obj_type << "_*.capnp" << std::endl;
        }
        
        std::cout << "   📊 Each object type has dedicated WAL stream" << std::endl;
        std::cout << "   ✅ Easy to: Replay only specific object types" << std::endl;
        std::cout << "   ✅ Easy to: Parallelize recovery per object type" << std::endl;
        std::cout << "   ✅ Easy to: Optimize WAL per object type characteristics" << std::endl;
        std::cout << "   ❌ More complex: Multiple WAL streams to coordinate" << std::endl;
    }
    
    void create_object_type_wal(const std::string& obj_type, const std::string& wal_dir) {
        capnp::MallocMessageBuilder wal_message;
        auto wal = wal_message.initRoot<ZoneWAL>();
        
        auto wal_header = wal.initHeader();
        wal_header.setZoneId("specialized_" + obj_type);
        wal_header.setWalSequence(1);
        wal_header.setStartLSN(1);
        
        if (obj_type == "nodes") {
            // Node-specific WAL operations
            wal_header.setEndLSN(3);
            wal_header.setEntryCount(3);
            auto entries = wal.initEntries(3);
            
            create_wal_entry(entries[0], 1, WALOperation::INSERT_NODE, "node_1:Person:Alice");
            create_wal_entry(entries[1], 2, WALOperation::INSERT_NODE, "node_2:Company:ACME");
            create_wal_entry(entries[2], 3, WALOperation::UPDATE_NODE, "node_1:Person:Alice_Updated");
            
        } else if (obj_type == "edges") {
            // Edge-specific WAL operations
            wal_header.setEndLSN(2);
            wal_header.setEntryCount(2);
            auto entries = wal.initEntries(2);
            
            create_wal_entry(entries[0], 1, WALOperation::INSERT_EDGE, "edge_1:1->2:works_at");
            create_wal_entry(entries[1], 2, WALOperation::INSERT_EDGE, "edge_2:1->2:knows");
            
        } else if (obj_type == "properties") {
            // Property-specific operations (hypothetical)
            wal_header.setEndLSN(2);
            wal_header.setEntryCount(2);
            auto entries = wal.initEntries(2);
            
            create_wal_entry(entries[0], 1, WALOperation::INSERT_NODE, "prop_set:node_1:age:30");  // Reusing enum
            create_wal_entry(entries[1], 2, WALOperation::UPDATE_NODE, "prop_update:node_1:age:31");
            
        } else if (obj_type == "indices") {
            // Index-specific operations
            wal_header.setEndLSN(2);
            wal_header.setEntryCount(2);
            auto entries = wal.initEntries(2);
            
            create_wal_entry(entries[0], 1, WALOperation::CREATE_INDEX, "index_person_by_age");
            create_wal_entry(entries[1], 2, WALOperation::CREATE_INDEX, "index_company_by_name");
            
        } else if (obj_type == "transactions") {
            // Transaction-specific operations
            wal_header.setEndLSN(3);
            wal_header.setEntryCount(3);
            auto entries = wal.initEntries(3);
            
            create_wal_entry(entries[0], 1, WALOperation::BEGIN_TRANSACTION, "tx_100:read_write");
            create_wal_entry(entries[1], 2, WALOperation::BEGIN_TRANSACTION, "tx_101:read_only");
            create_wal_entry(entries[2], 3, WALOperation::COMMIT_TRANSACTION, "tx_100:success");
        }
        
        // Save specialized WAL
        std::string wal_file = wal_dir + "/" + obj_type + "_operations.wal";
        std::ofstream file(wal_file, std::ios::binary);
        auto stream = kj::std::StdOutputStream(file);
        capnp::writeMessage(stream, wal_message);
    }
    
    void create_wal_entry(WALEntry::Builder& entry, uint64_t lsn, WALOperation operation, const std::string& data_str) {
        entry.setLsn(lsn);
        entry.setTimestamp(std::chrono::system_clock::now().time_since_epoch().count());
        entry.setOperation(operation);
        entry.setTransactionId(100);
        
        auto data = entry.initData(data_str.size());
        std::memcpy(data.begin(), data_str.c_str(), data_str.size());
        entry.setChecksum(std::hash<std::string>{}(data_str));
    }
    
    void compare_wal_approaches() {
        std::cout << "\n⚖️  WAL Approach Comparison" << std::endl;
        std::cout << "===========================" << std::endl;
        
        std::cout << "🔥 **Zone-Level WAL** (Current):" << std::endl;
        std::cout << "   ✅ Simpler coordination - one stream per zone" << std::endl;
        std::cout << "   ✅ Easier transaction management across object types" << std::endl;
        std::cout << "   ✅ Lower I/O overhead - fewer file handles" << std::endl;
        std::cout << "   ✅ Simpler replication - replicate entire zone WAL" << std::endl;
        std::cout << "   ❌ Must replay ALL operations even if you only need nodes" << std::endl;
        std::cout << "   ❌ Object types are coupled in recovery" << std::endl;
        
        std::cout << "\n🚀 **Object-Type WAL** (Your Idea):" << std::endl;
        std::cout << "   ✅ Selective recovery - replay only nodes if edges are fine" << std::endl;
        std::cout << "   ✅ Parallel recovery - replay nodes and edges simultaneously" << std::endl;
        std::cout << "   ✅ Object-specific optimization - tune WAL per object characteristics" << std::endl;
        std::cout << "   ✅ Independent object scaling - nodes WAL can be huge, properties WAL tiny" << std::endl;
        std::cout << "   ✅ Better failure isolation - corrupted edge WAL doesn't affect nodes" << std::endl;
        std::cout << "   ❌ More complex coordination - must coordinate across multiple WALs" << std::endl;
        std::cout << "   ❌ Transaction boundaries span multiple WAL files" << std::endl;
        
        std::cout << "\n🎯 **Recommendation**: " << std::endl;
        std::cout << "   💡 **Object-Type WAL is BETTER** for your use case!" << std::endl;
        std::cout << "   💡 Enables: Selective recovery, parallel processing, better isolation" << std::endl;
        std::cout << "   💡 Perfect for: S3 storage (separate buckets per object type)" << std::endl;
        std::cout << "   💡 Perfect for: Hive access (processes can work on different object types)" << std::endl;
        
        show_hybrid_approach();
    }
    
    void show_hybrid_approach() {
        std::cout << "\n🧠 **Hybrid Approach** (Best of Both):" << std::endl;
        std::cout << "======================================" << std::endl;
        
        std::string hybrid_path = _demo_path + "/hybrid_example";
        mkdir(hybrid_path.c_str(), 0755);
        
        // Create structure with both levels
        std::cout << "   📁 Hybrid structure:" << std::endl;
        std::cout << "     hybrid_example/" << std::endl;
        std::cout << "     ├── zone_wal.wal           # Cross-object-type operations (transactions, checkpoints)" << std::endl;
        std::cout << "     ├── nodes/" << std::endl;
        std::cout << "     │   ├── nodes_wal.wal      # Pure node operations" << std::endl;
        std::cout << "     │   └── node_*.capnp" << std::endl;
        std::cout << "     ├── edges/" << std::endl;
        std::cout << "     │   ├── edges_wal.wal      # Pure edge operations" << std::endl;
        std::cout << "     │   └── edge_*.capnp" << std::endl;
        std::cout << "     └── properties/" << std::endl;
        std::cout << "         ├── props_wal.wal      # Pure property operations" << std::endl;
        std::cout << "         └── prop_*.capnp" << std::endl;
        
        // Create the hybrid structure
        create_hybrid_wal_structure(hybrid_path);
        
        std::cout << "\n   💡 **Hybrid Benefits**:" << std::endl;
        std::cout << "     ✅ Object-type WALs: Fast selective recovery" << std::endl;
        std::cout << "     ✅ Zone WAL: Cross-cutting concerns (transactions, checkpoints)" << std::endl;
        std::cout << "     ✅ Best performance: Parallel recovery + coordination" << std::endl;
        std::cout << "     ✅ Perfect for S3: Each WAL type can go to different buckets" << std::endl;
    }
    
    void create_hybrid_wal_structure(const std::string& base_path) {
        // Create zone-level WAL for coordination
        std::string zone_wal_file = base_path + "/zone_wal.wal";
        create_coordination_wal(zone_wal_file);
        
        // Create object-type directories and WALs
        std::vector<std::string> types = {"nodes", "edges", "properties"};
        
        for (const auto& type : types) {
            std::string type_dir = base_path + "/" + type;
            std::string wal_dir = type_dir + "/wal";
            mkdir(type_dir.c_str(), 0755);
            mkdir(wal_dir.c_str(), 0755);
            
            std::string type_wal = wal_dir + "/" + type + "_wal.wal";
            create_object_specific_wal(type_wal, type);
        }
    }
    
    void create_coordination_wal(const std::string& wal_file) {
        capnp::MallocMessageBuilder message;
        auto wal = message.initRoot<ZoneWAL>();
        
        auto header = wal.initHeader();
        header.setZoneId("coordination");
        header.setWalSequence(1);
        header.setEntryCount(3);
        
        auto entries = wal.initEntries(3);
        create_wal_entry(entries[0], 1, WALOperation::BEGIN_TRANSACTION, "tx_100:cross_object");
        create_wal_entry(entries[1], 2, WALOperation::CHECKPOINT, "checkpoint_1");
        create_wal_entry(entries[2], 3, WALOperation::COMMIT_TRANSACTION, "tx_100:success");
        
        std::ofstream file(wal_file, std::ios::binary);
        auto stream = kj::std::StdOutputStream(file);
        capnp::writeMessage(stream, message);
    }
    
    void create_object_specific_wal(const std::string& wal_file, const std::string& type) {
        capnp::MallocMessageBuilder message;
        auto wal = message.initRoot<ZoneWAL>();
        
        auto header = wal.initHeader();
        header.setZoneId(type + "_specific");
        header.setWalSequence(1);
        
        if (type == "nodes") {
            header.setEntryCount(2);
            auto entries = wal.initEntries(2);
            create_wal_entry(entries[0], 1, WALOperation::INSERT_NODE, "pure_node_op_1");
            create_wal_entry(entries[1], 2, WALOperation::UPDATE_NODE, "pure_node_op_2");
            
        } else if (type == "edges") {
            header.setEntryCount(2);
            auto entries = wal.initEntries(2);
            create_wal_entry(entries[0], 1, WALOperation::INSERT_EDGE, "pure_edge_op_1");
            create_wal_entry(entries[1], 2, WALOperation::UPDATE_EDGE, "pure_edge_op_2");
            
        } else if (type == "properties") {
            header.setEntryCount(2);
            auto entries = wal.initEntries(2);
            create_wal_entry(entries[0], 1, WALOperation::INSERT_NODE, "pure_prop_set");  // Reusing enum
            create_wal_entry(entries[1], 2, WALOperation::UPDATE_NODE, "pure_prop_update");
        }
        
        std::ofstream file(wal_file, std::ios::binary);
        auto stream = kj::std::StdOutputStream(file);
        capnp::writeMessage(stream, message);
    }
    
    void show_directory_comparison() {
        std::cout << "\n📂 Directory Structure Comparison:" << std::endl;
        std::cout << "===================================" << std::endl;
        
        std::cout << "🔍 Zone-Level WAL:" << std::endl;
        system(("find " + _demo_path + "/zone_level_example -name '*.wal' | sort").c_str());
        
        std::cout << "\n🔍 Object-Type WAL:" << std::endl;
        system(("find " + _demo_path + "/object_type_example -name '*.wal' | sort").c_str());
        
        std::cout << "\n🔍 Hybrid Approach:" << std::endl;
        system(("find " + _demo_path + "/hybrid_example -name '*.wal' | sort").c_str());
    }
};

int main() {
    try {
        std::string demo_path = "/tmp/wal_comparison";
        WALComparisonDemo demo(demo_path);
        
        demo.run_wal_comparison();
        demo.show_directory_comparison();
        
        std::cout << "\n🎯 Conclusion: Object-Type WAL is Superior!" << std::endl;
        std::cout << "==========================================" << std::endl;
        std::cout << "✅ Better isolation: Each object type independent" << std::endl;
        std::cout << "✅ Better performance: Parallel recovery" << std::endl;
        std::cout << "✅ Better for S3: Object types in separate buckets" << std::endl;
        std::cout << "✅ Better for hive: Processes can focus on specific types" << std::endl;
        std::cout << "💡 Perfect for your distributed vision!" << std::endl;
        
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "❌ Demo failed: " << e.what() << std::endl;
        return 1;
    }
}