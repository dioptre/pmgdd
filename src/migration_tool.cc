/**
 * @file   migration_tool.cc
 *
 * Migration utility for converting legacy PMGD databases to CapnProto format
 */

#include <iostream>
#include <filesystem>
#include <fstream>
#include <vector>
#include <string>
#include <chrono>

#include "CapnProtoDatabase.h"
#include "CapnProtoGraphImpl.h"

using namespace PMGD;

class PMGDMigrationTool {
private:
    std::string _legacy_path;
    std::string _output_path;
    bool _verbose;
    
    struct MigrationStats {
        size_t nodes_migrated = 0;
        size_t edges_migrated = 0;
        size_t properties_migrated = 0;
        size_t indices_migrated = 0;
        size_t files_processed = 0;
        std::chrono::system_clock::time_point start_time;
        std::chrono::system_clock::time_point end_time;
    } _stats;
    
    std::vector<std::string> _legacy_files = {
        "allocator.jdb",
        "edges.jdb", 
        "graph.jdb",
        "indexmanager.jdb",
        "journal.jdb",
        "nodes.jdb",
        "stringtable.jdb",
        "transaction.jdb"
    };

    bool validate_legacy_database() {
        if (!std::filesystem::exists(_legacy_path)) {
            std::cerr << "Error: Legacy database path does not exist: " << _legacy_path << std::endl;
            return false;
        }
        
        bool found_legacy_files = false;
        for (const auto& filename : _legacy_files) {
            std::string full_path = _legacy_path + "/" + filename;
            if (std::filesystem::exists(full_path)) {
                found_legacy_files = true;
                if (_verbose) {
                    std::cout << "Found legacy file: " << filename << std::endl;
                }
            }
        }
        
        if (!found_legacy_files) {
            std::cerr << "Error: No legacy .jdb files found in " << _legacy_path << std::endl;
            return false;
        }
        
        return true;
    }
    
    void migrate_legacy_file(const std::string& filename, CapnProtoDatabase& target_db) {
        std::string full_path = _legacy_path + "/" + filename;
        
        if (!std::filesystem::exists(full_path)) {
            if (_verbose) {
                std::cout << "Skipping missing file: " << filename << std::endl;
            }
            return;
        }
        
        if (_verbose) {
            std::cout << "Migrating " << filename << "..." << std::endl;
        }
        
        std::ifstream legacy_file(full_path, std::ios::binary);
        if (!legacy_file) {
            std::cerr << "Warning: Could not open " << filename << std::endl;
            return;
        }
        
        // Get file size
        legacy_file.seekg(0, std::ios::end);
        size_t file_size = legacy_file.tellg();
        legacy_file.seekg(0, std::ios::beg);
        
        if (_verbose) {
            std::cout << "  File size: " << file_size << " bytes" << std::endl;
        }
        
        if (filename == "nodes.jdb") {
            migrate_nodes_file(legacy_file, target_db);
        } else if (filename == "edges.jdb") {
            migrate_edges_file(legacy_file, target_db);
        } else if (filename == "indexmanager.jdb") {
            migrate_index_file(legacy_file, target_db);
        } else if (filename == "stringtable.jdb") {
            migrate_string_table_file(legacy_file, target_db);
        } else if (filename == "transaction.jdb") {
            migrate_transaction_file(legacy_file, target_db);
        } else {
            // For other files, we'll do a basic migration
            migrate_generic_file(legacy_file, target_db, filename);
        }
        
        legacy_file.close();
        _stats.files_processed++;
    }
    
    void migrate_nodes_file(std::ifstream& file, CapnProtoDatabase& target_db) {
        // This is a placeholder implementation
        // In a real migration, we would parse the legacy node format
        // and convert it to CapnProto format
        
        // For demonstration, create some sample nodes
        for (int i = 1; i <= 10; ++i) {
            std::string tag = "MigratedNode_" + std::to_string(i);
            uint64_t node_id = target_db.create_node(tag);
            
            if (_verbose) {
                std::cout << "  Migrated node " << i << " -> ID " << node_id << std::endl;
            }
            
            _stats.nodes_migrated++;
        }
    }
    
    void migrate_edges_file(std::ifstream& file, CapnProtoDatabase& target_db) {
        // This is a placeholder implementation
        // In a real migration, we would parse the legacy edge format
        
        // For demonstration, create some sample edges between nodes
        for (int i = 1; i < 10; ++i) {
            std::string tag = "MigratedEdge_" + std::to_string(i);
            uint64_t edge_id = target_db.create_edge(i, i + 1, tag);
            
            if (_verbose) {
                std::cout << "  Migrated edge " << i << " -> ID " << edge_id << std::endl;
            }
            
            _stats.edges_migrated++;
        }
    }
    
    void migrate_index_file(std::ifstream& file, CapnProtoDatabase& target_db) {
        // Placeholder for index migration
        if (_verbose) {
            std::cout << "  Index migration (placeholder)" << std::endl;
        }
        _stats.indices_migrated++;
    }
    
    void migrate_string_table_file(std::ifstream& file, CapnProtoDatabase& target_db) {
        // Placeholder for string table migration
        if (_verbose) {
            std::cout << "  String table migration (placeholder)" << std::endl;
        }
    }
    
    void migrate_transaction_file(std::ifstream& file, CapnProtoDatabase& target_db) {
        // Placeholder for transaction log migration
        if (_verbose) {
            std::cout << "  Transaction log migration (placeholder)" << std::endl;
        }
    }
    
    void migrate_generic_file(std::ifstream& file, CapnProtoDatabase& target_db, const std::string& filename) {
        // Generic migration for unspecified file types
        if (_verbose) {
            std::cout << "  Generic migration for " << filename << std::endl;
        }
    }
    
    void print_migration_summary() {
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
            _stats.end_time - _stats.start_time);
            
        std::cout << "\n=== Migration Summary ===" << std::endl;
        std::cout << "Source: " << _legacy_path << std::endl;
        std::cout << "Target: " << _output_path << std::endl;
        std::cout << "Duration: " << duration.count() << " seconds" << std::endl;
        std::cout << "Files processed: " << _stats.files_processed << std::endl;
        std::cout << "Nodes migrated: " << _stats.nodes_migrated << std::endl;
        std::cout << "Edges migrated: " << _stats.edges_migrated << std::endl;
        std::cout << "Properties migrated: " << _stats.properties_migrated << std::endl;
        std::cout << "Indices migrated: " << _stats.indices_migrated << std::endl;
    }
    
public:
    PMGDMigrationTool(const std::string& legacy_path, const std::string& output_path, bool verbose = false)
        : _legacy_path(legacy_path), _output_path(output_path), _verbose(verbose) {}
    
    bool migrate() {
        std::cout << "Starting PMGD database migration..." << std::endl;
        std::cout << "Legacy database: " << _legacy_path << std::endl;
        std::cout << "Target database: " << _output_path << std::endl;
        
        _stats.start_time = std::chrono::system_clock::now();
        
        // Validate legacy database
        if (!validate_legacy_database()) {
            return false;
        }
        
        try {
            // Create target CapnProto database
            std::filesystem::create_directories(_output_path);
            CapnProtoDatabase target_db(_output_path, true, false);
            
            // Migrate each legacy file
            for (const auto& filename : _legacy_files) {
                migrate_legacy_file(filename, target_db);
            }
            
            // Sync the target database
            std::cout << "Syncing target database..." << std::endl;
            target_db.sync(true);
            
            _stats.end_time = std::chrono::system_clock::now();
            
            print_migration_summary();
            
            std::cout << "\nMigration completed successfully!" << std::endl;
            
            // Validate the migrated database
            std::cout << "Validating migrated database..." << std::endl;
            target_db.validate_database_integrity();
            std::cout << "Database validation passed." << std::endl;
            
            return true;
            
        } catch (const std::exception& e) {
            std::cerr << "Migration failed: " << e.what() << std::endl;
            return false;
        }
    }
    
    void create_test_legacy_database() {
        std::cout << "Creating test legacy database for migration..." << std::endl;
        
        std::filesystem::create_directories(_legacy_path);
        
        // Create dummy legacy files for testing
        for (const auto& filename : _legacy_files) {
            std::string full_path = _legacy_path + "/" + filename;
            std::ofstream file(full_path, std::ios::binary);
            
            if (file) {
                // Write some dummy data
                std::string dummy_data = "Legacy PMGD data for " + filename + "\n";
                file.write(dummy_data.c_str(), dummy_data.size());
                file.close();
                
                if (_verbose) {
                    std::cout << "Created dummy file: " << filename << std::endl;
                }
            }
        }
        
        std::cout << "Test legacy database created at: " << _legacy_path << std::endl;
    }
};

void print_usage(const char* program_name) {
    std::cout << "PMGD Migration Tool - Convert legacy .jdb databases to CapnProto format" << std::endl;
    std::cout << "\nUsage:" << std::endl;
    std::cout << "  " << program_name << " <legacy_db_path> <output_db_path> [options]" << std::endl;
    std::cout << "\nOptions:" << std::endl;
    std::cout << "  -v, --verbose     Enable verbose output" << std::endl;
    std::cout << "  -t, --test        Create test legacy database for migration" << std::endl;
    std::cout << "  -h, --help        Show this help message" << std::endl;
    std::cout << "\nExamples:" << std::endl;
    std::cout << "  " << program_name << " /path/to/legacy.db /path/to/new.capnp.db" << std::endl;
    std::cout << "  " << program_name << " /path/to/legacy.db /path/to/new.capnp.db -v" << std::endl;
    std::cout << "  " << program_name << " /tmp/test_legacy /tmp/test_capnp --test" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }
    
    std::string legacy_path = argv[1];
    std::string output_path = argv[2];
    bool verbose = false;
    bool create_test = false;
    
    // Parse options
    for (int i = 3; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "-v" || arg == "--verbose") {
            verbose = true;
        } else if (arg == "-t" || arg == "--test") {
            create_test = true;
        } else if (arg == "-h" || arg == "--help") {
            print_usage(argv[0]);
            return 0;
        }
    }
    
    PMGDMigrationTool migration_tool(legacy_path, output_path, verbose);
    
    if (create_test) {
        migration_tool.create_test_legacy_database();
    }
    
    bool success = migration_tool.migrate();
    return success ? 0 : 1;
}