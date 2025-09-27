@0x8f3e6d9c4b2a1f7e;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("PMGD::Zones");

# Hierarchical Zone-Based Database Architecture
# Inspired by the old .jdb file structure but modernized for distributed systems

# Zone metadata and configuration
struct ZoneHeader {
  zoneId @0 :Text;              # Unique zone identifier
  zoneType @1 :ZoneType;        # Type of data stored in this zone
  magic @2 :UInt64 = 0x5A4F4E4544420000;  # "ZONEDB" magic
  version @3 :UInt32 = 1;
  created @4 :UInt64;           # Timestamp
  lastModified @5 :UInt64;
  parentZone @6 :Text;          # Parent zone ID (for hierarchy)
  childZones @7 :List(Text);    # Child zone IDs
  
  # Storage configuration
  storageBackend @8 :StorageBackend;
  backendConfig @9 :Text;       # JSON config for storage backend
  
  # WAL configuration
  walEnabled @10 :Bool = true;
  walRotationSize @11 :UInt64 = 134217728;  # 128MB
  walRetentionDays @12 :UInt32 = 7;
  
  # Clustering and distribution
  clusterId @13 :Text;          # Cluster this zone belongs to
  replicationFactor @14 :UInt32 = 1;
  consistencyLevel @15 :ConsistencyLevel;
  
  # Performance tuning
  cacheSize @16 :UInt64 = 67108864;  # 64MB default
  batchSize @17 :UInt32 = 10000;
  enableCompression @18 :Bool = true;
}

enum ZoneType {
  nodes @0;           # Pure node storage (nodes.zdb)
  edges @1;           # Pure edge storage (edges.zdb)
  indices @2;         # Index storage (indices.zdb)
  transactions @3;    # Transaction log (transactions.zdb)
  strings @4;         # String table (strings.zdb)
  allocator @5;       # Memory allocator data (allocator.zdb)
  mixed @6;           # Mixed content zone
  custom @7;          # Custom zone type
}

enum StorageBackend {
  filesystem @0;      # Local filesystem
  s3 @1;             # Amazon S3
  gcs @2;            # Google Cloud Storage
  azure @3;          # Azure Blob Storage
  memory @4;         # In-memory only
  hybrid @5;         # Local + cloud hybrid
}

enum ConsistencyLevel {
  eventual @0;        # Eventual consistency
  strong @1;         # Strong consistency
  causal @2;         # Causal consistency
}

# Zone-specific data structures
struct NodeZone {
  header @0 :ZoneHeader;
  nodes @1 :List(SimpleNode);
  nodeIndex @2 :List(NodeIndexEntry);
  statistics @3 :NodeZoneStats;
}

struct EdgeZone {
  header @0 :ZoneHeader;
  edges @1 :List(SimpleEdge);
  edgeIndex @2 :List(EdgeIndexEntry);
  statistics @3 :EdgeZoneStats;
}

struct IndexZone {
  header @0 :ZoneHeader;
  indices @1 :List(SimpleIndex);
  bloomFilters @2 :List(BloomFilter);
  statistics @3 :IndexZoneStats;
}

struct TransactionZone {
  header @0 :ZoneHeader;
  transactions @1 :List(SimpleTransaction);
  checkpoints @2 :List(Checkpoint);
  statistics @3 :TransactionZoneStats;
}

# Simplified structures for demo
struct SimpleIndex {
  name @0 :Text;
  type @1 :Text;
  entries @2 :List(Text);
}

struct SimpleTransaction {
  id @0 :UInt64;
  timestamp @1 :UInt64;
  status @2 :Text;
}

# WAL (Write-Ahead Log) structures per zone
struct ZoneWAL {
  header @0 :WALHeader;
  entries @1 :List(WALEntry);
  checksum @2 :UInt64;
}

struct WALHeader {
  zoneId @0 :Text;
  walSequence @1 :UInt64;       # WAL file sequence number
  startLSN @2 :UInt64;          # Log Sequence Number start
  endLSN @3 :UInt64;            # Log Sequence Number end
  created @4 :UInt64;
  entryCount @5 :UInt32;
}

struct WALEntry {
  lsn @0 :UInt64;               # Log Sequence Number
  timestamp @1 :UInt64;
  operation @2 :WALOperation;
  transactionId @3 :UInt64;
  data @4 :Data;               # Serialized operation data
  checksum @5 :UInt32;
}

enum WALOperation {
  insertNode @0;
  updateNode @1;
  deleteNode @2;
  insertEdge @3;
  updateEdge @4;
  deleteEdge @5;
  createIndex @6;
  dropIndex @7;
  beginTransaction @8;
  commitTransaction @9;
  rollbackTransaction @10;
  checkpoint @11;
  zoneCreate @12;
  zoneDelete @13;
  zoneMerge @14;
  zoneSplit @15;
}

# Distributed cluster structures
struct ClusterTopology {
  clusterId @0 :Text;
  zones @1 :List(ZoneLocation);
  coordinatorNodes @2 :List(NodeLocation);
  replicationMap @3 :List(ReplicationEntry);
  shardingStrategy @4 :ShardingStrategy;
}

struct ZoneLocation {
  zoneId @0 :Text;
  storageBackend @1 :StorageBackend;
  endpoint @2 :Text;            # S3 bucket, filesystem path, etc.
  credentials @3 :Text;         # Encrypted credentials
  health @4 :ZoneHealth;
  lastSeen @5 :UInt64;
}

struct NodeLocation {
  nodeId @0 :Text;
  endpoint @1 :Text;
  role @2 :NodeRole;
  capabilities @3 :List(Text);
}

enum NodeRole {
  coordinator @0;
  worker @1;
  storage @2;
  hybrid @3;
}

enum ZoneHealth {
  healthy @0;
  degraded @1;
  unavailable @2;
  recovering @3;
}

struct ReplicationEntry {
  zoneId @0 :Text;
  primaryLocation @1 :ZoneLocation;
  replicas @2 :List(ZoneLocation);
  replicationLag @3 :UInt64;    # Milliseconds
}

enum ShardingStrategy {
  hash @0;           # Hash-based sharding
  range @1;          # Range-based sharding
  directory @2;      # Directory-based sharding
  consistent @3;     # Consistent hashing
}

# Index structures for fast zone lookup
struct NodeIndexEntry {
  nodeId @0 :UInt64;
  tag @1 :Text;
  zoneOffset @2 :UInt64;        # Offset within zone file
  size @3 :UInt32;
}

struct EdgeIndexEntry {
  edgeId @0 :UInt64;
  src @1 :UInt64;
  dst @2 :UInt64;
  tag @3 :Text;
  zoneOffset @4 :UInt64;
  size @5 :UInt32;
}

struct BloomFilter {
  name @0 :Text;
  bitArray @1 :Data;
  hashFunctions @2 :UInt32;
  expectedElements @3 :UInt64;
  falsePositiveRate @4 :Float64;
}

# Statistics for monitoring and optimization
struct NodeZoneStats {
  totalNodes @0 :UInt64;
  deletedNodes @1 :UInt64;
  averageNodeSize @2 :UInt32;
  hotNodes @3 :List(UInt64);    # Most accessed nodes
  lastAccess @4 :UInt64;
}

struct EdgeZoneStats {
  totalEdges @0 :UInt64;
  deletedEdges @1 :UInt64;
  averageEdgeSize @2 :UInt32;
  hotEdges @3 :List(UInt64);
  lastAccess @4 :UInt64;
}

struct IndexZoneStats {
  totalIndices @0 :UInt64;
  indexHits @1 :UInt64;
  indexMisses @2 :UInt64;
  averageSeekTime @3 :UInt64;   # Microseconds
  lastOptimization @4 :UInt64;
}

struct TransactionZoneStats {
  totalTransactions @0 :UInt64;
  committedTransactions @1 :UInt64;
  rollbackTransactions @2 :UInt64;
  averageTransactionTime @3 :UInt64;  # Microseconds
  activeTransactions @4 :UInt32;
}

# Checkpoint and recovery structures
struct Checkpoint {
  checkpointId @0 :UInt64;
  timestamp @1 :UInt64;
  lsn @2 :UInt64;
  zoneState @3 :Data;           # Serialized zone state
  metadata @4 :Text;            # JSON metadata
}

# Simplified structures for zone demo (avoiding complex imports)
struct SimpleNode {
  id @0 :UInt64;
  tag @1 :Text;
  deleted @2 :Bool = false;
  createdTimestamp @3 :UInt64;
  modifiedTimestamp @4 :UInt64;
}

struct SimpleEdge {
  id @0 :UInt64;
  tag @1 :Text;
  src @2 :UInt64;
  dst @3 :UInt64;
  deleted @4 :Bool = false;
  createdTimestamp @5 :UInt64;
  modifiedTimestamp @6 :UInt64;
}

struct SimpleProperty {
  key @0 :Text;
  value @1 :Text;  # Simplified to string for demo
}