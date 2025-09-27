@0x9f2c7d8e5a1b3c4f;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("PMGD::Schema");

# Main database schema that replaces the old .jdb file format
# Supports all existing components with batching and mmap compatibility

struct DatabaseHeader {
  magic @0 :UInt64 = 0x4447444D50474442;  # "PMGDDB" magic number
  version @1 :UInt32 = 1;
  created @2 :UInt64;  # Timestamp
  lastModified @3 :UInt64;
  nodeSize @4 :UInt32;
  edgeSize @5 :UInt32;
  maxStringIdLength @6 :UInt32;
  numAllocators @7 :UInt32;
  baseAddress @8 :UInt64;
  regions @9 :List(RegionInfo);
}

struct RegionInfo {
  name @0 :Text;
  address @1 :UInt64;
  length @2 :UInt64;
  offset @3 :UInt64;  # File offset
  mmapped @4 :Bool;   # Whether this region is memory mapped
}

# Node structure with properties support
struct Node {
  id @0 :UInt64;
  tag @1 :Text;
  properties @2 :List(Property);
  edges @3 :List(UInt64);  # Edge IDs
  incomingEdges @4 :List(UInt64);  # Incoming edge IDs
  deleted @5 :Bool = false;
  createdTimestamp @6 :UInt64;
  modifiedTimestamp @7 :UInt64;
}

# Edge structure with properties support  
struct Edge {
  id @0 :UInt64;
  tag @1 :Text;
  src @2 :UInt64;  # Source node ID
  dst @3 :UInt64;  # Destination node ID
  properties @4 :List(Property);
  deleted @5 :Bool = false;
  createdTimestamp @6 :UInt64;
  modifiedTimestamp @7 :UInt64;
}

# Property value union supporting all PMGD property types
struct Property {
  key @0 :Text;
  value :union {
    boolVal @1 :Bool;
    intVal @2 :Int64;
    floatVal @3 :Float64;
    stringVal @4 :Text;
    timeVal @5 :UInt64;  # Unix timestamp
    blobVal @6 :Data;
  }
}

# String table for efficient string storage
struct StringTable {
  strings @0 :List(StringEntry);
  nextId @1 :UInt32;
}

struct StringEntry {
  id @0 :UInt32;
  value @1 :Text;
  refCount @2 :UInt32;
}

# Index structures for fast lookups
struct Index {
  name @0 :Text;
  type @1 :IndexType;
  entries @2 :List(IndexEntry);
}

enum IndexType {
  nodeTag @0;
  edgeTag @1;
  nodeProperty @2;
  edgeProperty @3;
}

struct IndexEntry {
  key @0 :Text;
  nodeIds @1 :List(UInt64);
  edgeIds @2 :List(UInt64);
}

# Transaction log for ACID properties
struct TransactionLog {
  transactions @0 :List(Transaction);
  nextTransactionId @1 :UInt64;
}

struct Transaction {
  id @0 :UInt64;
  timestamp @1 :UInt64;
  operations @2 :List(Operation);
  status @3 :TransactionStatus;
}

enum TransactionStatus {
  active @0;
  committed @1;
  aborted @2;
}

struct Operation {
  type @0 :OperationType;
  union {
    nodeOp @1 :NodeOperation;
    edgeOp @2 :EdgeOperation;
    propertyOp @3 :PropertyOperation;
  }
}

enum OperationType {
  createNode @0;
  deleteNode @1;
  createEdge @2;
  deleteEdge @3;
  setProperty @4;
  deleteProperty @5;
}

struct NodeOperation {
  nodeId @0 :UInt64;
  tag @1 :Text;
  oldNode @2 :Node;  # For rollback
}

struct EdgeOperation {
  edgeId @0 :UInt64;
  tag @1 :Text;
  src @2 :UInt64;
  dst @3 :UInt64;
  oldEdge @4 :Edge;  # For rollback
}

struct PropertyOperation {
  targetId @0 :UInt64;  # Node or edge ID
  targetType @1 :UInt32;  # 0=node, 1=edge
  property @2 :Property;
  oldProperty @3 :Property;  # For rollback
}

# Batching support for efficient bulk operations
struct BatchOperation {
  id @0 :UInt64;
  timestamp @1 :UInt64;
  operations @2 :List(Operation);
  batchSize @3 :UInt32;
  flushRequired @4 :Bool;
}

# Memory allocator information for mmap regions
struct AllocatorInfo {
  objectSize @0 :UInt32;
  offset @1 :UInt64;
  poolSize @2 :UInt64;
  freeBlocks @3 :List(UInt64);
  nextFreeBlock @4 :UInt64;
}

# Main database container
struct Database {
  header @0 :DatabaseHeader;
  nodes @1 :List(Node);
  edges @2 :List(Edge);
  stringTable @3 :StringTable;
  indices @4 :List(Index);
  transactionLog @5 :TransactionLog;
  allocators @6 :List(AllocatorInfo);
  batchOperations @7 :List(BatchOperation);
  
  # Compatibility with old format
  nodeCount @8 :UInt64;
  edgeCount @9 :UInt64;
  nextNodeId @10 :UInt64;
  nextEdgeId @11 :UInt64;
}