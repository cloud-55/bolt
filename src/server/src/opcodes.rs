// Operation codes for the wire protocol

// Basic operations
pub const OP_PUT: u16 = 1;
pub const OP_GET: u16 = 2;
pub const OP_DEL: u16 = 3;
pub const OP_DB_SWITCH: u16 = 4;
pub const OP_GET_ALL: u16 = 5;

// Authentication operations
pub const OP_AUTH: u16 = 6;
pub const OP_AUTH_OK: u16 = 7;
pub const OP_AUTH_FAIL: u16 = 8;

// Stats and status
pub const OP_STATS: u16 = 9;
pub const OP_CLUSTER_STATUS: u16 = 10;

// TTL operations
pub const OP_SETEX: u16 = 11;
pub const OP_TTL: u16 = 12;

// Batch operations
pub const OP_MGET: u16 = 13;
pub const OP_MSET: u16 = 14;
pub const OP_MDEL: u16 = 15;

// Metrics
pub const OP_METRICS: u16 = 16;

// Counter operations
pub const OP_INCR: u16 = 20;
pub const OP_DECR: u16 = 21;
pub const OP_INCRBY: u16 = 22;

// CRDT Counter operations (distributed counters)
pub const OP_CINCR: u16 = 23;
pub const OP_CDECR: u16 = 24;
pub const OP_CGET: u16 = 25;
pub const OP_CINCRBY: u16 = 26;

// List operations
pub const OP_LPUSH: u16 = 30;
pub const OP_RPUSH: u16 = 31;
pub const OP_LPOP: u16 = 32;
pub const OP_RPOP: u16 = 33;
pub const OP_LRANGE: u16 = 34;
pub const OP_LLEN: u16 = 35;

// Set operations
pub const OP_SADD: u16 = 40;
pub const OP_SREM: u16 = 41;
pub const OP_SMEMBERS: u16 = 42;
pub const OP_SCARD: u16 = 43;
pub const OP_SISMEMBER: u16 = 44;

// Utility operations
pub const OP_EXISTS: u16 = 50;
pub const OP_TYPE: u16 = 51;
pub const OP_KEYS: u16 = 52;

// User management operations
pub const OP_USER_ADD: u16 = 60;
pub const OP_USER_DEL: u16 = 61;
pub const OP_USER_LIST: u16 = 62;
pub const OP_USER_PASSWD: u16 = 63;
pub const OP_USER_ROLE: u16 = 64;
pub const OP_WHOAMI: u16 = 65;

// Pub/Sub operations (Agent Coordination)
pub const OP_SUBSCRIBE: u16 = 70;      // SUBSCRIBE <task_type> <agent_id>
pub const OP_UNSUBSCRIBE: u16 = 71;    // UNSUBSCRIBE <task_type> <agent_id>
pub const OP_NOTIFY: u16 = 72;         // NOTIFY <task_data> (server → client)

// Task management operations
pub const OP_TASK_CREATE: u16 = 73;    // Create task for distribution
pub const OP_TASK_COMPLETE: u16 = 74;  // Mark task as completed
pub const OP_TASK_FAIL: u16 = 75;      // Mark task as failed
pub const OP_TASK_STATUS: u16 = 76;    // Get task status
pub const OP_TASK_LIST: u16 = 77;      // List tasks by type/status
pub const OP_TASK_CLAIM: u16 = 78;     // Manually claim a task
