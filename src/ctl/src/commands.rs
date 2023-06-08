// Command name constants

pub const COMMAND_PUT: &str = "put";
pub const COMMAND_GET: &str = "get";
pub const COMMAND_DEL: &str = "del";
pub const COMMAND_STATS: &str = "stats";
pub const COMMAND_CLUSTER: &str = "cluster";
pub const COMMAND_SETEX: &str = "setex";
pub const COMMAND_TTL: &str = "ttl";
pub const COMMAND_MGET: &str = "mget";
pub const COMMAND_MSET: &str = "mset";
pub const COMMAND_MDEL: &str = "mdel";
pub const COMMAND_METRICS: &str = "metrics";

// Counter commands
pub const COMMAND_INCR: &str = "incr";
pub const COMMAND_DECR: &str = "decr";
pub const COMMAND_INCRBY: &str = "incrby";

// CRDT Counter commands (distributed counters)
pub const COMMAND_CINCR: &str = "cincr";
pub const COMMAND_CDECR: &str = "cdecr";
pub const COMMAND_CGET: &str = "cget";
pub const COMMAND_CINCRBY: &str = "cincrby";

// List commands
pub const COMMAND_LPUSH: &str = "lpush";
pub const COMMAND_RPUSH: &str = "rpush";
pub const COMMAND_LPOP: &str = "lpop";
pub const COMMAND_RPOP: &str = "rpop";
pub const COMMAND_LRANGE: &str = "lrange";
pub const COMMAND_LLEN: &str = "llen";

// Set commands
pub const COMMAND_SADD: &str = "sadd";
pub const COMMAND_SREM: &str = "srem";
pub const COMMAND_SMEMBERS: &str = "smembers";
pub const COMMAND_SCARD: &str = "scard";
pub const COMMAND_SISMEMBER: &str = "sismember";

// Utility commands
pub const COMMAND_EXISTS: &str = "exists";
pub const COMMAND_TYPE: &str = "type";
pub const COMMAND_KEYS: &str = "keys";

// User management commands
pub const COMMAND_USER_ADD: &str = "useradd";
pub const COMMAND_USER_DEL: &str = "userdel";
pub const COMMAND_USER_LIST: &str = "users";
pub const COMMAND_USER_PASSWD: &str = "passwd";
pub const COMMAND_USER_ROLE: &str = "role";
pub const COMMAND_WHOAMI: &str = "whoami";
