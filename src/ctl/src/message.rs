use std::io::{Read, Write, Result};
use byteordered::byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crate::opcodes::*;

const MAX_MESSAGE_SIZE: u64 = 64 * 1024 * 1024; // 64 MB

/// Wire protocol format (same as server):
/// 1. code (u16)
/// 2. database_id_length (u32)
/// 3. key_length (u32)
/// 4. value_length (u32)
/// 5. database_id (bytes) - if length > 0
/// 6. key (bytes)
/// 7. value (bytes)

#[derive(Debug)]
pub struct Message {
    pub code: u16,
    pub key: String,
    pub value: String,
    pub database_id: String,
}

impl Message {
    pub fn put(key: &str, value: &str, database_id: &str) -> Self {
        Message {
            code: OP_PUT,
            key: key.to_string(),
            value: value.to_string(),
            database_id: database_id.to_string(),
        }
    }

    pub fn get(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_GET,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn del(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_DEL,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn auth(username: &str, password: &str) -> Self {
        Message {
            code: OP_AUTH,
            key: username.to_string(),
            value: password.to_string(),
            database_id: String::new(),
        }
    }

    pub fn stats() -> Self {
        Message {
            code: OP_STATS,
            key: String::new(),
            value: String::new(),
            database_id: String::new(),
        }
    }

    pub fn cluster() -> Self {
        Message {
            code: OP_CLUSTER_STATUS,
            key: String::new(),
            value: String::new(),
            database_id: String::new(),
        }
    }

    pub fn setex(key: &str, ttl_seconds: u64, value: &str, database_id: &str) -> Self {
        Message {
            code: OP_SETEX,
            key: key.to_string(),
            value: format!("{}:{}", ttl_seconds, value),
            database_id: database_id.to_string(),
        }
    }

    pub fn ttl(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_TTL,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn mget(keys: &[String], database_id: &str) -> Self {
        Message {
            code: OP_MGET,
            key: keys.join("\n"),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn mset(keys: &[String], values: &[String], database_id: &str) -> Self {
        Message {
            code: OP_MSET,
            key: keys.join("\n"),
            value: values.join("\n"),
            database_id: database_id.to_string(),
        }
    }

    pub fn mdel(keys: &[String], database_id: &str) -> Self {
        Message {
            code: OP_MDEL,
            key: keys.join("\n"),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn metrics() -> Self {
        Message {
            code: OP_METRICS,
            key: String::new(),
            value: String::new(),
            database_id: String::new(),
        }
    }

    // Counter operations
    pub fn incr(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_INCR,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn decr(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_DECR,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn incrby(key: &str, delta: i64, database_id: &str) -> Self {
        Message {
            code: OP_INCRBY,
            key: key.to_string(),
            value: delta.to_string(),
            database_id: database_id.to_string(),
        }
    }

    // CRDT Counter operations (distributed counters)
    pub fn cincr(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_CINCR,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn cdecr(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_CDECR,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn cget(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_CGET,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn cincrby(key: &str, amount: u64, database_id: &str) -> Self {
        Message {
            code: OP_CINCRBY,
            key: key.to_string(),
            value: amount.to_string(),
            database_id: database_id.to_string(),
        }
    }

    // List operations
    pub fn lpush(key: &str, values: &[String], database_id: &str) -> Self {
        Message {
            code: OP_LPUSH,
            key: key.to_string(),
            value: values.join("\n"),
            database_id: database_id.to_string(),
        }
    }

    pub fn rpush(key: &str, values: &[String], database_id: &str) -> Self {
        Message {
            code: OP_RPUSH,
            key: key.to_string(),
            value: values.join("\n"),
            database_id: database_id.to_string(),
        }
    }

    pub fn lpop(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_LPOP,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn rpop(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_RPOP,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn lrange(key: &str, start: i64, stop: i64, database_id: &str) -> Self {
        Message {
            code: OP_LRANGE,
            key: key.to_string(),
            value: format!("{}:{}", start, stop),
            database_id: database_id.to_string(),
        }
    }

    pub fn llen(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_LLEN,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    // Set operations
    pub fn sadd(key: &str, members: &[String], database_id: &str) -> Self {
        Message {
            code: OP_SADD,
            key: key.to_string(),
            value: members.join("\n"),
            database_id: database_id.to_string(),
        }
    }

    pub fn srem(key: &str, members: &[String], database_id: &str) -> Self {
        Message {
            code: OP_SREM,
            key: key.to_string(),
            value: members.join("\n"),
            database_id: database_id.to_string(),
        }
    }

    pub fn smembers(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_SMEMBERS,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn scard(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_SCARD,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn sismember(key: &str, member: &str, database_id: &str) -> Self {
        Message {
            code: OP_SISMEMBER,
            key: key.to_string(),
            value: member.to_string(),
            database_id: database_id.to_string(),
        }
    }

    // Utility operations
    pub fn exists(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_EXISTS,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn key_type(key: &str, database_id: &str) -> Self {
        Message {
            code: OP_TYPE,
            key: key.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    pub fn keys(pattern: &str, database_id: &str) -> Self {
        Message {
            code: OP_KEYS,
            key: pattern.to_string(),
            value: String::new(),
            database_id: database_id.to_string(),
        }
    }

    // User management operations
    pub fn user_add(username: &str, password: &str, role: &str) -> Self {
        Message {
            code: OP_USER_ADD,
            key: username.to_string(),
            value: format!("{}:{}", password, role),
            database_id: String::new(),
        }
    }

    pub fn user_del(username: &str) -> Self {
        Message {
            code: OP_USER_DEL,
            key: username.to_string(),
            value: String::new(),
            database_id: String::new(),
        }
    }

    pub fn user_list() -> Self {
        Message {
            code: OP_USER_LIST,
            key: String::new(),
            value: String::new(),
            database_id: String::new(),
        }
    }

    pub fn user_passwd(username: &str, new_password: &str) -> Self {
        Message {
            code: OP_USER_PASSWD,
            key: username.to_string(),
            value: new_password.to_string(),
            database_id: String::new(),
        }
    }

    pub fn user_role(username: &str, role: &str) -> Self {
        Message {
            code: OP_USER_ROLE,
            key: username.to_string(),
            value: role.to_string(),
            database_id: String::new(),
        }
    }

    pub fn whoami() -> Self {
        Message {
            code: OP_WHOAMI,
            key: String::new(),
            value: String::new(),
            database_id: String::new(),
        }
    }

    pub fn send<W: Write>(&self, stream: &mut W) -> Result<()> {
        let database_id_length = self.database_id.len() as u32;
        let key_length = self.key.len() as u32;
        let value_length = self.value.len() as u32;

        // Write header
        stream.write_u16::<BigEndian>(self.code)?;
        stream.write_u32::<BigEndian>(database_id_length)?;
        stream.write_u32::<BigEndian>(key_length)?;
        stream.write_u32::<BigEndian>(value_length)?;

        // Write body
        if !self.database_id.is_empty() {
            stream.write_all(self.database_id.as_bytes())?;
        }
        stream.write_all(self.key.as_bytes())?;
        stream.write_all(self.value.as_bytes())?;

        stream.flush()?;
        Ok(())
    }

    pub fn receive<R: Read>(stream: &mut R) -> Result<Message> {
        // Read header
        let code = stream.read_u16::<BigEndian>()?;
        let database_id_length = stream.read_u32::<BigEndian>()?;
        let key_length = stream.read_u32::<BigEndian>()?;
        let value_length = stream.read_u32::<BigEndian>()?;

        // Validate total message size to prevent OOM attacks
        let total_size = database_id_length as u64 + key_length as u64 + value_length as u64;
        if total_size > MAX_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("message too large: {} bytes (max: {} bytes)", total_size, MAX_MESSAGE_SIZE),
            ));
        }

        // Read body
        let database_id = if database_id_length > 0 {
            let mut buf = vec![0u8; database_id_length as usize];
            stream.read_exact(&mut buf)?;
            String::from_utf8_lossy(&buf).to_string()
        } else {
            String::new()
        };

        let mut key = vec![0u8; key_length as usize];
        stream.read_exact(&mut key)?;
        let key = String::from_utf8_lossy(&key).to_string();

        let mut value = vec![0u8; value_length as usize];
        stream.read_exact(&mut value)?;
        let value = String::from_utf8_lossy(&value).to_string();

        Ok(Message {
            code,
            key,
            value,
            database_id,
        })
    }
}
