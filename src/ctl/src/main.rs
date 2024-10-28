mod commands;
mod message;
mod opcodes;

use std::env;
use std::io::Result;
use std::net::TcpStream;

use commands::*;
use message::Message;
use opcodes::*;

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_PORT: &str = "2012";

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  bolt-ctl put <key> <value> [database_id]");
    eprintln!("  bolt-ctl get <key> [database_id]");
    eprintln!("  bolt-ctl del <key> [database_id]");
    eprintln!("  bolt-ctl setex <key> <ttl_seconds> <value> [database_id]");
    eprintln!("  bolt-ctl ttl <key> [database_id]");
    eprintln!("  bolt-ctl mget <key1> <key2> ... [--db database_id]");
    eprintln!("  bolt-ctl mset <key1> <val1> <key2> <val2> ... [--db database_id]");
    eprintln!("  bolt-ctl mdel <key1> <key2> ... [--db database_id]");
    eprintln!();
    eprintln!("Counter operations:");
    eprintln!("  bolt-ctl incr <key> [database_id]");
    eprintln!("  bolt-ctl decr <key> [database_id]");
    eprintln!("  bolt-ctl incrby <key> <delta> [database_id]");
    eprintln!();
    eprintln!("CRDT Counter operations (distributed/multi-master safe):");
    eprintln!("  bolt-ctl cincr <key> [database_id]      - CRDT increment");
    eprintln!("  bolt-ctl cdecr <key> [database_id]      - CRDT decrement");
    eprintln!("  bolt-ctl cget <key> [database_id]       - Get CRDT counter value");
    eprintln!("  bolt-ctl cincrby <key> <amount> [database_id] - CRDT increment by");
    eprintln!();
    eprintln!("List operations:");
    eprintln!("  bolt-ctl lpush <key> <val1> [val2...] [--db database_id]");
    eprintln!("  bolt-ctl rpush <key> <val1> [val2...] [--db database_id]");
    eprintln!("  bolt-ctl lpop <key> [database_id]");
    eprintln!("  bolt-ctl rpop <key> [database_id]");
    eprintln!("  bolt-ctl lrange <key> <start> <stop> [database_id]");
    eprintln!("  bolt-ctl llen <key> [database_id]");
    eprintln!();
    eprintln!("Set operations:");
    eprintln!("  bolt-ctl sadd <key> <member1> [member2...] [--db database_id]");
    eprintln!("  bolt-ctl srem <key> <member1> [member2...] [--db database_id]");
    eprintln!("  bolt-ctl smembers <key> [database_id]");
    eprintln!("  bolt-ctl scard <key> [database_id]");
    eprintln!("  bolt-ctl sismember <key> <member> [database_id]");
    eprintln!();
    eprintln!("Utility operations:");
    eprintln!("  bolt-ctl exists <key> [database_id]");
    eprintln!("  bolt-ctl type <key> [database_id]");
    eprintln!("  bolt-ctl keys <pattern> [database_id]");
    eprintln!();
    eprintln!("User management (Admin only):");
    eprintln!("  bolt-ctl useradd <username> <password> <role>  - Create user (role: admin|readwrite|readonly)");
    eprintln!("  bolt-ctl userdel <username>                    - Delete user");
    eprintln!("  bolt-ctl users                                 - List all users");
    eprintln!("  bolt-ctl passwd <username> <new_password>      - Change user password");
    eprintln!("  bolt-ctl role <username> <new_role>            - Change user role");
    eprintln!("  bolt-ctl whoami                                - Show current user info");
    eprintln!();
    eprintln!("Server operations:");
    eprintln!("  bolt-ctl stats");
    eprintln!("  bolt-ctl metrics");
    eprintln!("  bolt-ctl cluster");
    eprintln!();
    eprintln!("Environment variables:");
    eprintln!("  BOLT_HOST     - Server host (default: 127.0.0.1)");
    eprintln!("  BOLT_PORT     - Server port (default: 2012)");
    eprintln!("  BOLT_USER     - Username for authentication");
    eprintln!("  BOLT_PASSWORD - Password for authentication");
}

fn authenticate(stream: &mut TcpStream, username: &str, password: &str) -> Result<()> {
    let auth_msg = Message::auth(username, password);
    auth_msg.send(stream)?;

    let response = Message::receive(stream)?;
    match response.code {
        OP_AUTH_OK => Ok(()),
        OP_AUTH_FAIL => {
            eprintln!("Authentication failed: {}", response.value);
            std::process::exit(1);
        }
        _ => {
            eprintln!("Unexpected response during authentication");
            std::process::exit(1);
        }
    }
}

/// Parse arguments, extracting --db flag if present
fn parse_args_with_db(args: &[String]) -> (Vec<String>, String) {
    let mut remaining = Vec::new();
    let mut database_id = String::new();
    let mut skip_next = false;

    for (i, arg) in args.iter().enumerate() {
        if skip_next {
            skip_next = false;
            continue;
        }
        if arg == "--db" {
            if let Some(db) = args.get(i + 1) {
                database_id = db.clone();
                skip_next = true;
            }
        } else {
            remaining.push(arg.clone());
        }
    }

    (remaining, database_id)
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let command = &args[1];

    let host = env::var("BOLT_HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let port_str = env::var("BOLT_PORT").unwrap_or_else(|_| DEFAULT_PORT.to_string());
    let port: u16 = match port_str.parse() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error: Invalid port number '{}': {}", port_str, e);
            std::process::exit(1);
        }
    };

    let mut stream = TcpStream::connect(format!("{}:{}", &host, &port))?;

    // Authenticate if credentials are provided
    if let (Ok(username), Ok(password)) = (env::var("BOLT_USER"), env::var("BOLT_PASSWORD")) {
        authenticate(&mut stream, &username, &password)?;
    }

    match command.as_str() {
        COMMAND_PUT => {
            if args.len() < 4 {
                eprintln!("Error: put requires at least <key> and <value>");
                print_usage();
                std::process::exit(1);
            }

            let key = &args[2];
            let value = &args[3];
            let database_id = args.get(4).map(|s| s.as_str()).unwrap_or("");

            let message = Message::put(key, value, database_id);
            message.send(&mut stream)?;
            println!("OK PUT {} {}", key, value);
        }
        COMMAND_SETEX => {
            if args.len() < 5 {
                eprintln!("Error: setex requires <key> <ttl_seconds> <value>");
                print_usage();
                std::process::exit(1);
            }

            let key = &args[2];
            let ttl_seconds: u64 = args[3].parse().unwrap_or_else(|_| {
                eprintln!("Error: ttl_seconds must be a number");
                std::process::exit(1);
            });
            let value = &args[4];
            let database_id = args.get(5).map(|s| s.as_str()).unwrap_or("");

            let message = Message::setex(key, ttl_seconds, value, database_id);
            message.send(&mut stream)?;
            println!("OK SETEX {} {} (TTL: {}s)", key, value, ttl_seconds);
        }
        COMMAND_TTL => {
            if args.len() < 3 {
                eprintln!("Error: ttl requires <key>");
                print_usage();
                std::process::exit(1);
            }

            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::ttl(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == 0 && response.value.is_empty() {
                eprintln!("ERR KEY_NOT_FOUND: {}", key);
                std::process::exit(1);
            }
            let ttl_value = &response.value;
            if ttl_value == "-1" {
                println!("{}: no expiration", key);
            } else {
                println!("{}: {}s", key, ttl_value);
            }
        }
        COMMAND_GET => {
            if args.len() < 3 {
                eprintln!("Error: get requires <key>");
                print_usage();
                std::process::exit(1);
            }

            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::get(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.value.is_empty() && response.code == 0 {
                eprintln!("ERR KEY_NOT_FOUND: {}", key);
                std::process::exit(1);
            }
            println!("{}", response.value);
        }
        COMMAND_MGET => {
            if args.len() < 3 {
                eprintln!("Error: mget requires at least one key");
                print_usage();
                std::process::exit(1);
            }

            let (remaining, database_id) = parse_args_with_db(&args[2..]);
            if remaining.is_empty() {
                eprintln!("Error: mget requires at least one key");
                std::process::exit(1);
            }

            let message = Message::mget(&remaining, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            let values: Vec<&str> = response.value.split('\n').collect();
            for (key, value) in remaining.iter().zip(values.iter()) {
                if value.is_empty() {
                    println!("{}: (nil)", key);
                } else {
                    println!("{}: {}", key, value);
                }
            }
        }
        COMMAND_MSET => {
            if args.len() < 4 {
                eprintln!("Error: mset requires at least one key-value pair");
                print_usage();
                std::process::exit(1);
            }

            let (remaining, database_id) = parse_args_with_db(&args[2..]);
            if remaining.len() < 2 || remaining.len() % 2 != 0 {
                eprintln!("Error: mset requires key-value pairs (even number of arguments)");
                std::process::exit(1);
            }

            let keys: Vec<String> = remaining.iter().step_by(2).cloned().collect();
            let values: Vec<String> = remaining.iter().skip(1).step_by(2).cloned().collect();

            let message = Message::mset(&keys, &values, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("OK MSET {} pairs", response.value);
        }
        COMMAND_MDEL => {
            if args.len() < 3 {
                eprintln!("Error: mdel requires at least one key");
                print_usage();
                std::process::exit(1);
            }

            let (remaining, database_id) = parse_args_with_db(&args[2..]);
            if remaining.is_empty() {
                eprintln!("Error: mdel requires at least one key");
                std::process::exit(1);
            }

            let message = Message::mdel(&remaining, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("OK MDEL {} keys deleted", response.value);
        }
        COMMAND_DEL => {
            if args.len() < 3 {
                eprintln!("Error: del requires <key>");
                print_usage();
                std::process::exit(1);
            }

            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::del(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.value.is_empty() && response.code == 0 {
                eprintln!("ERR KEY_NOT_FOUND: {}", key);
                std::process::exit(1);
            }
            println!("OK DEL {}", key);
        }
        COMMAND_STATS => {
            let message = Message::stats();
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        COMMAND_METRICS => {
            let message = Message::metrics();
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            print!("{}", response.value);
        }
        COMMAND_CLUSTER => {
            let message = Message::cluster();
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        // Counter operations
        COMMAND_INCR => {
            if args.len() < 3 {
                eprintln!("Error: incr requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::incr(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        COMMAND_DECR => {
            if args.len() < 3 {
                eprintln!("Error: decr requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::decr(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        COMMAND_INCRBY => {
            if args.len() < 4 {
                eprintln!("Error: incrby requires <key> <delta>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let delta: i64 = args[3].parse().unwrap_or_else(|_| {
                eprintln!("Error: delta must be an integer");
                std::process::exit(1);
            });
            let database_id = args.get(4).map(|s| s.as_str()).unwrap_or("");

            let message = Message::incrby(key, delta, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        // CRDT Counter operations
        COMMAND_CINCR => {
            if args.len() < 3 {
                eprintln!("Error: cincr requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::cincr(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        COMMAND_CDECR => {
            if args.len() < 3 {
                eprintln!("Error: cdecr requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::cdecr(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        COMMAND_CGET => {
            if args.len() < 3 {
                eprintln!("Error: cget requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::cget(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.value.is_empty() && response.code == 0 {
                println!("(nil)");
            } else {
                println!("{}", response.value);
            }
        }
        COMMAND_CINCRBY => {
            if args.len() < 4 {
                eprintln!("Error: cincrby requires <key> <amount>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let amount: u64 = args[3].parse().unwrap_or_else(|_| {
                eprintln!("Error: amount must be a positive integer");
                std::process::exit(1);
            });
            let database_id = args.get(4).map(|s| s.as_str()).unwrap_or("");

            let message = Message::cincrby(key, amount, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        // List operations
        COMMAND_LPUSH => {
            if args.len() < 4 {
                eprintln!("Error: lpush requires <key> <value1> [value2...]");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let (remaining, database_id) = parse_args_with_db(&args[3..]);
            if remaining.is_empty() {
                eprintln!("Error: lpush requires at least one value");
                std::process::exit(1);
            }

            let message = Message::lpush(key, &remaining, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        COMMAND_RPUSH => {
            if args.len() < 4 {
                eprintln!("Error: rpush requires <key> <value1> [value2...]");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let (remaining, database_id) = parse_args_with_db(&args[3..]);
            if remaining.is_empty() {
                eprintln!("Error: rpush requires at least one value");
                std::process::exit(1);
            }

            let message = Message::rpush(key, &remaining, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        COMMAND_LPOP => {
            if args.len() < 3 {
                eprintln!("Error: lpop requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::lpop(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == 0 && response.value.is_empty() {
                println!("(nil)");
            } else {
                println!("{}", response.value);
            }
        }
        COMMAND_RPOP => {
            if args.len() < 3 {
                eprintln!("Error: rpop requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::rpop(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == 0 && response.value.is_empty() {
                println!("(nil)");
            } else {
                println!("{}", response.value);
            }
        }
        COMMAND_LRANGE => {
            if args.len() < 5 {
                eprintln!("Error: lrange requires <key> <start> <stop>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let start: i64 = args[3].parse().unwrap_or_else(|_| {
                eprintln!("Error: start must be an integer");
                std::process::exit(1);
            });
            let stop: i64 = args[4].parse().unwrap_or_else(|_| {
                eprintln!("Error: stop must be an integer");
                std::process::exit(1);
            });
            let database_id = args.get(5).map(|s| s.as_str()).unwrap_or("");

            let message = Message::lrange(key, start, stop, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.value.is_empty() {
                println!("(empty list)");
            } else {
                for (i, item) in response.value.split('\n').enumerate() {
                    println!("{}) {}", i + 1, item);
                }
            }
        }
        COMMAND_LLEN => {
            if args.len() < 3 {
                eprintln!("Error: llen requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::llen(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        // Set operations
        COMMAND_SADD => {
            if args.len() < 4 {
                eprintln!("Error: sadd requires <key> <member1> [member2...]");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let (remaining, database_id) = parse_args_with_db(&args[3..]);
            if remaining.is_empty() {
                eprintln!("Error: sadd requires at least one member");
                std::process::exit(1);
            }

            let message = Message::sadd(key, &remaining, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        COMMAND_SREM => {
            if args.len() < 4 {
                eprintln!("Error: srem requires <key> <member1> [member2...]");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let (remaining, database_id) = parse_args_with_db(&args[3..]);
            if remaining.is_empty() {
                eprintln!("Error: srem requires at least one member");
                std::process::exit(1);
            }

            let message = Message::srem(key, &remaining, &database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        COMMAND_SMEMBERS => {
            if args.len() < 3 {
                eprintln!("Error: smembers requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::smembers(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.value.is_empty() {
                println!("(empty set)");
            } else {
                for (i, member) in response.value.split('\n').enumerate() {
                    println!("{}) {}", i + 1, member);
                }
            }
        }
        COMMAND_SCARD => {
            if args.len() < 3 {
                eprintln!("Error: scard requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::scard(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        COMMAND_SISMEMBER => {
            if args.len() < 4 {
                eprintln!("Error: sismember requires <key> <member>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let member = &args[3];
            let database_id = args.get(4).map(|s| s.as_str()).unwrap_or("");

            let message = Message::sismember(key, member, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        // Utility operations
        COMMAND_EXISTS => {
            if args.len() < 3 {
                eprintln!("Error: exists requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::exists(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("(integer) {}", response.value);
        }
        COMMAND_TYPE => {
            if args.len() < 3 {
                eprintln!("Error: type requires <key>");
                print_usage();
                std::process::exit(1);
            }
            let key = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::key_type(key, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            println!("{}", response.value);
        }
        COMMAND_KEYS => {
            if args.len() < 3 {
                eprintln!("Error: keys requires <pattern>");
                print_usage();
                std::process::exit(1);
            }
            let pattern = &args[2];
            let database_id = args.get(3).map(|s| s.as_str()).unwrap_or("");

            let message = Message::keys(pattern, database_id);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.value.is_empty() {
                println!("(empty list)");
            } else {
                for (i, key) in response.value.split('\n').enumerate() {
                    println!("{}) {}", i + 1, key);
                }
            }
        }
        // User management commands
        COMMAND_USER_ADD => {
            if args.len() < 5 {
                eprintln!("Error: useradd requires <username> <password> <role>");
                eprintln!("       role must be one of: admin, readwrite, readonly");
                print_usage();
                std::process::exit(1);
            }
            let username = &args[2];
            let password = &args[3];
            let role = &args[4];

            let message = Message::user_add(username, password, role);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == OP_AUTH_FAIL {
                eprintln!("Error: {}", response.value);
                std::process::exit(1);
            }
            println!("OK User '{}' created with role '{}'", username, role);
        }
        COMMAND_USER_DEL => {
            if args.len() < 3 {
                eprintln!("Error: userdel requires <username>");
                print_usage();
                std::process::exit(1);
            }
            let username = &args[2];

            let message = Message::user_del(username);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == OP_AUTH_FAIL {
                eprintln!("Error: {}", response.value);
                std::process::exit(1);
            }
            println!("OK User '{}' deleted", username);
        }
        COMMAND_USER_LIST => {
            let message = Message::user_list();
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == OP_AUTH_FAIL {
                eprintln!("Error: {}", response.value);
                std::process::exit(1);
            }
            println!("{}", response.value);
        }
        COMMAND_USER_PASSWD => {
            if args.len() < 4 {
                eprintln!("Error: passwd requires <username> <new_password>");
                print_usage();
                std::process::exit(1);
            }
            let username = &args[2];
            let new_password = &args[3];

            let message = Message::user_passwd(username, new_password);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == OP_AUTH_FAIL {
                eprintln!("Error: {}", response.value);
                std::process::exit(1);
            }
            println!("OK Password changed for user '{}'", username);
        }
        COMMAND_USER_ROLE => {
            if args.len() < 4 {
                eprintln!("Error: role requires <username> <new_role>");
                eprintln!("       role must be one of: admin, readwrite, readonly");
                print_usage();
                std::process::exit(1);
            }
            let username = &args[2];
            let new_role = &args[3];

            let message = Message::user_role(username, new_role);
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == OP_AUTH_FAIL {
                eprintln!("Error: {}", response.value);
                std::process::exit(1);
            }
            println!("OK Role changed to '{}' for user '{}'", new_role, username);
        }
        COMMAND_WHOAMI => {
            let message = Message::whoami();
            message.send(&mut stream)?;

            let response = Message::receive(&mut stream)?;
            if response.code == OP_AUTH_FAIL {
                eprintln!("Error: {}", response.value);
                std::process::exit(1);
            }
            println!("{}", response.value);
        }
        "help" | "--help" | "-h" => {
            print_usage();
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            print_usage();
            std::process::exit(1);
        }
    }

    Ok(())
}

