use logger::logger;
use server::server;

// Use jemalloc as the global allocator for better memory efficiency
// jemalloc reduces memory fragmentation significantly compared to the system allocator
// Used by Redis, Firefox, and other high-performance systems
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() {
    logger::setup_logging();

    let ascii_logo = r#"
   ___  ____  __ ______
  / _ )/ __ \/ //_  __/
 / _  / /_/ / /__/ /
/____/\____/____/_/
-----------------------------------------------
High-performance in-memory key-value database
-----------------------------------------------
    "#;

    println!("{}", ascii_logo);

    let server = match server::Server::new() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize server: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = server.run().await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
