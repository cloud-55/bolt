use storage::{Storage, DatabaseId};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Barrier;
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;

const NUM_KEYS: usize = 500_000;
const VALUE_SIZE: usize = 100;

fn generate_value(size: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..size).map(|_| rng.gen_range('a'..='z')).collect()
}

async fn benchmark_sequential_writes(storage: &Storage, name: &str) {
    println!("\n=== Sequential Writes ({}) ===", name);

    let value = generate_value(VALUE_SIZE);
    let start = Instant::now();

    for i in 0..NUM_KEYS {
        storage.set(DatabaseId::Default, &format!("key:{}", i), &value).await;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();

    println!("  {} writes in {:?}", NUM_KEYS, elapsed);
    println!("  {:.0} ops/sec", ops_per_sec);
}

async fn benchmark_sequential_reads(storage: &Storage, name: &str) {
    println!("\n=== Sequential Reads ({}) ===", name);

    let start = Instant::now();

    for i in 0..NUM_KEYS {
        let _ = storage.get(DatabaseId::Default, &format!("key:{}", i)).await;
    }

    let elapsed = start.elapsed();
    let ops_per_sec = NUM_KEYS as f64 / elapsed.as_secs_f64();

    println!("  {} reads in {:?}", NUM_KEYS, elapsed);
    println!("  {:.0} ops/sec", ops_per_sec);
}

async fn benchmark_concurrent_writes(storage: Arc<Storage>, num_tasks: usize, name: &str) {
    println!("\n=== Concurrent Writes ({} tasks) ({}) ===", num_tasks, name);

    let ops_per_task = NUM_KEYS / num_tasks;
    let value = generate_value(VALUE_SIZE);
    let barrier = Arc::new(Barrier::new(num_tasks + 1));

    let mut handles = Vec::new();

    for task_id in 0..num_tasks {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        let value = value.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let start_key = task_id * ops_per_task;

            for i in 0..ops_per_task {
                storage.set(DatabaseId::Default, &format!("ckey:{}:{}", task_id, i), &value).await;
            }

            start_key
        }));
    }

    let start = Instant::now();
    barrier.wait().await;

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_ops = ops_per_task * num_tasks;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!("  {} writes in {:?}", total_ops, elapsed);
    println!("  {:.0} ops/sec", ops_per_sec);
}

async fn benchmark_concurrent_reads(storage: Arc<Storage>, num_tasks: usize, name: &str) {
    println!("\n=== Concurrent Reads ({} tasks) ({}) ===", num_tasks, name);

    let ops_per_task = NUM_KEYS / num_tasks;
    let barrier = Arc::new(Barrier::new(num_tasks + 1));

    let mut handles = Vec::new();

    for task_id in 0..num_tasks {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);

        handles.push(tokio::spawn(async move {
            barrier.wait().await;

            for i in 0..ops_per_task {
                let _ = storage.get(DatabaseId::Default, &format!("ckey:{}:{}", task_id, i)).await;
            }
        }));
    }

    let start = Instant::now();
    barrier.wait().await;

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_ops = ops_per_task * num_tasks;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!("  {} reads in {:?}", total_ops, elapsed);
    println!("  {:.0} ops/sec", ops_per_sec);
}

async fn benchmark_mixed_workload(storage: Arc<Storage>, num_tasks: usize, name: &str) {
    println!("\n=== Mixed Workload (80% reads, 20% writes) ({} tasks) ({}) ===", num_tasks, name);

    let ops_per_task = NUM_KEYS / num_tasks;
    let value = generate_value(VALUE_SIZE);
    let barrier = Arc::new(Barrier::new(num_tasks + 1));

    let mut handles = Vec::new();

    for task_id in 0..num_tasks {
        let storage = Arc::clone(&storage);
        let barrier = Arc::clone(&barrier);
        let value = value.clone();

        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let mut rng = StdRng::seed_from_u64(task_id as u64);

            for i in 0..ops_per_task {
                let key = format!("mkey:{}:{}", task_id, i);
                if rng.gen_ratio(1, 5) {
                    // 20% writes
                    storage.set(DatabaseId::Default, &key, &value).await;
                } else {
                    // 80% reads
                    let _ = storage.get(DatabaseId::Default, &key).await;
                }
            }
        }));
    }

    let start = Instant::now();
    barrier.wait().await;

    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed();
    let total_ops = ops_per_task * num_tasks;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();

    println!("  {} ops in {:?}", total_ops, elapsed);
    println!("  {:.0} ops/sec", ops_per_sec);
}

async fn benchmark_batch_operations(storage: &Storage, name: &str) {
    println!("\n=== Batch Operations ({}) ===", name);

    let value = generate_value(VALUE_SIZE);
    let batch_size = 1000;
    let num_batches = NUM_KEYS / batch_size;

    // MSET benchmark
    let pairs: Vec<(String, String)> = (0..batch_size)
        .map(|i| (format!("bkey:{}", i), value.clone()))
        .collect();

    let start = Instant::now();
    for _ in 0..num_batches {
        storage.mset(DatabaseId::Default, &pairs).await;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = (num_batches * batch_size) as f64 / elapsed.as_secs_f64();
    println!("  MSET: {} ops in {:?} ({:.0} ops/sec)", num_batches * batch_size, elapsed, ops_per_sec);

    // MGET benchmark
    let keys: Vec<String> = (0..batch_size)
        .map(|i| format!("bkey:{}", i))
        .collect();

    let start = Instant::now();
    for _ in 0..num_batches {
        let _ = storage.mget(DatabaseId::Default, &keys).await;
    }
    let elapsed = start.elapsed();
    let ops_per_sec = (num_batches * batch_size) as f64 / elapsed.as_secs_f64();
    println!("  MGET: {} ops in {:?} ({:.0} ops/sec)", num_batches * batch_size, elapsed, ops_per_sec);
}

async fn run_benchmarks(storage: Arc<Storage>, name: &str) {
    println!("\n######################################");
    println!("# Benchmarking: {}", name);
    println!("######################################");

    // Sequential benchmarks
    benchmark_sequential_writes(&storage, name).await;
    benchmark_sequential_reads(&storage, name).await;

    // Concurrent benchmarks with different task counts
    for num_tasks in [10, 100, 500, 1000, 2500, 5000] {
        benchmark_concurrent_writes(Arc::clone(&storage), num_tasks, name).await;
        benchmark_concurrent_reads(Arc::clone(&storage), num_tasks, name).await;
    }

    // Mixed workload with high concurrency
    benchmark_mixed_workload(Arc::clone(&storage), 1000, name).await;
    benchmark_mixed_workload(Arc::clone(&storage), 5000, name).await;

    // Batch operations
    benchmark_batch_operations(&storage, name).await;
}

#[tokio::main]
async fn main() {
    println!("BOLT Storage Benchmark");
    println!("======================");
    println!("Keys: {}", NUM_KEYS);
    println!("Value size: {} bytes", VALUE_SIZE);

    // Benchmark standard mode (in-memory, no WAL)
    let standard_storage = Arc::new(Storage::new());
    run_benchmarks(standard_storage, "Standard Mode (in-memory)").await;

    // Benchmark high-performance mode (in-memory, no WAL)
    let hp_storage = Arc::new(Storage::new_high_performance());
    run_benchmarks(hp_storage, "High-Performance Mode (64 shards, in-memory)").await;

    println!("\n######################################");
    println!("# Benchmark Complete");
    println!("######################################");
}

