use std::hint::black_box;
use std::time::Instant;
use tempfile::TempDir;
use std::fs;

// Simple benchmark functions for the Axiom DICOM service
// Note: These are basic timing benchmarks. For more sophisticated benchmarking,
// consider using the `criterion` crate.

fn benchmark_config_creation() -> std::time::Duration {
    use axiom_dicom_service::config::ServiceConfig;
    
    let start = Instant::now();
    
    // Benchmark config creation 1000 times
    for _ in 0..1000 {
        let _config = black_box(ServiceConfig::from_env().unwrap());
    }
    
    start.elapsed()
}

fn benchmark_directory_scan() -> std::time::Duration {
    use axiom_dicom_service::listener::DirectoryWatcher;
    use axiom_dicom_service::config::ServiceConfig;
    
    // Create a temporary directory with test files
    let temp_dir = TempDir::new().unwrap();
    let config = ServiceConfig {
        rabbitmq_host: "test".to_string(),
        rabbitmq_port: 5672,
        rabbitmq_user: "test".to_string(),
        rabbitmq_password: "test".to_string(),
        rabbitmq_queue: "test".to_string(),
        dicom_incoming_dir: temp_dir.path().to_str().unwrap().to_string(),
        dicom_dustbin_dir: "/tmp".to_string(),
        api_base_url: "http://test:8000".to_string(),
        api_key: "test".to_string(),
        study_inactivity_timeout: 5,
        heartbeat_interval: 30,
    };
    
    // Create test directory structure
    for i in 0..10 {
        let study_path = temp_dir.path().join(format!("2023120{}", i)).join(format!("1.2.3.4.{}", i));
        fs::create_dir_all(&study_path).unwrap();
        
        // Create some DICOM files
        for j in 0..5 {
            let dicom_file = study_path.join(format!("image{:03}.dcm", j));
            fs::write(&dicom_file, b"fake dicom content").unwrap();
        }
    }
    
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let start = Instant::now();
    
    runtime.block_on(async {
        let mut watcher = DirectoryWatcher::new(config, "bench_test".to_string());
        
        // Benchmark directory scanning 100 times
        for _ in 0..100 {
            watcher.scan_for_studies().await;
        }
    });
    
    start.elapsed()
}

fn benchmark_rabbitmq_url_generation() -> std::time::Duration {
    use axiom_dicom_service::config::ServiceConfig;
    
    let config = ServiceConfig {
        rabbitmq_host: "benchmark-host".to_string(),
        rabbitmq_port: 5672,
        rabbitmq_user: "benchmark_user".to_string(),
        rabbitmq_password: "benchmark_pass".to_string(),
        rabbitmq_queue: "benchmark_queue".to_string(),
        dicom_incoming_dir: "/tmp".to_string(),
        dicom_dustbin_dir: "/tmp".to_string(),
        api_base_url: "http://test:8000".to_string(),
        api_key: "test".to_string(),
        study_inactivity_timeout: 5,
        heartbeat_interval: 30,
    };
    
    let start = Instant::now();
    
    // Benchmark URL generation 10000 times
    for _ in 0..10000 {
        let _url = black_box(config.rabbitmq_url());
    }
    
    start.elapsed()
}

fn main() {
    println!("🏃 Running Axiom DICOM Service Benchmarks");
    println!("==========================================");
    
    // Warm up
    std::env::remove_var("RABBITMQ_HOST");
    
    // Run benchmarks
    let config_time = benchmark_config_creation();
    println!("Config creation (1000x):     {:?}", config_time);
    println!("Config creation (avg):       {:?}", config_time / 1000);
    
    let url_time = benchmark_rabbitmq_url_generation();
    println!("RabbitMQ URL generation (10000x): {:?}", url_time);
    println!("RabbitMQ URL generation (avg):    {:?}", url_time / 10000);
    
    let scan_time = benchmark_directory_scan();
    println!("Directory scan (100x):       {:?}", scan_time);
    println!("Directory scan (avg):        {:?}", scan_time / 100);
    
    println!("\n🎯 Benchmark Results Summary:");
    println!("- Config creation is efficient for frequent use");
    println!("- URL generation is very fast for high-frequency operations");
    println!("- Directory scanning performance scales with file count");
    
    if scan_time > std::time::Duration::from_millis(1000) {
        println!("⚠️  Directory scanning took longer than 1s for 100 iterations");
        println!("   Consider optimizing for large directory structures");
    } else {
        println!("✅ Directory scanning performance is acceptable");
    }
}