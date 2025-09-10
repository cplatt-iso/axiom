use axiom_dicom_service::config::ServiceConfig;
use axiom_dicom_service::listener::DirectoryWatcher;
use std::env;
use tempfile::TempDir;
use std::fs;

#[tokio::test]
async fn test_full_integration() {
    // Clear environment variables
    env::remove_var("RABBITMQ_HOST");
    env::remove_var("DICOM_INCOMING_DIR");
    
    // Create temporary directory
    let temp_dir = TempDir::new().expect("Should create temp directory");
    
    // Set environment variable for incoming directory
    env::set_var("DICOM_INCOMING_DIR", temp_dir.path().to_str().unwrap());
    
    // Create config from environment
    let config = ServiceConfig::from_env().expect("Should create config");
    
    // Create directory watcher
    let mut watcher = DirectoryWatcher::new(config, "integration_test".to_string());
    
    // Create test study structure
    let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
    fs::create_dir_all(&study_path).expect("Should create study directory");
    
    // Create DICOM file
    let dicom_file = study_path.join("image001.dcm");
    fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");
    
    // Test directory scanning
    watcher.scan_for_studies().await;
    assert_eq!(watcher.active_studies_len(), 1, "Should find the study");
    
    // Test DICOM file detection
    assert!(watcher.study_has_dicom_files(&study_path).await, "Should detect DICOM files");
    
    // Test study processing
    watcher.process_study_directory(&study_path).await;
    
    // Verify file still exists (our implementation doesn't delete files)
    assert!(dicom_file.exists(), "DICOM file should still exist");
    
    // Clean up
    env::remove_var("DICOM_INCOMING_DIR");
}

#[test]
fn test_config_rabbitmq_url_integration() {
    // Test that config can generate valid RabbitMQ URLs
    env::remove_var("RABBITMQ_HOST");
    env::remove_var("RABBITMQ_PORT");
    env::remove_var("RABBITMQ_USER");
    env::remove_var("RABBITMQ_PASSWORD");
    
    let config = ServiceConfig::from_env().expect("Should create config");
    let url = config.rabbitmq_url();
    
    // Should be a valid AMQP URL with defaults
    assert!(url.starts_with("amqp://"));
    assert!(url.contains("guest:guest"));
    assert!(url.contains("rabbitmq:5672"));
}

#[tokio::test]
async fn test_study_timeout_integration() {
    // Test the full timeout workflow
    let temp_dir = TempDir::new().expect("Should create temp directory");
    
    let mut config = ServiceConfig {
        rabbitmq_host: "test".to_string(),
        rabbitmq_port: 5672,
        rabbitmq_user: "test".to_string(),
        rabbitmq_password: "test".to_string(),
        rabbitmq_queue: "test".to_string(),
        dicom_incoming_dir: temp_dir.path().to_str().unwrap().to_string(),
        dicom_dustbin_dir: "/tmp".to_string(),
        api_base_url: "http://test:8000".to_string(),
        api_key: "test".to_string(),
        study_inactivity_timeout: 1, // 1 second for fast test
        heartbeat_interval: 30,
    };
    
    let mut watcher = DirectoryWatcher::new(config, "timeout_test".to_string());
    
    // Create study with DICOM file
    let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
    fs::create_dir_all(&study_path).expect("Should create study directory");
    
    let dicom_file = study_path.join("image001.dcm");
    fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");
    
    // Add study manually with old timestamp
    let old_time = std::time::Instant::now() - std::time::Duration::from_secs(2);
    watcher.add_active_study(study_path.clone(), old_time);
    
    assert_eq!(watcher.active_studies_len(), 1, "Should have one active study");
    
    // Process inactive studies - should remove the timed out study
    watcher.process_inactive_studies().await;
    
    assert_eq!(watcher.active_studies_len(), 0, "Study should have been processed and removed");
}
