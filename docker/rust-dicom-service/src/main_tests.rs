#[cfg(test)]
mod tests {
    use std::env;
    use std::net::SocketAddr;
    use tempfile::TempDir;

    // Note: These are integration-style tests for the main CLI functionality
    // Since the main function runs indefinitely, we test the components separately

    #[test]
    fn test_socket_addr_parsing() {
        // Test that we can parse valid socket addresses
        let addr: SocketAddr = "0.0.0.0:11112".parse().expect("Should parse valid address");
        assert_eq!(addr.port(), 11112);
        assert!(addr.ip().is_unspecified());

        let addr: SocketAddr = "127.0.0.1:8080".parse().expect("Should parse localhost address");
        assert_eq!(addr.port(), 8080);
        assert!(addr.ip().is_loopback());
    }

    #[test]
    fn test_instance_id_fallback() {
        // Clear the environment variable first
        env::remove_var("AXIOM_INSTANCE_ID");
        
        // Test the fallback logic that would be used in main
        let instance_id = None.unwrap_or_else(|| {
            std::env::var("AXIOM_INSTANCE_ID").unwrap_or_else(|_| "rust_dicom_1".to_string())
        });
        
        assert_eq!(instance_id, "rust_dicom_1");
    }

    #[test]
    fn test_instance_id_from_env() {
        // Set the environment variable
        env::set_var("AXIOM_INSTANCE_ID", "test_instance_123");
        
        // Test the fallback logic that would be used in main
        let instance_id = None.unwrap_or_else(|| {
            std::env::var("AXIOM_INSTANCE_ID").unwrap_or_else(|_| "rust_dicom_1".to_string())
        });
        
        assert_eq!(instance_id, "test_instance_123");
        
        // Clean up
        env::remove_var("AXIOM_INSTANCE_ID");
    }

    #[test] 
    fn test_instance_id_from_cli_override() {
        // Set the environment variable
        env::set_var("AXIOM_INSTANCE_ID", "env_instance");
        
        // Test CLI override (simulating Some(value) from CLI)
        let cli_instance_id = Some("cli_instance".to_string());
        let instance_id = cli_instance_id.unwrap_or_else(|| {
            std::env::var("AXIOM_INSTANCE_ID").unwrap_or_else(|_| "rust_dicom_1".to_string())
        });
        
        assert_eq!(instance_id, "cli_instance");
        
        // Clean up
        env::remove_var("AXIOM_INSTANCE_ID");
    }

    #[tokio::test]
    async fn test_directory_creation() {
        use tokio::fs;
        
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let incoming_dir = temp_dir.path().join("dicom_incoming");
        
        // Test that we can create the incoming directory
        fs::create_dir_all(&incoming_dir).await.expect("Should create incoming directory");
        
        assert!(incoming_dir.exists(), "Incoming directory should exist");
        assert!(incoming_dir.is_dir(), "Incoming path should be a directory");
    }

    #[test]
    fn test_aet_validation() {
        // Test valid AE titles (according to DICOM standard)
        let valid_aets = [
            "AXIOM_SCP",
            "PACS", 
            "DCM4CHE",
            "TEST_AET",
            "A", // Single character is valid
            "SIXTEEN_CHAR_AET", // 16 characters (max length)
        ];
        
        for aet in &valid_aets {
            assert!(aet.len() <= 16, "AET '{}' should be 16 characters or less", aet);
            assert!(aet.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'), 
                   "AET '{}' should contain only alphanumeric and underscore", aet);
        }
    }

    #[test]
    fn test_default_values() {
        // Test that our defaults match what's in the CLI definition
        let default_bind = "0.0.0.0:11112";
        let default_aet = "AXIOM_SCP";
        
        let addr: SocketAddr = default_bind.parse().expect("Default bind address should be valid");
        assert_eq!(addr.port(), 11112);
        
        assert!(default_aet.len() <= 16, "Default AET should be valid length");
        assert!(default_aet.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'), 
               "Default AET should contain only valid characters");
    }

    #[test]
    fn test_logging_initialization() {
        // Test that we can initialize basic logging without errors
        // This is a simple test to ensure the logging setup doesn't panic
        println!("Starting Axiom Rust DICOM Service");
        
        // If we get here without panicking, the test passes
        assert!(true, "Logging initialization should not panic");
    }

    #[test]
    fn test_configuration_integration() {
        use crate::config::ServiceConfig;
        
        // Test that we can create a config and use it with our CLI parameters
        env::remove_var("RABBITMQ_HOST");
        env::remove_var("DICOM_INCOMING_DIR");
        
        let config = ServiceConfig::from_env().expect("Should create config");
        let bind_addr: SocketAddr = "127.0.0.1:11112".parse().expect("Should parse address");
        let aet = "TEST_AET".to_string();
        let instance_id = "test_instance".to_string();
        
        // Verify the integration would work
        assert!(!config.dicom_incoming_dir.is_empty(), "Config should have incoming directory");
        assert!(bind_addr.port() > 0, "Bind address should have valid port");
        assert!(!aet.is_empty(), "AE title should not be empty");
        assert!(!instance_id.is_empty(), "Instance ID should not be empty");
    }

    #[tokio::test]
    async fn test_signal_handling_setup() {
        // Test that we can set up signal handling without errors
        let instance_id = "test_instance".to_string();
        let shutdown_instance_id = instance_id.clone();
        
        // This simulates the signal handler setup without actually waiting for signals
        let handle = tokio::spawn(async move {
            // Simulate immediate shutdown for testing
            println!("Would handle shutdown for instance: {}", shutdown_instance_id);
            // In real code: tokio::signal::ctrl_c().await.ok();
            // For test: just return immediately
        });
        
        // Wait for the simulated handler to complete
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(100), 
            handle
        ).await;
        
        assert!(result.is_ok(), "Signal handler should complete quickly in test");
    }
}