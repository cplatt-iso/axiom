#[cfg(test)]
mod tests {
    use super::super::config::ServiceConfig;
    use std::env;

    #[test]
    fn test_service_config_from_env_with_defaults() {
        // Clear any existing environment variables
        let env_vars = [
            "RABBITMQ_HOST",
            "RABBITMQ_PORT", 
            "RABBITMQ_USER",
            "RABBITMQ_PASSWORD",
            "RABBITMQ_QUEUE",
            "DICOM_INCOMING_DIR",
            "DICOM_DUSTBIN_DIR",
            "API_BASE_URL",
            "API_KEY",
            "STUDY_INACTIVITY_TIMEOUT",
            "HEARTBEAT_INTERVAL",
        ];
        
        for var in &env_vars {
            env::remove_var(var);
        }

        let config = ServiceConfig::from_env().expect("Should create config with defaults");

        assert_eq!(config.rabbitmq_host, "rabbitmq");
        assert_eq!(config.rabbitmq_port, 5672);
        assert_eq!(config.rabbitmq_user, "guest");
        assert_eq!(config.rabbitmq_password, "guest");
        assert_eq!(config.rabbitmq_queue, "rules_engine_intake");
        assert_eq!(config.dicom_incoming_dir, "/dicom_data/incoming");
        assert_eq!(config.dicom_dustbin_dir, "/dicom_data/dustbin");
        assert_eq!(config.api_base_url, "http://api:8000");
        assert_eq!(config.api_key, "");
        assert_eq!(config.study_inactivity_timeout, 5);
        assert_eq!(config.heartbeat_interval, 30);
    }

    #[test]
    fn test_service_config_from_env_with_custom_values() {
        // Clear all environment variables first
        let env_vars = [
            "RABBITMQ_HOST",
            "RABBITMQ_PORT", 
            "RABBITMQ_USER",
            "RABBITMQ_PASSWORD",
            "RABBITMQ_QUEUE",
            "DICOM_INCOMING_DIR",
            "DICOM_DUSTBIN_DIR",
            "API_BASE_URL",
            "API_KEY",
            "STUDY_INACTIVITY_TIMEOUT",
            "HEARTBEAT_INTERVAL",
        ];
        
        for var in &env_vars {
            env::remove_var(var);
        }

        // Set custom environment variables
        env::set_var("RABBITMQ_HOST", "custom-rabbitmq");
        env::set_var("RABBITMQ_PORT", "5673");
        env::set_var("RABBITMQ_USER", "custom_user");
        env::set_var("RABBITMQ_PASSWORD", "custom_pass");
        env::set_var("RABBITMQ_QUEUE", "custom_queue");
        env::set_var("DICOM_INCOMING_DIR", "/custom/incoming");
        env::set_var("DICOM_DUSTBIN_DIR", "/custom/dustbin");
        env::set_var("API_BASE_URL", "http://custom-api:9000");
        env::set_var("API_KEY", "custom_api_key");
        env::set_var("STUDY_INACTIVITY_TIMEOUT", "10");
        env::set_var("HEARTBEAT_INTERVAL", "60");

        let config = ServiceConfig::from_env().expect("Should create config with custom values");

        assert_eq!(config.rabbitmq_host, "custom-rabbitmq");
        assert_eq!(config.rabbitmq_port, 5673);
        assert_eq!(config.rabbitmq_user, "custom_user");
        assert_eq!(config.rabbitmq_password, "custom_pass");
        assert_eq!(config.rabbitmq_queue, "custom_queue");
        assert_eq!(config.dicom_incoming_dir, "/custom/incoming");
        assert_eq!(config.dicom_dustbin_dir, "/custom/dustbin");
        assert_eq!(config.api_base_url, "http://custom-api:9000");
        assert_eq!(config.api_key, "custom_api_key");
        assert_eq!(config.study_inactivity_timeout, 10);
        assert_eq!(config.heartbeat_interval, 60);

        // Clean up
        for var in &env_vars {
            env::remove_var(var);
        }
    }

    #[test]
    fn test_service_config_invalid_port() {
        env::set_var("RABBITMQ_PORT", "invalid_port");
        
        let result = ServiceConfig::from_env();
        assert!(result.is_err(), "Should fail with invalid port");
        
        env::remove_var("RABBITMQ_PORT");
    }

    #[test]
    fn test_service_config_invalid_timeout() {
        env::set_var("STUDY_INACTIVITY_TIMEOUT", "invalid_timeout");
        
        let result = ServiceConfig::from_env();
        assert!(result.is_err(), "Should fail with invalid timeout");
        
        env::remove_var("STUDY_INACTIVITY_TIMEOUT");
    }

    #[test]
    fn test_service_config_invalid_heartbeat() {
        env::set_var("HEARTBEAT_INTERVAL", "invalid_heartbeat");
        
        let result = ServiceConfig::from_env();
        assert!(result.is_err(), "Should fail with invalid heartbeat interval");
        
        env::remove_var("HEARTBEAT_INTERVAL");
    }

    #[test]
    fn test_rabbitmq_url_generation() {
        let config = ServiceConfig {
            rabbitmq_host: "test-host".to_string(),
            rabbitmq_port: 5672,
            rabbitmq_user: "test_user".to_string(),
            rabbitmq_password: "test_pass".to_string(),
            rabbitmq_queue: "test_queue".to_string(),
            dicom_incoming_dir: "/test/incoming".to_string(),
            dicom_dustbin_dir: "/test/dustbin".to_string(),
            api_base_url: "http://test-api:8000".to_string(),
            api_key: "test_key".to_string(),
            study_inactivity_timeout: 5,
            heartbeat_interval: 30,
        };

        let url = config.rabbitmq_url();
        assert_eq!(url, "amqp://test_user:test_pass@test-host:5672");
    }

    #[test]
    fn test_service_config_serialization() {
        let config = ServiceConfig {
            rabbitmq_host: "test-host".to_string(),
            rabbitmq_port: 5672,
            rabbitmq_user: "test_user".to_string(),
            rabbitmq_password: "test_pass".to_string(),
            rabbitmq_queue: "test_queue".to_string(),
            dicom_incoming_dir: "/test/incoming".to_string(),
            dicom_dustbin_dir: "/test/dustbin".to_string(),
            api_base_url: "http://test-api:8000".to_string(),
            api_key: "test_key".to_string(),
            study_inactivity_timeout: 5,
            heartbeat_interval: 30,
        };

        // Test serialization to JSON
        let json = serde_json::to_string(&config).expect("Should serialize to JSON");
        assert!(json.contains("test-host"));
        assert!(json.contains("test_user"));

        // Test deserialization from JSON
        let deserialized: ServiceConfig = serde_json::from_str(&json).expect("Should deserialize from JSON");
        assert_eq!(deserialized.rabbitmq_host, config.rabbitmq_host);
        assert_eq!(deserialized.rabbitmq_port, config.rabbitmq_port);
        assert_eq!(deserialized.rabbitmq_user, config.rabbitmq_user);
    }
}