#[cfg(test)]
mod tests {
    use super::super::listener::DirectoryWatcher;
    use super::super::config::ServiceConfig;
    use std::fs;
    use tempfile::TempDir;
    use tokio::time::Duration;

    fn create_test_config(incoming_dir: &str) -> ServiceConfig {
        ServiceConfig {
            rabbitmq_host: "test-host".to_string(),
            rabbitmq_port: 5672,
            rabbitmq_user: "test_user".to_string(),
            rabbitmq_password: "test_pass".to_string(),
            rabbitmq_queue: "test_queue".to_string(),
            dicom_incoming_dir: incoming_dir.to_string(),
            dicom_dustbin_dir: "/test/dustbin".to_string(),
            api_base_url: "http://test-api:8000".to_string(),
            api_key: "test_key".to_string(),
            study_inactivity_timeout: 2, // Short timeout for testing
            heartbeat_interval: 30,
        }
    }

    #[test]
    fn test_directory_watcher_creation() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let instance_id = "test_instance".to_string();

        let watcher = DirectoryWatcher::new(config.clone(), instance_id.clone());

        // Access should work (testing the structure exists)
        assert_eq!(watcher.instance_id(), &instance_id);
        assert_eq!(watcher.config().dicom_incoming_dir, config.dicom_incoming_dir);
    }

    #[tokio::test]
    async fn test_study_has_dicom_files_with_dcm_files() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create a study directory structure
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");

        // Create a DICOM file
        let dicom_file = study_path.join("image001.dcm");
        fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");

        let has_dicom = watcher.study_has_dicom_files(&study_path).await;
        assert!(has_dicom, "Should detect DICOM files");
    }

    #[tokio::test]
    async fn test_study_has_dicom_files_without_dcm_files() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create a study directory structure
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");

        // Create a non-DICOM file
        let text_file = study_path.join("readme.txt");
        fs::write(&text_file, b"not a dicom file").expect("Should create text file");

        let has_dicom = watcher.study_has_dicom_files(&study_path).await;
        assert!(!has_dicom, "Should not detect DICOM files");
    }

    #[tokio::test]
    async fn test_study_has_dicom_files_empty_directory() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create empty study directory
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");

        let has_dicom = watcher.study_has_dicom_files(&study_path).await;
        assert!(!has_dicom, "Should not detect DICOM files in empty directory");
    }

    #[test]
    fn test_study_has_dicom_files_nonexistent_directory() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let temp_dir = TempDir::new().expect("Should create temp directory");
            let config = create_test_config(temp_dir.path().to_str().unwrap());
            let watcher = DirectoryWatcher::new(config, "test_instance".to_string());

            let nonexistent_path = temp_dir.path().join("nonexistent");
            let has_dicom = watcher.study_has_dicom_files(&nonexistent_path).await;
            assert!(!has_dicom, "Should not detect DICOM files in nonexistent directory");
        });
    }

    #[tokio::test]
    async fn test_scan_for_studies_with_multiple_studies() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let mut watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create multiple study directories with DICOM files
        let studies = [
            ("20231201", "1.2.3.4.5"),
            ("20231202", "1.2.3.4.6"), 
            ("20231203", "1.2.3.4.7"),
        ];

        for (date, uid) in &studies {
            let study_path = temp_dir.path().join(date).join(uid);
            fs::create_dir_all(&study_path).expect("Should create study directory");
            
            let dicom_file = study_path.join("image001.dcm");
            fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");
        }

        // Scan for studies
        watcher.scan_for_studies().await;

        // Check that all studies were found
        assert_eq!(watcher.active_studies_len(), 3, "Should find all three studies");
    }

    #[tokio::test]
    async fn test_scan_for_studies_ignores_studies_without_dicom() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let mut watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create study directory without DICOM files
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");
        
        let text_file = study_path.join("readme.txt");
        fs::write(&text_file, b"not a dicom file").expect("Should create text file");

        // Create study directory with DICOM files
        let study_path_with_dicom = temp_dir.path().join("20231202").join("1.2.3.4.6");
        fs::create_dir_all(&study_path_with_dicom).expect("Should create study directory");
        
        let dicom_file = study_path_with_dicom.join("image001.dcm");
        fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");

        // Scan for studies
        watcher.scan_for_studies().await;

        // Check that only the study with DICOM files was found
        assert_eq!(watcher.active_studies_len(), 1, "Should find only one study with DICOM files");
    }

    #[tokio::test]
    async fn test_process_study_directory_with_dicom_files() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create study directory with multiple DICOM files
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");

        let dicom_files = ["image001.dcm", "image002.dcm", "image003.dcm"];
        for file_name in &dicom_files {
            let dicom_file = study_path.join(file_name);
            fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");
        }

        // Process the study directory (should not panic)
        watcher.process_study_directory(&study_path).await;

        // Verify files still exist (we don't delete them in the current implementation)
        for file_name in &dicom_files {
            let dicom_file = study_path.join(file_name);
            assert!(dicom_file.exists(), "DICOM file should still exist after processing");
        }
    }

    #[tokio::test]
    async fn test_process_study_directory_empty() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let config = create_test_config(temp_dir.path().to_str().unwrap());
        let watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create empty study directory
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");

        // Process the empty study directory (should not panic)
        watcher.process_study_directory(&study_path).await;
    }

    #[tokio::test]
    async fn test_process_inactive_studies_timeout() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let mut config = create_test_config(temp_dir.path().to_str().unwrap());
        config.study_inactivity_timeout = 1; // 1 second timeout
        
        let mut watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create study directory with DICOM files
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");
        
        let dicom_file = study_path.join("image001.dcm");
        fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");

        // Manually add study to active studies with old timestamp
        let old_time = std::time::Instant::now() - Duration::from_secs(2);
        watcher.add_active_study(study_path.clone(), old_time);

        assert_eq!(watcher.active_studies_len(), 1, "Should have one active study");

        // Process inactive studies
        watcher.process_inactive_studies().await;

        // The study should have been removed from active studies after processing
        assert_eq!(watcher.active_studies_len(), 0, "Study should have been processed and removed");
    }

    #[tokio::test]
    async fn test_process_inactive_studies_keeps_active() {
        let temp_dir = TempDir::new().expect("Should create temp directory");
        let mut config = create_test_config(temp_dir.path().to_str().unwrap());
        config.study_inactivity_timeout = 10; // 10 second timeout
        
        let mut watcher = DirectoryWatcher::new(config, "test_instance".to_string());

        // Create study directory with DICOM files
        let study_path = temp_dir.path().join("20231201").join("1.2.3.4.5");
        fs::create_dir_all(&study_path).expect("Should create study directory");
        
        let dicom_file = study_path.join("image001.dcm");
        fs::write(&dicom_file, b"fake dicom content").expect("Should create DICOM file");

        // Manually add study to active studies with recent timestamp
        let recent_time = std::time::Instant::now();
        watcher.add_active_study(study_path.clone(), recent_time);

        assert_eq!(watcher.active_studies_len(), 1, "Should have one active study");

        // Process inactive studies
        watcher.process_inactive_studies().await;

        // The study should still be in active studies since it's not timed out
        assert_eq!(watcher.active_studies_len(), 1, "Study should still be active");
    }
}