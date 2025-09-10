use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::interval;
use tracing::{debug, info, warn, error};
use walkdir::WalkDir;

use crate::config::ServiceConfig;
use crate::database::Database;
use crate::rabbitmq::RabbitMQClient;

/// Simple DICOM listener function - like the original but with optional RabbitMQ
pub async fn start_simple_listener(
    bind: SocketAddr,
    ae_title: String,
    instance_id: String,
    config: ServiceConfig,
) -> anyhow::Result<()> {
    println!("Starting simple DICOM listener on {}", bind);
    println!("AE Title: {}, Instance: {}", ae_title, instance_id);
    
    // For now, just run a simple loop to keep the service alive
    // This can be enhanced later with actual DICOM protocol handling
    let mut interval = interval(Duration::from_secs(30));
    
    loop {
        interval.tick().await;
        println!("Rust DICOM listener is alive - {}", ae_title);
    }
}

/// Directory watcher for batching received DICOM files
pub struct DirectoryWatcher {
    active_studies: HashMap<PathBuf, Instant>,
    config: ServiceConfig,
    instance_id: String,
    database: Database,
    rabbitmq_client: RabbitMQClient,
}

impl DirectoryWatcher {
    pub async fn new(
        config: ServiceConfig,
        instance_id: String,
        database: Database,
    ) -> anyhow::Result<Self> {
        let rabbitmq_client = RabbitMQClient::new(
            &config.api_base_url,
            &config.api_key,
            &config.rabbitmq_queue,
            &config.celery_task_name,
        ).await?;

        Ok(Self {
            active_studies: HashMap::new(),
            config,
            instance_id,
            database,
            rabbitmq_client,
        })
    }

    // Getter methods for testing
    #[cfg(any(test, feature = "test-utils"))]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn config(&self) -> &ServiceConfig {
        &self.config
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn active_studies_len(&self) -> usize {
        self.active_studies.len()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn add_active_study(&mut self, study_path: std::path::PathBuf, time: std::time::Instant) {
        self.active_studies.insert(study_path, time);
    }

    pub async fn start_watching(&mut self) {
        let mut scan_interval = interval(Duration::from_secs(1));

        info!(
            event = "Starting directory watcher",
            directory = %self.config.dicom_incoming_dir,
            timeout = %self.config.study_inactivity_timeout
        );

        loop {
            tokio::select! {
                _ = scan_interval.tick() => {
                    self.scan_for_studies().await;
                    self.process_inactive_studies().await;
                }
            }
        }
    }

    pub async fn scan_for_studies(&mut self) {
        let incoming_path = Path::new(&self.config.dicom_incoming_dir);
        if !incoming_path.exists() {
            return;
        }

        // Scan for study directories: {StudyDate}/{StudyInstanceUID}/
        for entry in WalkDir::new(incoming_path)
            .min_depth(2)
            .max_depth(2)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_dir() {
                let study_path = entry.path().to_path_buf();
                
                // Check if this study has DICOM files
                if self.study_has_dicom_files(&study_path).await {
                    let now = Instant::now();
                    
                    // Update the last seen time for this study
                    self.active_studies.insert(study_path.clone(), now);
                    
                    debug!(
                        event = "Updated study timestamp",
                        study_path = %study_path.display()
                    );
                }
            }
        }
    }

    pub async fn study_has_dicom_files(&self, study_path: &Path) -> bool {
        for entry in WalkDir::new(study_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                if let Some(extension) = entry.path().extension() {
                    if extension == "dcm" {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub async fn process_inactive_studies(&mut self) {
        let timeout = Duration::from_secs(self.config.study_inactivity_timeout);
        let now = Instant::now();
        let mut studies_to_process = Vec::new();

        // Find studies that haven't been updated for the timeout period
        self.active_studies.retain(|study_path, last_update| {
            if now.duration_since(*last_update) > timeout {
                studies_to_process.push(study_path.clone());
                false // Remove from active studies
            } else {
                true // Keep in active studies
            }
        });

        // Process each inactive study
        for study_path in studies_to_process {
            self.process_study_directory(&study_path).await;
        }
    }

    pub async fn process_study_directory(&self, study_path: &Path) {
        let mut dicom_files = Vec::new();

        // Collect all DICOM files in the study directory
        for entry in WalkDir::new(study_path)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            if entry.file_type().is_file() {
                if let Some(extension) = entry.path().extension() {
                    if extension == "dcm" {
                        dicom_files.push(entry.path().to_string_lossy().to_string());
                    }
                }
            }
        }

        if dicom_files.is_empty() {
            warn!(
                event = "No DICOM files found in study directory",
                study_path = %study_path.display()
            );
            return;
        }

        info!(
            event = "Processing study batch",
            study_path = %study_path.display(),
            file_count = dicom_files.len()
        );

        // Get listener config from database
        match self.database.get_listener_config_by_instance_id(&self.instance_id).await {
            Ok(Some(listener_config)) => {
                // Send association task to rules engine for batch processing
                if let Err(e) = self.rabbitmq_client.send_association_task(
                    &dicom_files,
                    &listener_config,
                    Some("UNKNOWN_CALLER"), // TODO: Get actual calling AE title from DICOM association
                ).await {
                    error!(
                        event = "Failed to send association task to RabbitMQ",
                        error = %e,
                        study_path = %study_path.display(),
                        file_count = dicom_files.len()
                    );
                } else {
                    info!(
                        event = "Successfully sent association task to rules engine",
                        study_path = %study_path.display(),
                        file_count = dicom_files.len(),
                        instance_id = %self.instance_id
                    );
                }
            }
            Ok(None) => {
                error!(
                    event = "No listener config found for instance",
                    instance_id = %self.instance_id,
                    study_path = %study_path.display()
                );
            }
            Err(e) => {
                error!(
                    event = "Database error while fetching listener config",
                    error = %e,
                    instance_id = %self.instance_id,
                    study_path = %study_path.display()
                );
            }
        }

        // Note: We don't delete the directory here - let the rules engine handle it
        // following the same pattern as the Python implementation
    }
}

/// Simple DICOM listener that stores files to the filesystem
/// This is a simplified implementation that doesn't implement the full DICOM protocol
/// Instead, it uses external tools like dcm4che storescp or pynetdicom for actual DICOM reception
pub async fn start_listener(
    bind: SocketAddr,
    aet: String,
    instance_id: String,
    config: ServiceConfig,
    database: Database,
) -> anyhow::Result<()> {
    // Create incoming directory if it doesn't exist
    tokio::fs::create_dir_all(&config.dicom_incoming_dir).await?;

    // Start directory watcher in background
    let mut directory_watcher = DirectoryWatcher::new(
        config.clone(),
        instance_id.clone(),
        database,
    ).await?;

    let watcher_handle = tokio::spawn(async move {
        directory_watcher.start_watching().await;
    });

    info!(
        event = "DICOM Listener Started",
        bind_address = %bind,
        ae_title = %aet,
        instance_id = %instance_id,
        incoming_dir = %config.dicom_incoming_dir
    );

    info!(
        event = "Rust DICOM listener started",
        bind_address = %bind,
        ae_title = %aet,
        instance_id = %instance_id,
        note = "File watching mode - external DICOM receiver required"
    );

    // Set up shutdown handler
    let shutdown_instance_id = instance_id.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        warn!("Received shutdown signal for instance: {}", shutdown_instance_id);
        std::process::exit(0);
    });

    // For now, just run the directory watcher
    // In a full implementation, we would also start a DICOM SCP listener
    // For simplicity, we'll rely on external tools (dcm4che storescp) for DICOM reception
    // and focus on the file processing and batching logic
    
    info!(
        event = "Note",
        message = "This is a file processing service. Use dcm4che storescp or similar for DICOM reception."
    );

    // Wait for the watcher
    watcher_handle.await?;

    Ok(())
}
