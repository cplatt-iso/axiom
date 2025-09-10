use serde::{Deserialize, Serialize};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub rabbitmq_host: String,
    pub rabbitmq_port: u16,
    pub rabbitmq_user: String,
    pub rabbitmq_password: String,
    pub rabbitmq_queue: String,
    pub dicom_incoming_dir: String,
    pub dicom_dustbin_dir: String,
    pub api_base_url: String,
    pub api_key: String,
    pub study_inactivity_timeout: u64,
    pub heartbeat_interval: u64,
    pub database_url: String,
    pub celery_task_name: String,
}

impl ServiceConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            rabbitmq_host: env::var("RABBITMQ_HOST").unwrap_or_else(|_| "rabbitmq".to_string()),
            rabbitmq_port: env::var("RABBITMQ_PORT")
                .unwrap_or_else(|_| "5672".to_string())
                .parse()?,
            rabbitmq_user: env::var("RABBITMQ_USER").unwrap_or_else(|_| "guest".to_string()),
            rabbitmq_password: env::var("RABBITMQ_PASSWORD").unwrap_or_else(|_| "guest".to_string()),
            rabbitmq_queue: env::var("RABBITMQ_QUEUE").unwrap_or_else(|_| "rules_engine_intake".to_string()),
            dicom_incoming_dir: env::var("DICOM_INCOMING_DIR").unwrap_or_else(|_| "/dicom_data/incoming".to_string()),
            dicom_dustbin_dir: env::var("DICOM_DUSTBIN_DIR").unwrap_or_else(|_| "/dicom_data/dustbin".to_string()),
            api_base_url: env::var("API_BASE_URL").unwrap_or_else(|_| "http://api:8000".to_string()),
            api_key: env::var("API_KEY").unwrap_or_else(|_| "".to_string()),
            study_inactivity_timeout: env::var("STUDY_INACTIVITY_TIMEOUT")
                .unwrap_or_else(|_| "5".to_string())
                .parse()?,
            heartbeat_interval: env::var("HEARTBEAT_INTERVAL")
                .unwrap_or_else(|_| "30".to_string())
                .parse()?,
            database_url: env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql://postgres:password@postgres:5432/axiom".to_string()),
            celery_task_name: env::var("CELERY_TASK_NAME")
                .unwrap_or_else(|_| "app.worker.rules_engine.process_dicom_file".to_string()),
        })
    }

    pub fn rabbitmq_url(&self) -> String {
        format!(
            "amqp://{}:{}@{}:{}",
            self.rabbitmq_user,
            self.rabbitmq_password,
            self.rabbitmq_host,
            self.rabbitmq_port
        )
    }
}
