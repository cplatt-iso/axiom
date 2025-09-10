use serde::{Deserialize, Serialize};
use serde_json;
use uuid::Uuid;
use anyhow::Result;
use tracing::{info, error};
use reqwest::Client;

use crate::database::DimseListenerConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct AssociationInfo {
    pub calling_ae_title: String,
    pub called_ae_title: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CeleryTaskMessage {
    pub task: String,
    pub id: String,
    pub args: Vec<serde_json::Value>,
    pub kwargs: serde_json::Value,
    pub retries: u32,
    pub eta: Option<String>,
    pub expires: Option<String>,
    pub utc: bool,
    pub callbacks: Option<serde_json::Value>,
    pub errbacks: Option<serde_json::Value>,
    pub chain: Option<serde_json::Value>,
}

pub struct RabbitMQClient {
    client: Client,
    api_base_url: String,
    api_key: String,
    queue_name: String,
    task_name: String,
}

impl RabbitMQClient {
    pub async fn new(api_base_url: &str, api_key: &str, queue_name: &str, task_name: &str) -> Result<Self> {
        info!("Creating RabbitMQ client via API");
        
        let client = Client::new();

        info!("RabbitMQ client ready for queue: {}", queue_name);

        Ok(Self {
            client,
            api_base_url: api_base_url.to_string(),
            api_key: api_key.to_string(),
            queue_name: queue_name.to_string(),
            task_name: task_name.to_string(),
        })
    }

    pub async fn send_dicom_file_task(
        &self,
        file_path: &str,
        listener_config: &DimseListenerConfig,
        calling_ae_title: Option<&str>,
    ) -> Result<()> {
        let task_id = Uuid::new_v4().to_string();
        
        let association_info = AssociationInfo {
            calling_ae_title: calling_ae_title.unwrap_or("UNKNOWN_CALLER").to_string(),
            called_ae_title: listener_config.ae_title.clone(),
        };

        let kwargs = serde_json::json!({
            "association_info": association_info
        });

        let task_message = CeleryTaskMessage {
            task: self.task_name.clone(),
            id: task_id.clone(),
            args: vec![
                serde_json::Value::String(file_path.to_string()),
                serde_json::Value::String("DIMSE_LISTENER".to_string()),
                serde_json::Value::String(
                    listener_config.instance_id.as_ref()
                        .unwrap_or(&"rust_dicom_1".to_string()).clone()
                ),
            ],
            kwargs,
            retries: 0,
            eta: None,
            expires: None,
            utc: true,
            callbacks: None,
            errbacks: None,
            chain: None,
        };

        info!(
            "Sending DICOM file task via API: file={}, task_id={}, instance_id={}",
            file_path,
            task_id,
            listener_config.instance_id.as_ref().unwrap_or(&"rust_dicom_1".to_string())
        );

        // Send via API endpoint instead of direct RabbitMQ
        let url = format!("{}/api/v1/tasks/submit", self.api_base_url);
        
        let response = self.client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&task_message)
            .send()
            .await?;

        if response.status().is_success() {
            info!("Successfully sent task {} via API", task_id);
        } else {
            error!("Failed to send task via API: {}", response.status());
        }

        Ok(())
    }

    pub async fn send_association_task(
        &self,
        file_paths: &[String],
        listener_config: &DimseListenerConfig,
        calling_ae_title: Option<&str>,
    ) -> Result<()> {
        let task_id = Uuid::new_v4().to_string();
        
        let association_info = AssociationInfo {
            calling_ae_title: calling_ae_title.unwrap_or("UNKNOWN_CALLER").to_string(),
            called_ae_title: listener_config.ae_title.clone(),
        };

        let kwargs = serde_json::json!({
            "association_info": association_info
        });

        // For association tasks, we send the list of files as the first argument
        let task_message = CeleryTaskMessage {
            task: "app.worker.rules_engine.process_association".to_string(), // Different task for associations
            id: task_id.clone(),
            args: vec![
                serde_json::Value::Array(
                    file_paths.iter().map(|p| serde_json::Value::String(p.clone())).collect()
                ),
                serde_json::Value::String("DIMSE_LISTENER".to_string()),
                serde_json::Value::String(
                    listener_config.instance_id.as_ref()
                        .unwrap_or(&"rust_dicom_1".to_string()).clone()
                ),
            ],
            kwargs,
            retries: 0,
            eta: None,
            expires: None,
            utc: true,
            callbacks: None,
            errbacks: None,
            chain: None,
        };

        info!(
            "Sending association task via API: files={}, task_id={}, instance_id={}",
            file_paths.len(),
            task_id,
            listener_config.instance_id.as_ref().unwrap_or(&"rust_dicom_1".to_string())
        );

        // Send via API endpoint instead of direct RabbitMQ
        let url = format!("{}/api/v1/tasks/submit", self.api_base_url);
        
        let response = self.client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&task_message)
            .send()
            .await?;

        if response.status().is_success() {
            info!("Successfully sent association task {} via API", task_id);
        } else {
            error!("Failed to send association task via API: {}", response.status());
        }

        Ok(())
    }
}
