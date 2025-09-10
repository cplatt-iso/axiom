use serde::{Deserialize, Serialize};
use anyhow::Result;
use reqwest::Client;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimseListenerConfig {
    pub id: i32,
    pub name: String,
    pub description: Option<String>,
    pub ae_title: String,
    pub port: i32,
    pub is_enabled: bool,
    pub instance_id: Option<String>,
    pub tls_enabled: bool,
    pub tls_cert_secret_name: Option<String>,
    pub tls_key_secret_name: Option<String>,
    pub tls_ca_cert_secret_name: Option<String>,
    pub listener_type: String,
}

pub struct Database {
    client: Client,
    api_base_url: String,
    api_key: String,
}

impl Database {
    pub async fn new(api_base_url: &str, api_key: &str) -> Result<Self> {
        let client = Client::new();
        Ok(Self { 
            client, 
            api_base_url: api_base_url.to_string(),
            api_key: api_key.to_string(),
        })
    }

    pub async fn get_listener_config_by_instance_id(&self, instance_id: &str) -> Result<Option<DimseListenerConfig>> {
        let url = format!("{}/api/v1/config/dimse-listeners", self.api_base_url);
        
        let response = self.client
            .get(&url)
            .header("Authorization", format!("Api-Key {}", self.api_key))
            .send()
            .await?;

        if response.status().is_success() {
            let listeners: Vec<DimseListenerConfig> = response.json().await?;
            // Find the listener with matching instance_id
            let config = listeners.into_iter()
                .find(|listener| listener.instance_id.as_ref() == Some(&instance_id.to_string()));
            Ok(config)
        } else if response.status() == 404 {
            Ok(None)
        } else {
            Err(anyhow::anyhow!("API request failed: {}", response.status()))
        }
    }

    pub async fn create_listener_config_if_not_exists(&self, instance_id: &str, ae_title: &str, port: i32) -> Result<DimseListenerConfig> {
        // First try to get existing config
        if let Some(config) = self.get_listener_config_by_instance_id(instance_id).await? {
            return Ok(config);
        }

        // Create new config via API
        let create_request = serde_json::json!({
            "name": format!("Rust DICOM Listener ({})", instance_id),
            "description": format!("Auto-created Rust DICOM listener for instance {}", instance_id),
            "ae_title": ae_title,
            "port": port,
            "is_enabled": true,
            "instance_id": instance_id,
            "tls_enabled": false,
            "listener_type": "rust"
        });

        let url = format!("{}/api/v1/config/dimse-listeners", self.api_base_url);
        
        let response = self.client
            .post(&url)
            .header("Authorization", format!("Api-Key {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&create_request)
            .send()
            .await?;

        if response.status().is_success() {
            let config: DimseListenerConfig = response.json().await?;
            Ok(config)
        } else {
            Err(anyhow::anyhow!("Failed to create listener config: {}", response.status()))
        }
    }
}
