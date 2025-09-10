use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use tracing::info;

mod listener;
mod config;
mod database;
mod rabbitmq;

// Include test modules when testing
#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod listener_tests;
#[cfg(test)]
mod main_tests;

use config::ServiceConfig;
use database::Database;

#[derive(Parser)]
#[clap(name = "axiom-dicom")]
#[clap(about = "High-performance Rust DICOM service for Axiom")]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run as DICOM listener (SCP)
    Listen {
        #[clap(short, long, default_value = "0.0.0.0:11112")]
        bind: SocketAddr,
        #[clap(short, long, default_value = "AXIOM_SCP")]
        aet: String,
        #[clap(short, long)]
        instance_id: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize simple logging
    println!("Starting Axiom Rust DICOM Service");

    let cli = Cli::parse();
    let config = ServiceConfig::from_env()?;

    match cli.command {
        Commands::Listen { bind, aet, instance_id } => {
            let instance_id = instance_id.unwrap_or_else(|| {
                std::env::var("AXIOM_INSTANCE_ID").unwrap_or_else(|_| "rust_dicom_1".to_string())
            });
            
            println!("Starting DICOM listener on {} with AE title: {}", bind, aet);
            println!("Instance ID: {}", instance_id);
            
            // Start simple listener without database dependency at startup
            listener::start_simple_listener(bind, aet, instance_id, config).await?;
        }
    }

    Ok(())
}
