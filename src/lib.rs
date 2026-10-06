//! Kubernetes management plugin for VoidB

mod agent_session;
mod capabilities;
pub mod cli_plugin;
pub mod config;
mod k8s_ops;
pub mod service;
mod tui;
mod types;

pub use agent_session::K8sAgentSessionFactory;
pub use capabilities::{invoke_kubernetes_capability, kubernetes_capabilities};
pub use cli_plugin::create_k8s_cli_plugin;
pub use config::K8sConfig;
pub use tui::{
    K8S_AGENT_CONTEXT_STORE_DIR_ENV, LEGACY_K8S_ASSIST_STORE_DIR_ENV,
    kubernetes_agent_context_store_root,
};

/// Test a Kubernetes connection from a ConnectionConfig
pub async fn test_connection(
    conn: &voidb_core::connection::ConnectionConfig,
) -> anyhow::Result<String> {
    let config: K8sConfig = conn
        .plugin_config
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Missing plugin_config"))
        .and_then(|v| serde_json::from_value(v.clone()).map_err(Into::into))?;

    let client = k8s_ops::create_client(&config).await?;
    k8s_ops::test_connection(&client).await
}
