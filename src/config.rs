//! Kubernetes plugin configuration structures

use serde::{Deserialize, Serialize};

/// Kubernetes connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sConfig {
    /// Connection method
    pub connection: K8sConnection,
    /// Default namespace (None = "default")
    pub default_namespace: Option<String>,
    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

/// How to connect to the Kubernetes API server
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum K8sConnection {
    /// Use a kubeconfig file
    Kubeconfig {
        /// Path to kubeconfig file (None = ~/.kube/config or KUBECONFIG env)
        path: Option<String>,
        /// Context to use (None = current context)
        context: Option<String>,
    },
    /// Direct connection to API server
    Direct {
        /// API server URL, e.g. https://k8s.example.com:6443
        api_url: String,
        /// Authentication method
        auth: K8sAuth,
        /// Whether to verify TLS certificates
        #[serde(default = "default_verify_ssl")]
        verify_ssl: bool,
        /// Path to CA certificate (PEM)
        ca_cert: Option<String>,
    },
}

/// Authentication method for direct connections
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum K8sAuth {
    /// Bearer token (service account token or OIDC)
    Token { token: String },
    /// Client certificate + key (PEM paths)
    ClientCert { cert_path: String, key_path: String },
    /// In-cluster service account (mounted at /var/run/secrets/kubernetes.io/serviceaccount)
    InCluster,
}

impl Default for K8sConfig {
    fn default() -> Self {
        Self {
            connection: K8sConnection::Kubeconfig {
                path: None,
                context: None,
            },
            default_namespace: None,
            timeout: default_timeout(),
        }
    }
}

fn default_timeout() -> u64 {
    30
}

fn default_verify_ssl() -> bool {
    true
}

impl K8sConfig {
    /// Return the effective default namespace (falls back to "default")
    pub fn namespace(&self) -> &str {
        self.default_namespace.as_deref().unwrap_or("default")
    }
}
