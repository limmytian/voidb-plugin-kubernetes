//! Kubernetes service command types.
//!
//! Commands are sent from the TUI plugin to the background service task
//! via an unbounded mpsc channel.

use tokio::sync::oneshot;

use crate::config::K8sConfig;

/// Top-level command enum for the Kubernetes service.
#[derive(Debug)]
pub enum K8sCommand {
    // --- Connection lifecycle ---
    /// Create Kubernetes client and verify connection.
    Connect {
        config: K8sConfig,
        reply: oneshot::Sender<Result<String, String>>,
    },

    /// Disconnect and shut down the background task.
    Disconnect,

    // --- Resource listing ---
    /// List resources of a given type in a namespace.
    ListResource {
        resource_type: String,
        namespace: String,
    },

    /// List namespaces.
    ListNamespaces,

    /// List available kubeconfig contexts.
    ListContexts { config: K8sConfig },

    // --- Resource detail ---
    /// Get YAML representation of a resource.
    GetYaml {
        resource_type: String,
        name: String,
        namespace: String,
    },

    // --- Resource mutations ---
    /// Delete a resource.
    DeleteResource {
        resource_type: String,
        name: String,
        namespace: String,
    },

    /// Scale a deployment/statefulset.
    ScaleResource {
        kind: String,
        name: String,
        namespace: String,
        replicas: u32,
    },

    /// Restart a deployment (rollout restart).
    RestartResource {
        kind: String,
        name: String,
        namespace: String,
    },

    /// Apply YAML content.
    ApplyYaml { yaml: String, namespace: String },

    // --- Metrics ---
    /// Fetch pod metrics for a namespace.
    FetchPodMetrics { namespace: String },

    /// Fetch node metrics.
    FetchNodeMetrics,

    // --- Streaming operations ---
    /// Start streaming pod logs. Cancels any existing log stream.
    StartLogs {
        pod: String,
        namespace: String,
        container: Option<String>,
        follow: bool,
    },

    /// Stop the current log stream.
    StopLogs,

    /// Start an exec session in a pod.
    StartExec {
        pod: String,
        namespace: String,
        container: Option<String>,
    },

    /// Send input data to the active exec session.
    ExecInput { data: Vec<u8> },

    /// Stop the active exec session.
    StopExec,

    // --- Context/namespace switching ---
    /// Switch kubeconfig context (reconnects).
    SwitchContext { context: String },
}
