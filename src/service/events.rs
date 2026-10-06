//! Kubernetes service event types.
//!
//! The service event enum wraps the existing `K8sEvent` from `types.rs`,
//! adding service-specific lifecycle events. The `ClientConnected` variant
//! is NOT re-exported because the service owns the client internally.

use crate::types::{
    ConfigMapInfo, CronJobInfo, DaemonSetInfo, DeploymentInfo, EventInfo, IngressInfo, JobInfo,
    NodeInfo, PodInfo, PvcInfo, SecretInfo, ServiceAccountInfo, ServiceInfo, StatefulSetInfo,
};

/// Service-layer event enum for the Kubernetes plugin.
///
/// This replaces `types::K8sEvent` for service consumers. The key difference
/// is that `ClientConnected` is removed (service owns the client internally).
pub enum K8sServiceEvent {
    // --- Connection lifecycle ---
    /// Connection established.
    Connected {
        server_info: String,
    },

    /// Connection closed.
    Disconnected,

    // --- Resource lists ---
    PodsLoaded(Vec<PodInfo>),
    ServicesLoaded(Vec<ServiceInfo>),
    DeploymentsLoaded(Vec<DeploymentInfo>),
    StatefulSetsLoaded(Vec<StatefulSetInfo>),
    DaemonSetsLoaded(Vec<DaemonSetInfo>),
    JobsLoaded(Vec<JobInfo>),
    ConfigMapsLoaded(Vec<ConfigMapInfo>),
    SecretsLoaded(Vec<SecretInfo>),
    NodesLoaded(Vec<NodeInfo>),
    CronJobsLoaded(Vec<CronJobInfo>),
    PvcsLoaded(Vec<PvcInfo>),
    IngressesLoaded(Vec<IngressInfo>),
    ServiceAccountsLoaded(Vec<ServiceAccountInfo>),
    EventsLoaded(Vec<EventInfo>),
    NamespacesLoaded(Vec<String>),

    // --- Detail ---
    YamlLoaded(String),

    // --- Streaming ---
    LogLine {
        text: String,
    },

    // --- Operations ---
    OperationComplete(String),

    // --- Metrics ---
    PodMetricsLoaded(Vec<(String, String, String)>),
    NodeMetricsLoaded(Vec<(String, String, String)>),

    // --- Context ---
    ContextsLoaded(Vec<String>),

    // --- Exec ---
    ExecOutput(Vec<u8>),
    ExecStarted,
    ExecEnded,

    // --- Errors ---
    Error(String),
}
