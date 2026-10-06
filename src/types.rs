//! Shared display types for the Kubernetes plugin UI

/// Pod information extracted from Kubernetes API
#[derive(Debug, Clone, PartialEq)]
pub struct PodInfo {
    pub name: String,
    pub namespace: String,
    pub status: PodStatus,
    /// Ready containers string, e.g. "2/3"
    pub ready: String,
    pub restarts: u32,
    /// Human-readable age, e.g. "2d", "5h", "30s"
    pub age: String,
    pub node: Option<String>,
    pub containers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodStatus {
    Running,
    Pending,
    Succeeded,
    Failed,
    Terminating,
    Unknown,
    Other(String),
}

impl PodStatus {
    pub fn as_str(&self) -> &str {
        match self {
            PodStatus::Running => "Running",
            PodStatus::Pending => "Pending",
            PodStatus::Succeeded => "Succeeded",
            PodStatus::Failed => "Failed",
            PodStatus::Terminating => "Terminating",
            PodStatus::Unknown => "Unknown",
            PodStatus::Other(s) => s.as_str(),
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "Running" => PodStatus::Running,
            "Pending" => PodStatus::Pending,
            "Succeeded" => PodStatus::Succeeded,
            "Failed" => PodStatus::Failed,
            "Terminating" => PodStatus::Terminating,
            "Unknown" => PodStatus::Unknown,
            other => PodStatus::Other(other.to_string()),
        }
    }
}

/// Service information
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceInfo {
    pub name: String,
    pub namespace: String,
    /// ClusterIP, NodePort, LoadBalancer, ExternalName
    pub service_type: String,
    pub cluster_ip: String,
    /// Port strings, e.g. ["80:30080/TCP"]
    pub ports: Vec<String>,
    pub age: String,
}

/// Deployment information
#[derive(Debug, Clone, PartialEq)]
pub struct DeploymentInfo {
    pub name: String,
    pub namespace: String,
    /// "ready/desired", e.g. "3/3"
    pub ready: String,
    pub up_to_date: i64,
    pub available: i64,
    pub age: String,
}

/// Node information
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub name: String,
    pub status: String,
    pub roles: Vec<String>,
    pub version: String,
    pub age: String,
}

/// StatefulSet information
#[derive(Debug, Clone, PartialEq)]
pub struct StatefulSetInfo {
    pub name: String,
    pub namespace: String,
    /// "ready/desired", e.g. "3/3"
    pub ready: String,
    pub age: String,
}

/// DaemonSet information
#[derive(Debug, Clone, PartialEq)]
pub struct DaemonSetInfo {
    pub name: String,
    pub namespace: String,
    pub desired: i64,
    pub current: i64,
    pub ready: i64,
    pub age: String,
}

/// Job information
#[derive(Debug, Clone, PartialEq)]
pub struct JobInfo {
    pub name: String,
    pub namespace: String,
    /// "succeeded/total"
    pub completions: String,
    pub duration: String,
    pub age: String,
    pub status: JobStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobStatus {
    Complete,
    Running,
    Failed,
    Suspended,
    Unknown,
}

impl JobStatus {
    pub fn as_str(&self) -> &str {
        match self {
            JobStatus::Complete => "Complete",
            JobStatus::Running => "Running",
            JobStatus::Failed => "Failed",
            JobStatus::Suspended => "Suspended",
            JobStatus::Unknown => "Unknown",
        }
    }
}

/// CronJob information
#[derive(Debug, Clone, PartialEq)]
pub struct CronJobInfo {
    pub name: String,
    pub namespace: String,
    pub schedule: String,
    pub timezone: String,
    pub active: i32,
    pub last_schedule: String,
    pub age: String,
}

/// PersistentVolumeClaim information
#[derive(Debug, Clone, PartialEq)]
pub struct PvcInfo {
    pub name: String,
    pub namespace: String,
    pub status: String,
    pub volume: String,
    pub capacity: String,
    pub access_modes: String,
    pub storage_class: String,
    pub age: String,
}

/// Ingress information
#[derive(Debug, Clone, PartialEq)]
pub struct IngressInfo {
    pub name: String,
    pub namespace: String,
    pub class: String,
    pub hosts: Vec<String>,
    pub addresses: Vec<String>,
    pub age: String,
}

/// ServiceAccount information
#[derive(Debug, Clone, PartialEq)]
pub struct ServiceAccountInfo {
    pub name: String,
    pub namespace: String,
    pub secrets_count: usize,
    pub age: String,
}

/// Kubernetes Event information
#[derive(Debug, Clone, PartialEq)]
pub struct EventInfo {
    /// "Normal" or "Warning"
    pub event_type: String,
    pub reason: String,
    pub object: String,
    pub message: String,
    pub age: String,
    pub count: u32,
}

/// ConfigMap summary
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigMapInfo {
    pub name: String,
    pub namespace: String,
    pub data_count: usize,
    pub age: String,
}

/// Secret summary
#[derive(Debug, Clone, PartialEq)]
pub struct SecretInfo {
    pub name: String,
    pub namespace: String,
    pub secret_type: String,
    pub data_count: usize,
    pub age: String,
}

/// Events sent from background Kubernetes tasks to the UI
pub enum K8sEvent {
    /// Pod list loaded
    PodsLoaded(Vec<PodInfo>),
    /// Service list loaded
    ServicesLoaded(Vec<ServiceInfo>),
    /// Deployment list loaded
    DeploymentsLoaded(Vec<DeploymentInfo>),
    /// StatefulSet list loaded
    StatefulSetsLoaded(Vec<StatefulSetInfo>),
    /// DaemonSet list loaded
    DaemonSetsLoaded(Vec<DaemonSetInfo>),
    /// Job list loaded
    JobsLoaded(Vec<JobInfo>),
    /// ConfigMap list loaded
    ConfigMapsLoaded(Vec<ConfigMapInfo>),
    /// Secret list loaded
    SecretsLoaded(Vec<SecretInfo>),
    /// Node list loaded
    NodesLoaded(Vec<NodeInfo>),
    /// CronJob list loaded
    CronJobsLoaded(Vec<CronJobInfo>),
    /// PVC list loaded
    PvcsLoaded(Vec<PvcInfo>),
    /// Ingress list loaded
    IngressesLoaded(Vec<IngressInfo>),
    /// ServiceAccount list loaded
    ServiceAccountsLoaded(Vec<ServiceAccountInfo>),
    /// Event list loaded
    EventsLoaded(Vec<EventInfo>),
    /// Namespace list loaded
    NamespacesLoaded(Vec<String>),
    /// Resource YAML loaded
    YamlLoaded(String),
    /// Log line received
    LogLine { text: String },
    /// Operation completed with message
    OperationComplete(String),
    /// Pod metrics loaded: Vec<(pod_name, cpu, memory)>
    PodMetricsLoaded(Vec<(String, String, String)>),
    /// Node metrics loaded: Vec<(node_name, cpu, memory)>
    NodeMetricsLoaded(Vec<(String, String, String)>),
    /// Client successfully connected
    ClientConnected(kube::Client),
    /// An error occurred
    Error(String),
}
