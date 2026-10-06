//! Kubernetes service layer.
//!
//! Provides `K8sService`, the service facade for the Kubernetes plugin.
//! Follows the MySqlService convention with streaming lifecycle management:
//!
//! - Background tokio task processes commands asynchronously
//! - `send()` dispatches commands via unbounded mpsc channel (non-blocking)
//! - `poll_event()` drains events via `try_recv()` (non-blocking)
//! - Streaming operations (logs, exec) are managed via AbortHandles
//! - Uses the shared runtime from ShellCapabilities (no plugin-owned Runtime)
//!
//! # Dual-mode operation
//!
//! `K8sService` supports two operational modes:
//!
//! - **Channel mode** (`new()`): background task + mpsc channels for TUI use
//! - **Direct mode** (`new_direct()`): synchronous async calls for CLI use
//!
//! CLI callers use `new_direct()` to get a `kube::Client` and call direct
//! async methods without spawning any background tasks or channels.

pub mod agent_live;
pub mod commands;
pub mod events;

pub use commands::K8sCommand;
pub use events::K8sServiceEvent;

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;

use crate::config::{K8sConfig, K8sConnection};
use crate::k8s_ops;
use crate::types::K8sEvent;
use voidb_core::TabManager;

// Re-export type aliases used by direct callers.
pub use crate::types::{
    ConfigMapInfo, CronJobInfo, DaemonSetInfo, DeploymentInfo, EventInfo, IngressInfo, JobInfo,
    NodeInfo, PodInfo, PvcInfo, SecretInfo, ServiceAccountInfo, ServiceInfo, StatefulSetInfo,
};

/// Internal mode for `K8sService`.
///
/// Keeps the service polymorphic: TUI callers get Channel mode (channels +
/// background task); CLI callers get Direct mode (kube::Client, no task).
enum ServiceMode {
    /// TUI channel-based mode.
    Channel {
        cmd_tx: mpsc::UnboundedSender<K8sCommand>,
        event_rx: mpsc::UnboundedReceiver<K8sServiceEvent>,
        /// Background task handle — kept alive for the service lifetime.
        _task: tokio::task::JoinHandle<()>,
    },
    /// CLI direct mode — holds the connected client.
    Direct { client: kube::Client },
}

/// Kubernetes service facade.
///
/// Owns the command sender and event receiver channels (Channel mode) or a
/// connected `kube::Client` (Direct mode). The background task runs on the
/// shared tokio runtime via `runtime.spawn()`.
///
/// # Send + Sync
///
/// `K8sService` is `Send` but NOT `Sync` (because `UnboundedReceiver`
/// is `!Sync`). Plugin structs must wrap it in `std::sync::Mutex` to
/// satisfy `Plugin: Send + Sync`.
pub struct K8sService {
    mode: ServiceMode,
}

impl K8sService {
    /// Create a new K8sService with a background processing task (TUI mode).
    ///
    /// Uses the shared runtime from ShellCapabilities — does NOT create
    /// a plugin-owned Runtime.
    pub fn new(
        _config: K8sConfig,
        tabs: Arc<dyn TabManager>,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<K8sCommand>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<K8sServiceEvent>();

        let task = runtime.spawn(Self::background_task(cmd_rx, event_tx, tabs));

        Self {
            mode: ServiceMode::Channel {
                cmd_tx,
                event_rx,
                _task: task,
            },
        }
    }

    /// Create a service in Direct mode from an already-connected `kube::Client`.
    ///
    /// No background task is spawned. This is intended for CLI callers that
    /// call direct async methods rather than the channel-based API.
    ///
    /// # Errors
    ///
    /// Returns an error if `create_client()` fails (bad kubeconfig, network
    /// unreachable, etc.).
    pub async fn new_direct(config: &K8sConfig) -> anyhow::Result<Self> {
        let client = k8s_ops::create_client(config).await?;
        Ok(Self {
            mode: ServiceMode::Direct { client },
        })
    }

    /// Send a command to the background service task (non-blocking).
    ///
    /// Only valid in Channel mode. Calls in Direct mode are silently ignored.
    pub fn send(&self, cmd: K8sCommand) {
        if let ServiceMode::Channel { cmd_tx, .. } = &self.mode {
            let _ = cmd_tx.send(cmd);
        }
    }

    /// Poll for the next event from the service (non-blocking).
    ///
    /// Only valid in Channel mode. Returns `None` in Direct mode.
    pub fn poll_event(&mut self) -> Option<K8sServiceEvent> {
        if let ServiceMode::Channel { event_rx, .. } = &mut self.mode {
            event_rx.try_recv().ok()
        } else {
            None
        }
    }

    // -------------------------------------------------------------------------
    // Direct async methods (CLI mode)
    // -------------------------------------------------------------------------

    /// Return a reference to the underlying client.
    ///
    /// # Panics
    ///
    /// Panics if called on a Channel-mode service. Only use after `new_direct()`.
    fn direct_client(&self) -> &kube::Client {
        match &self.mode {
            ServiceMode::Direct { client } => client,
            ServiceMode::Channel { .. } => {
                panic!("direct_client() called on a Channel-mode K8sService")
            }
        }
    }

    /// Test the connection and return server version string (Direct mode).
    pub async fn test_connection_direct(&self) -> anyhow::Result<String> {
        k8s_ops::test_connection(self.direct_client()).await
    }

    /// List kubeconfig contexts (Direct mode, synchronous).
    pub fn list_contexts_direct(config: &K8sConfig) -> anyhow::Result<Vec<String>> {
        k8s_ops::list_contexts(config)
    }

    /// List all namespaces (Direct mode).
    pub async fn list_namespaces_direct(&self) -> anyhow::Result<Vec<String>> {
        k8s_ops::list_namespaces(self.direct_client()).await
    }

    /// List pods in a namespace (Direct mode).
    pub async fn list_pods_direct(&self, namespace: &str) -> anyhow::Result<Vec<PodInfo>> {
        k8s_ops::list_pods(self.direct_client(), namespace).await
    }

    /// List services in a namespace (Direct mode).
    pub async fn list_services_direct(&self, namespace: &str) -> anyhow::Result<Vec<ServiceInfo>> {
        k8s_ops::list_services(self.direct_client(), namespace).await
    }

    /// List deployments in a namespace (Direct mode).
    pub async fn list_deployments_direct(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<DeploymentInfo>> {
        k8s_ops::list_deployments(self.direct_client(), namespace).await
    }

    /// List statefulsets in a namespace (Direct mode).
    pub async fn list_statefulsets_direct(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<StatefulSetInfo>> {
        k8s_ops::list_statefulsets(self.direct_client(), namespace).await
    }

    /// List daemonsets in a namespace (Direct mode).
    pub async fn list_daemonsets_direct(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<DaemonSetInfo>> {
        k8s_ops::list_daemonsets(self.direct_client(), namespace).await
    }

    /// List jobs in a namespace (Direct mode).
    pub async fn list_jobs_direct(&self, namespace: &str) -> anyhow::Result<Vec<JobInfo>> {
        k8s_ops::list_jobs(self.direct_client(), namespace).await
    }

    /// List cronjobs in a namespace (Direct mode).
    pub async fn list_cronjobs_direct(&self, namespace: &str) -> anyhow::Result<Vec<CronJobInfo>> {
        k8s_ops::list_cronjobs(self.direct_client(), namespace).await
    }

    /// List persistent volume claims in a namespace (Direct mode).
    pub async fn list_pvcs_direct(&self, namespace: &str) -> anyhow::Result<Vec<PvcInfo>> {
        k8s_ops::list_pvcs(self.direct_client(), namespace).await
    }

    /// List ingresses in a namespace (Direct mode).
    pub async fn list_ingresses_direct(&self, namespace: &str) -> anyhow::Result<Vec<IngressInfo>> {
        k8s_ops::list_ingresses(self.direct_client(), namespace).await
    }

    /// List service accounts in a namespace (Direct mode).
    pub async fn list_service_accounts_direct(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<ServiceAccountInfo>> {
        k8s_ops::list_service_accounts(self.direct_client(), namespace).await
    }

    /// List configmaps in a namespace (Direct mode).
    pub async fn list_configmaps_direct(
        &self,
        namespace: &str,
    ) -> anyhow::Result<Vec<ConfigMapInfo>> {
        k8s_ops::list_configmaps(self.direct_client(), namespace).await
    }

    /// List secrets in a namespace (Direct mode).
    pub async fn list_secrets_direct(&self, namespace: &str) -> anyhow::Result<Vec<SecretInfo>> {
        k8s_ops::list_secrets(self.direct_client(), namespace).await
    }

    /// List nodes (Direct mode).
    pub async fn list_nodes_direct(&self) -> anyhow::Result<Vec<NodeInfo>> {
        k8s_ops::list_nodes(self.direct_client()).await
    }

    /// List events in a namespace (Direct mode).
    pub async fn list_events_direct(&self, namespace: &str) -> anyhow::Result<Vec<EventInfo>> {
        k8s_ops::list_events(self.direct_client(), namespace).await
    }

    /// Get a resource as YAML (Direct mode).
    pub async fn get_resource_yaml_direct(
        &self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<String> {
        k8s_ops::get_resource_yaml(self.direct_client(), resource_type, name, namespace).await
    }

    /// Delete a resource (Direct mode).
    pub async fn delete_resource_direct(
        &self,
        resource_type: &str,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        k8s_ops::delete_resource(self.direct_client(), resource_type, name, namespace).await
    }

    /// Scale a deployment (Direct mode).
    pub async fn scale_deployment_direct(
        &self,
        name: &str,
        namespace: &str,
        replicas: u32,
    ) -> anyhow::Result<()> {
        k8s_ops::scale_deployment(self.direct_client(), name, namespace, replicas).await
    }

    /// Restart a deployment (Direct mode).
    pub async fn restart_deployment_direct(
        &self,
        name: &str,
        namespace: &str,
    ) -> anyhow::Result<()> {
        k8s_ops::restart_deployment(self.direct_client(), name, namespace).await
    }

    /// Apply a YAML manifest (Direct mode).
    pub async fn apply_yaml_direct(&self, yaml: &str, namespace: &str) -> anyhow::Result<String> {
        k8s_ops::apply_yaml(self.direct_client(), yaml, namespace).await
    }

    /// Stream pod logs to a channel (Direct mode).
    ///
    /// Spawns a task and returns a receiver. The caller reads lines until the
    /// channel closes.
    pub async fn stream_logs_direct(
        &self,
        pod: String,
        namespace: String,
        container: Option<String>,
        follow: bool,
        tail_lines: Option<i64>,
    ) -> mpsc::UnboundedReceiver<K8sEvent> {
        let (tx, rx) = mpsc::unbounded_channel();
        let client = self.direct_client().clone();
        tokio::spawn(async move {
            k8s_ops::stream_pod_logs(client, pod, namespace, container, follow, tail_lines, tx)
                .await;
        });
        rx
    }

    // -------------------------------------------------------------------------
    // Background task (Channel mode)
    // -------------------------------------------------------------------------

    /// Background task that processes commands and manages streaming lifecycles.
    async fn background_task(
        mut cmd_rx: mpsc::UnboundedReceiver<K8sCommand>,
        event_tx: mpsc::UnboundedSender<K8sServiceEvent>,
        tabs: Arc<dyn TabManager>,
    ) {
        let mut client: Option<kube::Client> = None;
        let mut active_config: Option<K8sConfig> = None;
        let mut log_handle: Option<AbortHandle> = None;
        let mut exec_handle: Option<AbortHandle> = None;
        let mut exec_input_tx: Option<mpsc::UnboundedSender<Vec<u8>>> = None;

        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                // === Connection lifecycle ===
                K8sCommand::Connect { config, reply } => {
                    match k8s_ops::create_client(&config).await {
                        Ok(c) => {
                            match k8s_ops::test_connection(&c).await {
                                Ok(info) => {
                                    // Load namespaces on connect
                                    if let Ok(ns) = k8s_ops::list_namespaces(&c).await {
                                        let _ =
                                            event_tx.send(K8sServiceEvent::NamespacesLoaded(ns));
                                    }
                                    let _ = event_tx.send(K8sServiceEvent::Connected {
                                        server_info: info.clone(),
                                    });
                                    let _ = tabs.request_render();
                                    client = Some(c);
                                    active_config = Some(config);
                                    let _ = reply.send(Ok(info));
                                }
                                Err(e) => {
                                    let msg = e.to_string();
                                    let _ = event_tx.send(K8sServiceEvent::Error(msg.clone()));
                                    let _ = tabs.request_render();
                                    let _ = reply.send(Err(msg));
                                }
                            }
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            let _ = event_tx.send(K8sServiceEvent::Error(msg.clone()));
                            let _ = tabs.request_render();
                            let _ = reply.send(Err(msg));
                        }
                    }
                }

                K8sCommand::Disconnect => {
                    Self::cancel_streams(&mut log_handle, &mut exec_handle, &mut exec_input_tx);
                    drop(client.take());
                    break;
                }

                // === Resource listing ===
                K8sCommand::ListResource {
                    resource_type,
                    namespace,
                } => {
                    if let Some(ref c) = client {
                        Self::dispatch_list(c, &resource_type, &namespace, &event_tx, &tabs).await;
                    }
                }

                K8sCommand::ListNamespaces => {
                    if let Some(ref c) = client {
                        match k8s_ops::list_namespaces(c).await {
                            Ok(ns) => {
                                let _ = event_tx.send(K8sServiceEvent::NamespacesLoaded(ns));
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                K8sCommand::ListContexts { config } => {
                    match k8s_ops::list_contexts(&config) {
                        Ok(contexts) => {
                            let _ = event_tx.send(K8sServiceEvent::ContextsLoaded(contexts));
                        }
                        Err(e) => {
                            let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                        }
                    }
                    let _ = tabs.request_render();
                }

                // === Resource detail ===
                K8sCommand::GetYaml {
                    resource_type,
                    name,
                    namespace,
                } => {
                    if let Some(ref c) = client {
                        match k8s_ops::get_resource_yaml(c, &resource_type, &name, &namespace).await
                        {
                            Ok(yaml) => {
                                let _ = event_tx.send(K8sServiceEvent::YamlLoaded(yaml));
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                // === Mutations ===
                K8sCommand::DeleteResource {
                    resource_type,
                    name,
                    namespace,
                } => {
                    if let Some(ref c) = client {
                        match k8s_ops::delete_resource(c, &resource_type, &name, &namespace).await {
                            Ok(()) => {
                                let _ = event_tx.send(K8sServiceEvent::OperationComplete(format!(
                                    "Deleted {} {}",
                                    resource_type, name
                                )));
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                K8sCommand::ScaleResource {
                    kind: _,
                    name,
                    namespace,
                    replicas,
                } => {
                    if let Some(ref c) = client {
                        match k8s_ops::scale_deployment(c, &name, &namespace, replicas).await {
                            Ok(()) => {
                                let _ = event_tx.send(K8sServiceEvent::OperationComplete(format!(
                                    "Scaled {} to {} replicas",
                                    name, replicas
                                )));
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                K8sCommand::RestartResource {
                    kind: _,
                    name,
                    namespace,
                } => {
                    if let Some(ref c) = client {
                        match k8s_ops::restart_deployment(c, &name, &namespace).await {
                            Ok(()) => {
                                let _ = event_tx.send(K8sServiceEvent::OperationComplete(format!(
                                    "Restarted {}",
                                    name
                                )));
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                K8sCommand::ApplyYaml { yaml, namespace } => {
                    if let Some(ref c) = client {
                        match k8s_ops::apply_yaml(c, &yaml, &namespace).await {
                            Ok(msg) => {
                                let _ = event_tx.send(K8sServiceEvent::OperationComplete(msg));
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                // === Metrics ===
                K8sCommand::FetchPodMetrics { namespace } => {
                    if let Some(ref c) = client {
                        match k8s_ops::fetch_pod_metrics(c, &namespace).await {
                            Ok(metrics) => {
                                let _ = event_tx.send(K8sServiceEvent::PodMetricsLoaded(metrics));
                            }
                            Err(_) => {
                                // Metrics API may not be available; silently ignore
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                K8sCommand::FetchNodeMetrics => {
                    if let Some(ref c) = client {
                        match k8s_ops::fetch_node_metrics(c).await {
                            Ok(metrics) => {
                                let _ = event_tx.send(K8sServiceEvent::NodeMetricsLoaded(metrics));
                            }
                            Err(_) => {
                                // Metrics API may not be available; silently ignore
                            }
                        }
                        let _ = tabs.request_render();
                    }
                }

                // === Streaming: Logs ===
                K8sCommand::StartLogs {
                    pod,
                    namespace,
                    container,
                    follow,
                } => {
                    if let Some(ref c) = client {
                        if let Some(h) = log_handle.take() {
                            h.abort();
                        }
                        let c = c.clone();
                        let tx = event_tx.clone();
                        let tabs_c = tabs.clone();

                        // Bridge K8sEvent::LogLine to K8sServiceEvent::LogLine
                        let (inner_tx, mut inner_rx) = mpsc::unbounded_channel::<K8sEvent>();

                        let handle = tokio::spawn(async move {
                            // Spawn the stream
                            let stream_handle = tokio::spawn(async move {
                                k8s_ops::stream_pod_logs(
                                    c,
                                    pod,
                                    namespace,
                                    container,
                                    follow,
                                    Some(200),
                                    inner_tx,
                                )
                                .await;
                            });

                            // Forward events
                            while let Some(evt) = inner_rx.recv().await {
                                match evt {
                                    K8sEvent::LogLine { text } => {
                                        let _ = tx.send(K8sServiceEvent::LogLine { text });
                                        let _ = tabs_c.request_render();
                                    }
                                    K8sEvent::Error(e) => {
                                        let _ = tx.send(K8sServiceEvent::Error(e));
                                        let _ = tabs_c.request_render();
                                    }
                                    _ => {}
                                }
                            }

                            let _ = stream_handle.await;
                        });

                        log_handle = Some(handle.abort_handle());
                    }
                }

                K8sCommand::StopLogs => {
                    if let Some(h) = log_handle.take() {
                        h.abort();
                    }
                }

                // === Streaming: Exec ===
                K8sCommand::StartExec {
                    pod,
                    namespace,
                    container,
                } => {
                    if let Some(ref c) = client {
                        if let Some(h) = exec_handle.take() {
                            h.abort();
                        }
                        exec_input_tx = None;

                        let c = c.clone();
                        let tx = event_tx.clone();
                        let tabs_c = tabs.clone();

                        let (output_tx, mut output_rx) = mpsc::unbounded_channel::<Vec<u8>>();

                        match k8s_ops::exec_pod(c, pod, namespace, container, output_tx).await {
                            Ok(input_sender) => {
                                exec_input_tx = Some(input_sender);

                                let _ = tx.send(K8sServiceEvent::ExecStarted);
                                let _ = tabs_c.request_render();

                                let handle = tokio::spawn(async move {
                                    while let Some(bytes) = output_rx.recv().await {
                                        let _ = tx.send(K8sServiceEvent::ExecOutput(bytes));
                                        let _ = tabs_c.request_render();
                                    }
                                    let _ = tx.send(K8sServiceEvent::ExecEnded);
                                });
                                exec_handle = Some(handle.abort_handle());
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                                let _ = tabs.request_render();
                            }
                        }
                    }
                }

                K8sCommand::ExecInput { data } => {
                    if let Some(ref tx) = exec_input_tx {
                        let _ = tx.send(data);
                    }
                }

                K8sCommand::StopExec => {
                    if let Some(h) = exec_handle.take() {
                        h.abort();
                    }
                    exec_input_tx = None;
                }

                // === Context switching ===
                K8sCommand::SwitchContext { context } => {
                    // Reconnect with the new context
                    Self::cancel_streams(&mut log_handle, &mut exec_handle, &mut exec_input_tx);
                    let new_config = match active_config
                        .as_ref()
                        .ok_or_else(|| anyhow::anyhow!("Kubernetes service is not connected"))
                        .and_then(|config| config_with_context(config, context))
                    {
                        Ok(config) => config,
                        Err(error) => {
                            let _ = event_tx.send(K8sServiceEvent::Error(error.to_string()));
                            let _ = tabs.request_render();
                            continue;
                        }
                    };

                    match k8s_ops::create_client(&new_config).await {
                        Ok(c) => match k8s_ops::test_connection(&c).await {
                            Ok(info) => {
                                if let Ok(ns) = k8s_ops::list_namespaces(&c).await {
                                    let _ = event_tx.send(K8sServiceEvent::NamespacesLoaded(ns));
                                }
                                let _ =
                                    event_tx.send(K8sServiceEvent::Connected { server_info: info });
                                client = Some(c);
                                active_config = Some(new_config);
                            }
                            Err(e) => {
                                let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                            }
                        },
                        Err(e) => {
                            let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                        }
                    }
                    let _ = tabs.request_render();
                }
            }
        }

        // Cleanup on exit
        Self::cancel_streams(&mut log_handle, &mut exec_handle, &mut exec_input_tx);
    }

    /// Dispatch a resource list command to the appropriate k8s_ops function.
    async fn dispatch_list(
        client: &kube::Client,
        resource_type: &str,
        namespace: &str,
        event_tx: &mpsc::UnboundedSender<K8sServiceEvent>,
        tabs: &Arc<dyn TabManager>,
    ) {
        macro_rules! list_and_send {
            ($fn:path, $variant:ident) => {
                match $fn(client, namespace).await {
                    Ok(items) => {
                        let _ = event_tx.send(K8sServiceEvent::$variant(items));
                    }
                    Err(e) => {
                        let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                    }
                }
            };
        }

        match resource_type {
            "pods" => list_and_send!(k8s_ops::list_pods, PodsLoaded),
            "services" => list_and_send!(k8s_ops::list_services, ServicesLoaded),
            "deployments" => list_and_send!(k8s_ops::list_deployments, DeploymentsLoaded),
            "statefulsets" => list_and_send!(k8s_ops::list_statefulsets, StatefulSetsLoaded),
            "daemonsets" => list_and_send!(k8s_ops::list_daemonsets, DaemonSetsLoaded),
            "jobs" => list_and_send!(k8s_ops::list_jobs, JobsLoaded),
            "cronjobs" => list_and_send!(k8s_ops::list_cronjobs, CronJobsLoaded),
            "configmaps" => list_and_send!(k8s_ops::list_configmaps, ConfigMapsLoaded),
            "secrets" => list_and_send!(k8s_ops::list_secrets, SecretsLoaded),
            "pvcs" => list_and_send!(k8s_ops::list_pvcs, PvcsLoaded),
            "ingresses" => list_and_send!(k8s_ops::list_ingresses, IngressesLoaded),
            "serviceaccounts" => {
                list_and_send!(k8s_ops::list_service_accounts, ServiceAccountsLoaded)
            }
            "events" => list_and_send!(k8s_ops::list_events, EventsLoaded),
            "nodes" => match k8s_ops::list_nodes(client).await {
                Ok(items) => {
                    let _ = event_tx.send(K8sServiceEvent::NodesLoaded(items));
                }
                Err(e) => {
                    let _ = event_tx.send(K8sServiceEvent::Error(e.to_string()));
                }
            },
            other => {
                let _ = event_tx.send(K8sServiceEvent::Error(format!(
                    "Unknown resource type: {}",
                    other
                )));
            }
        }
        let _ = tabs.request_render();
    }

    fn cancel_streams(
        log_handle: &mut Option<AbortHandle>,
        exec_handle: &mut Option<AbortHandle>,
        exec_input_tx: &mut Option<mpsc::UnboundedSender<Vec<u8>>>,
    ) {
        if let Some(h) = log_handle.take() {
            h.abort();
        }
        if let Some(h) = exec_handle.take() {
            h.abort();
        }
        *exec_input_tx = None;
    }
}

fn config_with_context(config: &K8sConfig, context: String) -> anyhow::Result<K8sConfig> {
    let mut updated = config.clone();
    match &mut updated.connection {
        K8sConnection::Kubeconfig {
            context: selected, ..
        } => {
            *selected = Some(context);
            Ok(updated)
        }
        K8sConnection::Direct { .. } => {
            anyhow::bail!("Context switching requires a kubeconfig connection")
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}
    fn assert_send_sync<T: Send + Sync>() {}

    #[test]
    fn service_is_send() {
        assert_send::<K8sService>();
    }

    #[test]
    fn command_is_send() {
        assert_send::<K8sCommand>();
    }

    #[test]
    fn event_is_send() {
        assert_send::<K8sServiceEvent>();
    }

    #[test]
    fn mutex_service_is_send_sync() {
        assert_send_sync::<std::sync::Mutex<K8sService>>();
    }

    #[test]
    fn context_switch_preserves_kubeconfig_profile_settings() {
        let config = K8sConfig {
            connection: K8sConnection::Kubeconfig {
                path: Some("/tmp/team.kubeconfig".to_string()),
                context: Some("old".to_string()),
            },
            default_namespace: Some("operations".to_string()),
            timeout: 47,
        };

        let updated = config_with_context(&config, "new".to_string()).unwrap();
        assert_eq!(updated.default_namespace.as_deref(), Some("operations"));
        assert_eq!(updated.timeout, 47);
        match updated.connection {
            K8sConnection::Kubeconfig { path, context } => {
                assert_eq!(path.as_deref(), Some("/tmp/team.kubeconfig"));
                assert_eq!(context.as_deref(), Some("new"));
            }
            K8sConnection::Direct { .. } => panic!("expected kubeconfig connection"),
        }
    }

    #[test]
    fn context_switch_rejects_direct_connections() {
        let config = K8sConfig {
            connection: K8sConnection::Direct {
                api_url: "https://cluster.example.test".to_string(),
                auth: crate::config::K8sAuth::Token {
                    token: "test-token".to_string(),
                },
                verify_ssl: true,
                ca_cert: None,
            },
            default_namespace: None,
            timeout: 30,
        };

        assert!(config_with_context(&config, "other".to_string()).is_err());
    }
}
