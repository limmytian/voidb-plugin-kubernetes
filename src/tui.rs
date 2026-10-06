use std::collections::VecDeque;
use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use chrono::Utc;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot};
use voidb_core::{
    ActorRef, ActorType, AgentContextShare, AgentContextSharePolicy, AgentContextShareStatus,
    AgentContextShareStore, AgentOperation, AgentOperationConfirmation, AgentOperationRequest,
    AgentOperationTarget, AppConfig, AssistBoundedText, AssistContextPolicy, AssistContextSnapshot,
    AssistOwnerLease, AssistPluginState, AssistSessionBinding, AssistWithheldField,
    AssistWithholdingReason, DEFAULT_ASSIST_REQUEST_TTL_SECONDS, PluginSessionHealth,
    PluginSessionPurpose, PluginSessionRegistration, PluginSessionScope, RedactionStatus, TabInfo,
    TabManager, TuiLaunchPlan, retained_tui_quality_gate,
};
#[cfg(test)]
use voidb_core::{AgentOperationRisk, AgentPrincipal, AssistPermission};

use crate::config::{K8sAuth, K8sConfig, K8sConnection};
use crate::service::{K8sCommand, K8sService, K8sServiceEvent};
use crate::types::{DeploymentInfo, EventInfo, NodeInfo, PodInfo, PodStatus, ServiceInfo};

const LOG_LINE_LIMIT: usize = 2_000;
const LOG_BYTE_LIMIT: usize = 2 * 1024 * 1024;
const EXEC_LINE_LIMIT: usize = 1_000;
const EXEC_BYTE_LIMIT: usize = 1024 * 1024;
const YAML_LINE_LIMIT: usize = 200;
const YAML_BYTE_LIMIT: usize = 128 * 1024;
const EVENT_WATCH_INTERVAL: Duration = Duration::from_secs(2);
const OPERATION_SYNC_INTERVAL: Duration = Duration::from_millis(250);
pub const K8S_AGENT_CONTEXT_STORE_DIR_ENV: &str = "VOIDB_KUBERNETES_AGENT_CONTEXT_DIR";
#[doc(hidden)]
pub const LEGACY_K8S_ASSIST_STORE_DIR_ENV: &str = "VOIDB_KUBERNETES_ASSIST_DIR";

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum K8sTuiSource {
    Profile,
    Connection,
    Fixture,
}

#[derive(Debug, Clone)]
pub struct K8sTuiLaunch {
    pub profile_label: String,
    pub config: Option<K8sConfig>,
    pub source: K8sTuiSource,
    pub fixture_path: Option<String>,
    pub purpose: String,
    pub readonly: bool,
    pub restore: bool,
    pub launch_plan: Option<TuiLaunchPlan>,
}

#[derive(Debug, Deserialize)]
struct K8sTuiFixture {
    profile_label: Option<String>,
    cluster_label: String,
    connection_kind: String,
    active_namespace: Option<String>,
    #[serde(default)]
    namespaces: Vec<String>,
    #[serde(default)]
    pods: Vec<FixturePod>,
    #[serde(default)]
    deployments: Vec<FixtureDeployment>,
    #[serde(default)]
    services: Vec<FixtureService>,
    #[serde(default)]
    events: Vec<FixtureEvent>,
    #[serde(default)]
    nodes: Vec<FixtureNode>,
    #[serde(default)]
    logs: Vec<String>,
    status: Option<String>,
    rbac_error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixturePod {
    name: String,
    namespace: String,
    status: String,
    ready: String,
    restarts: u32,
    age: String,
    node: Option<String>,
    #[serde(default)]
    containers: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureDeployment {
    name: String,
    namespace: String,
    ready: String,
    up_to_date: i64,
    available: i64,
    age: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureService {
    name: String,
    namespace: String,
    service_type: String,
    cluster_ip: String,
    #[serde(default)]
    ports: Vec<String>,
    age: String,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureEvent {
    event_type: String,
    reason: String,
    object: String,
    message: String,
    age: String,
    count: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct FixtureNode {
    name: String,
    status: String,
    #[serde(default)]
    roles: Vec<String>,
    version: String,
    age: String,
}

#[derive(Debug, Clone)]
struct K8sTuiData {
    profile_label: String,
    cluster_label: String,
    connection_kind: String,
    active_namespace: String,
    namespaces: Vec<String>,
    pods: Vec<PodView>,
    deployments: Vec<DeploymentView>,
    services: Vec<ServiceView>,
    events: Vec<EventView>,
    nodes: Vec<NodeView>,
    logs: Vec<String>,
    status: String,
    rbac_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PodView {
    name: String,
    namespace: String,
    status: String,
    ready: String,
    restarts: u32,
    age: String,
    node: Option<String>,
    containers: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DeploymentView {
    name: String,
    namespace: String,
    ready: String,
    up_to_date: i64,
    available: i64,
    age: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ServiceView {
    name: String,
    namespace: String,
    service_type: String,
    cluster_ip: String,
    ports: Vec<String>,
    age: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct EventView {
    event_type: String,
    reason: String,
    object: String,
    message: String,
    age: String,
    count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NodeView {
    name: String,
    status: String,
    roles: Vec<String>,
    version: String,
    age: String,
}

impl From<PodInfo> for PodView {
    fn from(pod: PodInfo) -> Self {
        Self {
            name: pod.name,
            namespace: pod.namespace,
            status: pod.status.as_str().to_string(),
            ready: pod.ready,
            restarts: pod.restarts,
            age: pod.age,
            node: pod.node,
            containers: pod.containers,
        }
    }
}

impl From<FixturePod> for PodView {
    fn from(pod: FixturePod) -> Self {
        Self {
            name: pod.name,
            namespace: pod.namespace,
            status: PodStatus::from_str(&pod.status).as_str().to_string(),
            ready: pod.ready,
            restarts: pod.restarts,
            age: pod.age,
            node: pod.node,
            containers: pod.containers,
        }
    }
}

impl From<DeploymentInfo> for DeploymentView {
    fn from(deployment: DeploymentInfo) -> Self {
        Self {
            name: deployment.name,
            namespace: deployment.namespace,
            ready: deployment.ready,
            up_to_date: deployment.up_to_date,
            available: deployment.available,
            age: deployment.age,
        }
    }
}

impl From<FixtureDeployment> for DeploymentView {
    fn from(deployment: FixtureDeployment) -> Self {
        Self {
            name: deployment.name,
            namespace: deployment.namespace,
            ready: deployment.ready,
            up_to_date: deployment.up_to_date,
            available: deployment.available,
            age: deployment.age,
        }
    }
}

impl From<ServiceInfo> for ServiceView {
    fn from(service: ServiceInfo) -> Self {
        Self {
            name: service.name,
            namespace: service.namespace,
            service_type: service.service_type,
            cluster_ip: service.cluster_ip,
            ports: service.ports,
            age: service.age,
        }
    }
}

impl From<FixtureService> for ServiceView {
    fn from(service: FixtureService) -> Self {
        Self {
            name: service.name,
            namespace: service.namespace,
            service_type: service.service_type,
            cluster_ip: service.cluster_ip,
            ports: service.ports,
            age: service.age,
        }
    }
}

impl From<EventInfo> for EventView {
    fn from(event: EventInfo) -> Self {
        Self {
            event_type: event.event_type,
            reason: event.reason,
            object: event.object,
            message: event.message,
            age: event.age,
            count: event.count,
        }
    }
}

impl From<FixtureEvent> for EventView {
    fn from(event: FixtureEvent) -> Self {
        Self {
            event_type: event.event_type,
            reason: event.reason,
            object: event.object,
            message: event.message,
            age: event.age,
            count: event.count,
        }
    }
}

impl From<NodeInfo> for NodeView {
    fn from(node: NodeInfo) -> Self {
        Self {
            name: node.name,
            status: node.status,
            roles: node.roles,
            version: node.version,
            age: node.age,
        }
    }
}

impl From<FixtureNode> for NodeView {
    fn from(node: FixtureNode) -> Self {
        Self {
            name: node.name,
            status: node.status,
            roles: node.roles,
            version: node.version,
            age: node.age,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceKind {
    Pods,
    Deployments,
    Services,
    Events,
    Nodes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Browser,
    Filter,
    Help,
    Error,
    Exec,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResourceItem {
    Pod(PodView),
    Deployment(DeploymentView),
    Service(ServiceView),
    Event(EventView),
    Node(NodeView),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OperationKind {
    DeleteResource,
    RestartDeployment,
    ExecPod,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OperationPlanView {
    kind: OperationKind,
    resource_type: String,
    namespace: String,
    target_name: String,
    target_label: String,
    risk: &'static str,
    confirmations_required: u8,
    confirmations: u8,
}

#[derive(Debug, Clone)]
struct BoundedLines {
    lines: VecDeque<String>,
    bytes: usize,
    dropped_lines: usize,
    max_lines: usize,
    max_bytes: usize,
}

pub fn write_k8s_tui_preflight(launch: &K8sTuiLaunch) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&preflight_value(launch))?
    );
    Ok(())
}

pub fn build_k8s_tui_evidence(launch: &K8sTuiLaunch) -> Result<Value> {
    let data = if let Some(path) = &launch.fixture_path {
        load_fixture(path)?
    } else {
        let config = launch
            .config
            .as_ref()
            .context("kubernetes tui evidence requires a profile, connection, or fixture")?;
        config_data(launch.profile_label.clone(), config)
    };

    let mut evidence = json!({
        "schema_version": 1,
        "kind": "kubernetes_tui_fixture_evidence",
        "quality_gate": retained_tui_quality_gate(
            "kubernetes",
            &["fixture-kubernetes-ops", "fixture-kind"],
            &["rbac_error", "forbidden"],
            80,
            24,
            10_000
        ),
        "preflight": preflight_value(launch),
        "transcript": {
            "profile_label": data.profile_label,
            "cluster_label": data.cluster_label,
            "connection_kind": data.connection_kind,
            "active_namespace": data.active_namespace,
            "namespaces": data.namespaces,
            "pods": data.pods.iter().map(pod_value).collect::<Vec<_>>(),
            "deployments": data.deployments.iter().map(deployment_value).collect::<Vec<_>>(),
            "services": data.services.iter().map(service_value).collect::<Vec<_>>(),
            "events": data.events.iter().map(event_value).collect::<Vec<_>>(),
            "nodes": data.nodes.iter().map(node_value).collect::<Vec<_>>(),
            "logs": {
                "line_count": data.logs.len(),
                "lines": data.logs,
                "byte_limit": LOG_BYTE_LIMIT
            },
            "rbac_error": data.rbac_error
        },
        "plan_transcript": [
            {
                "action": "delete_pod",
                "risk": "destructive",
                "first_confirmation": "summary_visible",
                "second_confirmation": "required_before_service_command",
                "readonly_behavior": "plan_visible_but_blocked"
            },
            {
                "action": "rollout_restart_deployment",
                "risk": "side_effecting",
                "confirmation": "explicit_y",
                "namespace_context_visible": true
            },
            {
                "action": "exec_pod",
                "risk": "side_effecting",
                "escape": "Ctrl+] then q",
                "stdin_capture": "redacted"
            }
        ],
        "coverage": [
            "startup",
            "namespace_browse",
            "resource_browse",
            "pod_detail",
            "workload_detail",
            "events_snapshot",
            "event_watch_polling",
            "log_stream_bounds",
            "exec_escape_prompt",
            "context_share_snapshot",
            "external_agent_operation_review",
            "operation_decision_record",
            "current_pty_rejected",
            "delete_double_confirmation",
            "restart_confirmation",
            "readonly_block",
            "rbac_error",
            "resize",
            "quit_restore",
            "secret_leak_scan"
        ],
        "external_agent_interaction": {
            "store_env": K8S_AGENT_CONTEXT_STORE_DIR_ENV,
            "broker_policy": "non_pty",
            "context_share": {
                "namespace": data.active_namespace,
                "bounded_context": true,
                "withheld_fields": [
                    "kubernetes.client_handle",
                    "kubernetes.kubeconfig",
                    "kubernetes.secret.data",
                    "service_account_token"
                ]
            },
            "operation_review": {
                "capabilities": ["kubernetes.delete", "kubernetes.restart", "kubernetes.apply", "kubernetes.scale"],
                "current_pty_allowed": false,
                "decision_record": "AgentOperationConfirmation",
                "service_execution_requires_existing_plan_confirmation": true
            }
        },
        "secret_leak_scan": null
    });

    let rendered = serde_json::to_string(&evidence)?;
    let markers = secret_leak_markers(&rendered);
    evidence["secret_leak_scan"] = json!({
        "passed": markers.is_empty(),
        "marker_count": markers.len(),
        "markers": markers
    });
    Ok(evidence)
}

pub async fn run_k8s_tui(launch: K8sTuiLaunch) -> Result<()> {
    let mut app = K8sTuiApp::new(launch)?;
    let mut terminal = ratatui::init();
    let result = run_loop(&mut terminal, &mut app);
    app.shutdown();
    ratatui::restore();
    result
}

fn preflight_value(launch: &K8sTuiLaunch) -> Value {
    let fixture_data = launch
        .fixture_path
        .as_ref()
        .and_then(|path| load_fixture(path).ok());
    let connection = launch
        .config
        .as_ref()
        .map(connection_value)
        .or_else(|| {
            fixture_data.as_ref().map(|data| {
                json!({
                    "kind": data.connection_kind,
                    "cluster_label": data.cluster_label,
                    "secret_material": "redacted"
                })
            })
        })
        .unwrap_or_else(|| {
            json!({
                "kind": "fixture",
                "cluster_label": "fixture",
                "secret_material": "redacted"
            })
        });
    let namespace = launch
        .config
        .as_ref()
        .map(|config| config.namespace().to_string())
        .or_else(|| {
            fixture_data
                .as_ref()
                .map(|data| data.active_namespace.clone())
        })
        .unwrap_or_else(|| "default".to_string());
    let launch_plan = launch.launch_plan.as_ref().map(|plan| {
        json!({
            "schema_version": plan.schema_version,
            "plugin_id": plan.plugin_id,
            "command": plan.command,
            "args": plan.args,
            "profile": plan.profile,
            "purpose": plan.purpose,
            "readonly": plan.readonly,
            "restore": plan.restore,
            "raw_input": plan.raw_input,
            "credential_ref_count": plan.credential_grant.credential_refs.len(),
            "credential_grant_id": plan.credential_grant.id,
            "redaction": plan.redaction
        })
    });

    json!({
        "ok": true,
        "command": "kubernetes tui",
        "plugin_id": "kubernetes",
        "profile_label": fixture_data
            .as_ref()
            .map(|data| data.profile_label.clone())
            .unwrap_or_else(|| launch.profile_label.clone()),
        "source": launch.source,
        "purpose": launch.purpose,
        "readonly": launch.readonly,
        "restore": launch.restore,
        "raw_input": false,
        "fixture": launch.fixture_path.is_some(),
        "namespace": namespace,
        "connection": connection,
        "privacy": {
            "diagnostics_include_kubeconfig_tokens": false,
            "diagnostics_include_service_account_tokens": false,
            "diagnostics_include_secret_data": false,
            "diagnostics_include_raw_exec_input": false
        },
        "stream_bounds": {
            "logs": { "max_lines": LOG_LINE_LIMIT, "max_bytes": LOG_BYTE_LIMIT },
            "exec": { "max_lines": EXEC_LINE_LIMIT, "max_bytes": EXEC_BYTE_LIMIT }
        },
        "service_boundary": "K8sService::Channel",
        "modes": [
            "namespaces",
            "pods",
            "deployments",
            "services",
            "events",
            "nodes",
            "details",
            "filter",
            "logs",
            "event_watch_polling",
            "operation_plan",
            "exec_escape_prompt",
            "rbac_error"
        ],
        "launch_plan": launch_plan
    })
}

fn run_loop(terminal: &mut ratatui::DefaultTerminal, app: &mut K8sTuiApp) -> Result<()> {
    terminal.draw(|frame| app.draw(frame))?;
    loop {
        let mut dirty = app.drain_service();
        app.maybe_poll_event_watch();
        dirty |= app.sync_agent_operation();
        if app.should_quit {
            return Ok(());
        }

        if event::poll(Duration::from_millis(50))? {
            match event::read()? {
                Event::Key(key) => {
                    app.handle_key(key);
                    dirty = true;
                }
                Event::Resize(cols, rows) => {
                    app.status = format!("resized kubernetes view to {cols}x{rows}");
                    dirty = true;
                }
                _ => {}
            }
        }
        if dirty {
            terminal.draw(|frame| app.draw(frame))?;
        }
    }
}

fn load_fixture(path: &str) -> Result<K8sTuiData> {
    let text = fs::read_to_string(path).with_context(|| format!("read fixture {path}"))?;
    let fixture: K8sTuiFixture =
        serde_json::from_str(&text).with_context(|| format!("parse fixture {path}"))?;
    Ok(K8sTuiData {
        profile_label: fixture
            .profile_label
            .unwrap_or_else(|| "fixture-kubernetes".to_string()),
        cluster_label: fixture.cluster_label,
        connection_kind: fixture.connection_kind,
        active_namespace: fixture
            .active_namespace
            .unwrap_or_else(|| "default".to_string()),
        namespaces: fixture.namespaces,
        pods: fixture.pods.into_iter().map(PodView::from).collect(),
        deployments: fixture
            .deployments
            .into_iter()
            .map(DeploymentView::from)
            .collect(),
        services: fixture
            .services
            .into_iter()
            .map(ServiceView::from)
            .collect(),
        events: fixture.events.into_iter().map(EventView::from).collect(),
        nodes: fixture.nodes.into_iter().map(NodeView::from).collect(),
        logs: fixture.logs,
        status: fixture
            .status
            .unwrap_or_else(|| "fixture kubernetes operations ready".to_string()),
        rbac_error: fixture.rbac_error,
    })
}

fn config_data(profile_label: String, config: &K8sConfig) -> K8sTuiData {
    let (connection_kind, cluster_label) = connection_labels(config);
    K8sTuiData {
        profile_label,
        cluster_label,
        connection_kind,
        active_namespace: config.namespace().to_string(),
        namespaces: Vec::new(),
        pods: Vec::new(),
        deployments: Vec::new(),
        services: Vec::new(),
        events: Vec::new(),
        nodes: Vec::new(),
        logs: Vec::new(),
        status: "connecting through K8sService channel mode".to_string(),
        rbac_error: None,
    }
}

struct K8sTuiApp {
    profile_label: String,
    cluster_label: String,
    connection_kind: String,
    source: K8sTuiSource,
    purpose: String,
    readonly: bool,
    restore: bool,
    active: ResourceKind,
    active_namespace: String,
    namespaces: Vec<String>,
    pods: Vec<PodView>,
    deployments: Vec<DeploymentView>,
    services: Vec<ServiceView>,
    events: Vec<EventView>,
    nodes: Vec<NodeView>,
    selected: usize,
    filter: String,
    service: Option<K8sService>,
    operation_plan: Option<OperationPlanView>,
    status: String,
    mode: Mode,
    return_mode: Mode,
    logs: BoundedLines,
    exec_output: BoundedLines,
    yaml_preview: BoundedLines,
    active_log_pod: Option<String>,
    active_event_watch: bool,
    last_watch_poll: Instant,
    exec_target: Option<String>,
    exec_escape_armed: bool,
    context_share_store: AgentContextShareStore,
    context_share: Option<AgentContextShare>,
    context_share_owner_lease: Option<AssistOwnerLease>,
    operation_request: Option<AgentOperationRequest>,
    operation_request_seen_id: Option<String>,
    operation_confirmation: Option<AgentOperationConfirmation>,
    context_share_sequence: u64,
    last_operation_sync: Instant,
    should_quit: bool,
    render_quit: Arc<AtomicBool>,
}

impl K8sTuiApp {
    fn new(launch: K8sTuiLaunch) -> Result<Self> {
        let data = if let Some(path) = &launch.fixture_path {
            load_fixture(path)?
        } else {
            let config = launch
                .config
                .as_ref()
                .context("kubernetes tui requires a profile, connection, or fixture")?;
            config_data(launch.profile_label.clone(), config)
        };

        let render_quit = Arc::new(AtomicBool::new(false));
        let service = if launch.fixture_path.is_none() {
            let config = launch
                .config
                .clone()
                .context("kubernetes tui requires config outside fixture mode")?;
            let tabs = Arc::new(StandaloneK8sTabManager::new(render_quit.clone()));
            let runtime = tokio::runtime::Handle::current();
            let service = K8sService::new(config.clone(), tabs, runtime);
            let (reply, _reply_rx) = oneshot::channel();
            service.send(K8sCommand::Connect { config, reply });
            Some(service)
        } else {
            None
        };

        let mut logs = BoundedLines::new(LOG_LINE_LIMIT, LOG_BYTE_LIMIT);
        for line in &data.logs {
            logs.push_line(line.clone());
        }
        let status = data
            .rbac_error
            .as_deref()
            .map(|error| format!("{} | {}", data.status, safe_error_summary(error)))
            .unwrap_or_else(|| data.status.clone());

        Ok(Self {
            profile_label: data.profile_label,
            cluster_label: data.cluster_label,
            connection_kind: data.connection_kind,
            source: launch.source,
            purpose: launch.purpose,
            readonly: launch.readonly,
            restore: launch.restore,
            active: ResourceKind::Pods,
            active_namespace: data.active_namespace,
            namespaces: data.namespaces,
            pods: data.pods,
            deployments: data.deployments,
            services: data.services,
            events: data.events,
            nodes: data.nodes,
            selected: 0,
            filter: String::new(),
            service,
            operation_plan: None,
            status,
            mode: Mode::Browser,
            return_mode: Mode::Browser,
            logs,
            exec_output: BoundedLines::new(EXEC_LINE_LIMIT, EXEC_BYTE_LIMIT),
            yaml_preview: BoundedLines::new(YAML_LINE_LIMIT, YAML_BYTE_LIMIT),
            active_log_pod: None,
            active_event_watch: false,
            last_watch_poll: Instant::now(),
            exec_target: None,
            exec_escape_armed: false,
            context_share_store: k8s_context_share_store()?,
            context_share: None,
            context_share_owner_lease: None,
            operation_request: None,
            operation_request_seen_id: None,
            operation_confirmation: None,
            context_share_sequence: 0,
            last_operation_sync: Instant::now() - OPERATION_SYNC_INTERVAL,
            should_quit: false,
            render_quit,
        })
    }

    fn shutdown(&mut self) {
        self.cancel_context_share_on_shutdown();
        if let Some(service) = &self.service {
            service.send(K8sCommand::StopLogs);
            service.send(K8sCommand::StopExec);
            service.send(K8sCommand::Disconnect);
        }
    }

    fn cancel_context_share_on_shutdown(&mut self) {
        let Some(share_id) = self.context_share.as_ref().map(|share| share.id.clone()) else {
            return;
        };
        let now = Utc::now();
        let _ = self.context_share_store.update_plugin_state(
            &share_id,
            AssistPluginState {
                mode: mode_label(self.mode).to_string(),
                health: PluginSessionHealth::Closed,
                status: "Kubernetes TUI owner closed".to_string(),
                updated_at: now,
                metadata: json!({}),
                redaction: RedactionStatus::NotRequired,
            },
        );
        let _ = self.context_share_store.cancel(&share_id);
        self.operation_request = None;
        if self.operation_confirmation.take().is_some() {
            self.operation_plan = None;
        }
        self.context_share_owner_lease = None;
    }

    fn drain_service(&mut self) -> bool {
        let mut changed = false;
        if self.render_quit.load(Ordering::SeqCst) {
            changed |= !self.should_quit;
            self.should_quit = true;
        }
        let Some(mut service) = self.service.take() else {
            return changed;
        };
        while let Some(event) = service.poll_event() {
            changed = true;
            self.handle_service_event(event);
        }
        self.service = Some(service);
        changed
    }

    fn handle_service_event(&mut self, event: K8sServiceEvent) {
        match event {
            K8sServiceEvent::Connected { server_info } => {
                self.status = format!("connected: {}", safe_error_summary(&server_info));
                self.refresh_all();
            }
            K8sServiceEvent::Disconnected => {
                self.status = "disconnected".to_string();
            }
            K8sServiceEvent::NamespacesLoaded(namespaces) => {
                if !namespaces.is_empty() && !namespaces.contains(&self.active_namespace) {
                    self.active_namespace = namespaces[0].clone();
                }
                self.namespaces = namespaces;
                self.status = format!("loaded {} namespaces", self.namespaces.len());
            }
            K8sServiceEvent::PodsLoaded(pods) => {
                self.pods = pods.into_iter().map(PodView::from).collect();
                self.pods.sort_by(pod_sort);
                self.trim_selection();
                self.status = format!("listed {} pods", self.pods.len());
            }
            K8sServiceEvent::DeploymentsLoaded(deployments) => {
                self.deployments = deployments.into_iter().map(DeploymentView::from).collect();
                self.deployments
                    .sort_by(|left, right| left.name.cmp(&right.name));
                self.trim_selection();
                self.status = format!("listed {} deployments", self.deployments.len());
            }
            K8sServiceEvent::ServicesLoaded(services) => {
                self.services = services.into_iter().map(ServiceView::from).collect();
                self.services
                    .sort_by(|left, right| left.name.cmp(&right.name));
                self.trim_selection();
                self.status = format!("listed {} services", self.services.len());
            }
            K8sServiceEvent::EventsLoaded(events) => {
                self.events = events.into_iter().map(EventView::from).collect();
                self.status = format!("listed {} events", self.events.len());
            }
            K8sServiceEvent::NodesLoaded(nodes) => {
                self.nodes = nodes.into_iter().map(NodeView::from).collect();
                self.nodes.sort_by(|left, right| left.name.cmp(&right.name));
                self.trim_selection();
                self.status = format!("listed {} nodes", self.nodes.len());
            }
            K8sServiceEvent::YamlLoaded(yaml) => {
                self.yaml_preview.clear();
                self.yaml_preview.push_text(&redact_yaml_preview(&yaml));
                self.status = "resource YAML preview loaded with secret data redacted".to_string();
            }
            K8sServiceEvent::LogLine { text } => {
                self.logs.push_line(text);
                self.status = format!(
                    "log stream updated; retained {} lines, dropped {}",
                    self.logs.len(),
                    self.logs.dropped_lines
                );
            }
            K8sServiceEvent::OperationComplete(message) => {
                self.operation_plan = None;
                self.status = safe_error_summary(&message);
                self.refresh_current();
            }
            K8sServiceEvent::ExecStarted => {
                self.mode = Mode::Exec;
                self.exec_escape_armed = false;
                self.status = "exec attached; escape with Ctrl+] then q".to_string();
            }
            K8sServiceEvent::ExecOutput(bytes) => {
                self.exec_output.push_text(&String::from_utf8_lossy(&bytes));
            }
            K8sServiceEvent::ExecEnded => {
                self.mode = Mode::Browser;
                self.exec_target = None;
                self.status = "exec session ended".to_string();
            }
            K8sServiceEvent::PodMetricsLoaded(metrics) => {
                self.status = format!("pod metrics loaded for {} pods", metrics.len());
            }
            K8sServiceEvent::NodeMetricsLoaded(metrics) => {
                self.status = format!("node metrics loaded for {} nodes", metrics.len());
            }
            K8sServiceEvent::ContextsLoaded(contexts) => {
                self.status = format!("loaded {} kubeconfig contexts", contexts.len());
            }
            K8sServiceEvent::Error(message) => {
                self.mode = Mode::Error;
                self.status = safe_error_summary(&message);
            }
            K8sServiceEvent::StatefulSetsLoaded(_)
            | K8sServiceEvent::DaemonSetsLoaded(_)
            | K8sServiceEvent::JobsLoaded(_)
            | K8sServiceEvent::ConfigMapsLoaded(_)
            | K8sServiceEvent::SecretsLoaded(_)
            | K8sServiceEvent::CronJobsLoaded(_)
            | K8sServiceEvent::PvcsLoaded(_)
            | K8sServiceEvent::IngressesLoaded(_)
            | K8sServiceEvent::ServiceAccountsLoaded(_) => {
                self.status = "resource type loaded outside current TUI view".to_string();
            }
        }
    }

    fn handle_key(&mut self, key: KeyEvent) {
        if self.mode == Mode::Exec {
            self.handle_exec_key(key);
            return;
        }
        if self.mode == Mode::Help {
            self.mode = self.return_mode;
            self.status = format!("returned to {}", mode_label(self.mode));
            return;
        }
        if self.mode == Mode::Error {
            self.mode = Mode::Browser;
            self.status = "returned to kubernetes browser".to_string();
            return;
        }
        if self.mode == Mode::Filter {
            self.handle_filter_key(key);
            return;
        }

        match key.code {
            KeyCode::Char('q') => self.should_quit = true,
            KeyCode::Tab => self.next_resource(),
            KeyCode::BackTab => self.previous_resource(),
            KeyCode::Up | KeyCode::Char('k') => self.move_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => self.move_selection(1),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => {
                self.selected = self.filtered_items().len().saturating_sub(1);
            }
            KeyCode::Enter | KeyCode::Char('i') => self.inspect_selected(),
            KeyCode::Char('/') => {
                self.return_mode = self.mode;
                self.mode = Mode::Filter;
                self.status = "filter: type text, Enter apply, Esc clear".to_string();
            }
            KeyCode::Char('n') => self.next_namespace(),
            KeyCode::Char('N') => self.previous_namespace(),
            KeyCode::Char('r') => self.refresh_current(),
            KeyCode::Char('l') => self.start_logs(),
            KeyCode::Char('w') => self.toggle_event_watch(),
            KeyCode::Char('c') => self.cancel_streams(),
            KeyCode::Char('e') => self.plan_exec(),
            KeyCode::Char('x') | KeyCode::Delete => self.plan_delete(),
            KeyCode::Char('R') => self.plan_restart(),
            KeyCode::Char('a') => self.share_agent_context(),
            KeyCode::Char('y')
                if self.operation_request.is_some() && self.operation_plan.is_none() =>
            {
                self.stage_agent_operation()
            }
            KeyCode::Char('d')
                if self.operation_request.is_some() && self.operation_plan.is_none() =>
            {
                self.deny_agent_operation()
            }
            KeyCode::Char('y') => self.confirm_plan(),
            KeyCode::Esc => self.cancel_plan(),
            KeyCode::Char('?') => self.show_help(),
            _ => {
                self.status =
                    "k8s: Tab resources, n/N namespace, j/k move, i inspect, l logs, w events, a share context, y/d review agent operation, e exec, x/R plan, q quit"
                        .to_string();
            }
        }
    }

    fn handle_filter_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Enter => {
                self.mode = self.return_mode;
                self.selected = 0;
                self.status = format!("filter applied: {}", self.filter_label());
            }
            KeyCode::Esc => {
                self.filter.clear();
                self.mode = self.return_mode;
                self.selected = 0;
                self.status = "filter cleared".to_string();
            }
            KeyCode::Backspace => {
                self.filter.pop();
                self.selected = 0;
            }
            KeyCode::Char(ch) => {
                self.filter.push(ch);
                self.selected = 0;
            }
            _ => {}
        }
    }

    fn handle_exec_key(&mut self, key: KeyEvent) {
        if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char(']') {
            self.exec_escape_armed = true;
            self.status =
                "exec escape armed; press q to close or any other key to continue".to_string();
            return;
        }

        if self.exec_escape_armed {
            if key.code == KeyCode::Char('q') {
                if let Some(service) = &self.service {
                    service.send(K8sCommand::StopExec);
                }
                self.mode = Mode::Browser;
                self.exec_target = None;
                self.exec_escape_armed = false;
                self.status = "exec close requested".to_string();
                return;
            }
            self.exec_escape_armed = false;
        }

        let Some(bytes) = exec_key_bytes(key) else {
            return;
        };
        if let Some(service) = &self.service {
            service.send(K8sCommand::ExecInput { data: bytes });
        } else {
            self.exec_output
                .push_line("fixture exec input redacted".to_string());
        }
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.filtered_items().len();
        if len == 0 {
            self.selected = 0;
            return;
        }
        let last = len as isize - 1;
        self.selected = (self.selected as isize + delta).clamp(0, last) as usize;
    }

    fn next_resource(&mut self) {
        self.active = match self.active {
            ResourceKind::Pods => ResourceKind::Deployments,
            ResourceKind::Deployments => ResourceKind::Services,
            ResourceKind::Services => ResourceKind::Events,
            ResourceKind::Events => ResourceKind::Nodes,
            ResourceKind::Nodes => ResourceKind::Pods,
        };
        self.selected = 0;
        self.status = format!("resource view: {}", self.active.label());
    }

    fn previous_resource(&mut self) {
        self.active = match self.active {
            ResourceKind::Pods => ResourceKind::Nodes,
            ResourceKind::Deployments => ResourceKind::Pods,
            ResourceKind::Services => ResourceKind::Deployments,
            ResourceKind::Events => ResourceKind::Services,
            ResourceKind::Nodes => ResourceKind::Events,
        };
        self.selected = 0;
        self.status = format!("resource view: {}", self.active.label());
    }

    fn next_namespace(&mut self) {
        if self.namespaces.is_empty() {
            self.status = "no namespaces loaded".to_string();
            return;
        }
        let idx = self
            .namespaces
            .iter()
            .position(|ns| ns == &self.active_namespace)
            .unwrap_or(0);
        self.active_namespace = self.namespaces[(idx + 1) % self.namespaces.len()].clone();
        self.selected = 0;
        self.refresh_all();
    }

    fn previous_namespace(&mut self) {
        if self.namespaces.is_empty() {
            self.status = "no namespaces loaded".to_string();
            return;
        }
        let idx = self
            .namespaces
            .iter()
            .position(|ns| ns == &self.active_namespace)
            .unwrap_or(0);
        let next = if idx == 0 {
            self.namespaces.len() - 1
        } else {
            idx - 1
        };
        self.active_namespace = self.namespaces[next].clone();
        self.selected = 0;
        self.refresh_all();
    }

    fn refresh_all(&mut self) {
        if let Some(service) = &self.service {
            service.send(K8sCommand::ListNamespaces);
            for resource_type in ["pods", "deployments", "services", "events", "nodes"] {
                service.send(K8sCommand::ListResource {
                    resource_type: resource_type.to_string(),
                    namespace: self.active_namespace.clone(),
                });
            }
            self.status = format!("refreshing namespace {}", self.active_namespace);
        } else {
            self.status = format!("fixture refresh: namespace {}", self.active_namespace);
        }
    }

    fn refresh_current(&mut self) {
        if let Some(service) = &self.service {
            service.send(K8sCommand::ListResource {
                resource_type: self.active.resource_type().to_string(),
                namespace: self.active_namespace.clone(),
            });
            self.status = format!("refreshing {}", self.active.label());
        } else {
            self.status = format!("fixture refresh: {}", self.active.label());
        }
    }

    fn maybe_poll_event_watch(&mut self) {
        if !self.active_event_watch || self.last_watch_poll.elapsed() < EVENT_WATCH_INTERVAL {
            return;
        }
        self.last_watch_poll = Instant::now();
        if let Some(service) = &self.service {
            service.send(K8sCommand::ListResource {
                resource_type: "events".to_string(),
                namespace: self.active_namespace.clone(),
            });
        }
    }

    fn inspect_selected(&mut self) {
        let Some(item) = self.selected_item() else {
            self.status = "nothing selected".to_string();
            return;
        };
        if let Some(service) = &self.service {
            service.send(K8sCommand::GetYaml {
                resource_type: item.resource_type().to_string(),
                name: item.name(),
                namespace: item
                    .namespace()
                    .unwrap_or_else(|| self.active_namespace.clone()),
            });
            self.status = format!("loading YAML preview for {}", item.safe_label());
        } else {
            self.yaml_preview.clear();
            self.yaml_preview
                .push_line(format!("kind: {}", item.resource_type()));
            self.yaml_preview
                .push_line(format!("metadata.name: {}", item.name()));
            self.status = format!("fixture inspect: {}", item.safe_label());
        }
    }

    fn start_logs(&mut self) {
        let Some(pod) = self.selected_pod() else {
            self.status = "select a pod to follow logs".to_string();
            return;
        };
        let container = pod.containers.first().cloned();
        if let Some(service) = &self.service {
            service.send(K8sCommand::StopLogs);
            service.send(K8sCommand::StartLogs {
                pod: pod.name.clone(),
                namespace: pod.namespace.clone(),
                container,
                follow: true,
            });
            self.logs.clear();
            self.active_log_pod = Some(pod.name.clone());
            self.status = format!("following logs for {}; cancel with c", pod.name);
        } else {
            self.active_log_pod = Some(pod.name.clone());
            self.status = format!("fixture logs shown for {}", pod.name);
        }
    }

    fn toggle_event_watch(&mut self) {
        self.active_event_watch = !self.active_event_watch;
        self.last_watch_poll = Instant::now() - EVENT_WATCH_INTERVAL;
        if self.active_event_watch {
            self.status = "event watch polling enabled; cancel with c".to_string();
            self.maybe_poll_event_watch();
        } else {
            self.status = "event watch polling stopped".to_string();
        }
    }

    fn cancel_streams(&mut self) {
        if let Some(service) = &self.service {
            service.send(K8sCommand::StopLogs);
            service.send(K8sCommand::StopExec);
        }
        self.active_log_pod = None;
        self.active_event_watch = false;
        self.status = "stream/watch cancellation requested".to_string();
    }

    fn share_agent_context(&mut self) {
        let policy = AssistContextPolicy::default();
        let snapshot = match self.build_context_share_snapshot(&policy) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.status = format!("context snapshot failed: {error}");
                return;
            }
        };
        self.context_share_sequence = self.context_share_sequence.saturating_add(1);
        let now = Utc::now();
        let share_id = format!(
            "context:kubernetes:{}:{}",
            now.timestamp_millis(),
            self.context_share_sequence
        );
        let mut share = match AgentContextShare::new_context_share(
            share_id,
            format!(
                "Kubernetes {} current-view context in namespace {}",
                self.active.label(),
                self.active_namespace
            ),
            snapshot.binding.clone(),
            ActorRef {
                id: "kubernetes-tui".to_string(),
                actor_type: ActorType::Human,
            },
            None,
            policy,
            now,
            now + chrono::Duration::seconds(DEFAULT_ASSIST_REQUEST_TTL_SECONDS),
        ) {
            Ok(share) => share,
            Err(error) => {
                self.status = format!("context share failed: {error}");
                return;
            }
        };
        share.preview = Some(snapshot.preview());
        if let Err(error) = share.transition_to(AgentContextShareStatus::Pending) {
            self.status = format!("context share failed: {error}");
            return;
        }
        let plugin_state = AssistPluginState {
            mode: mode_label(self.mode).to_string(),
            health: PluginSessionHealth::Ready,
            status: safe_error_summary(&self.status),
            updated_at: now,
            metadata: self.context_share_metadata(),
            redaction: RedactionStatus::Applied,
        };
        match self.context_share_store.share_with_owner_lease(
            share.clone(),
            snapshot,
            Some(plugin_state),
        ) {
            Ok((record, owner_lease)) => {
                if let Some(previous_id) = self
                    .context_share
                    .as_ref()
                    .map(|previous| previous.id.clone())
                {
                    let _ = self.context_share_store.cancel(&previous_id);
                }
                self.context_share_owner_lease = Some(owner_lease);
                if self.operation_confirmation.take().is_some() {
                    self.operation_plan = None;
                }
                self.context_share = Some(record.request);
                self.operation_request = None;
                self.operation_request_seen_id = None;
                self.last_operation_sync = Instant::now() - OPERATION_SYNC_INTERVAL;
                self.status = format!("Kubernetes context shared: {}", share.id);
            }
            Err(error) => {
                self.status = format!("context share write failed: {error}");
            }
        }
    }

    fn sync_agent_operation(&mut self) -> bool {
        if self.last_operation_sync.elapsed() < OPERATION_SYNC_INTERVAL {
            return false;
        }
        self.last_operation_sync = Instant::now();
        let Some(share_id) = self.context_share.as_ref().map(|share| share.id.clone()) else {
            return false;
        };
        let detail = match self.context_share_store.detail(&share_id) {
            Ok(detail) => detail,
            Err(_) => {
                if self.operation_request.is_some() {
                    self.operation_request = None;
                    self.status = "agent operation synchronization unavailable".to_string();
                    return true;
                }
                return false;
            }
        };
        self.context_share = Some(detail.record.request.clone());
        if detail.record.request.status.is_terminal() {
            let changed = self.operation_request.take().is_some()
                || self.operation_confirmation.take().is_some();
            if changed {
                self.operation_plan = None;
                self.status = "context share ended; pending agent operation cleared".to_string();
            }
            return changed;
        }
        let Some(operation) = detail.record.latest_pending_operation_request().cloned() else {
            if self.operation_request.take().is_some() {
                self.status = "agent operation decision synchronized".to_string();
                return true;
            }
            return false;
        };
        if operation.actions.len() != 1 {
            if self.operation_request_seen_id.as_deref() == Some(operation.id.as_str()) {
                return false;
            }
            self.operation_request_seen_id = Some(operation.id);
            self.operation_request = None;
            self.status = "agent operation rejected: non-PTY review requires exactly one operation"
                .to_string();
            return true;
        }
        if self.operation_request_seen_id.as_deref() == Some(operation.id.as_str()) {
            return false;
        }
        self.operation_request_seen_id = Some(operation.id.clone());
        self.status = format!(
            "agent operation ready for y/n review: {}",
            operation.summary
        );
        self.operation_request = Some(operation);
        true
    }

    fn stage_agent_operation(&mut self) {
        let Some(operation_request) = self.operation_request.clone() else {
            self.status = "no agent operation to stage".to_string();
            return;
        };
        let operation = match operation_request.actions.as_slice() {
            [operation] => operation,
            _ => {
                self.status =
                    "agent operation rejected: expected exactly one operation".to_string();
                return;
            }
        };
        if let Err(error) = self.stage_agent_operation_action(operation) {
            self.status = format!("agent operation rejected: {error}");
            return;
        }
        let confirmation = match self.operation_confirmation(
            "staged_for_plan_review",
            "staged existing Kubernetes operation plan; service command still requires plan confirmation",
        ) {
            Ok(confirmation) => confirmation,
            Err(error) => {
                self.operation_plan = None;
                self.status = format!("operation review failed: {error}");
                return;
            }
        };
        let share_id = confirmation.request_id.clone();
        match self
            .context_share_store
            .confirm_operation(&share_id, confirmation.clone())
        {
            Ok(_) => {
                self.operation_confirmation = Some(confirmation);
                self.operation_request = None;
                self.status = "agent operation staged; review the Kubernetes plan".to_string();
            }
            Err(error) => {
                self.operation_plan = None;
                self.status = format!("operation decision write failed: {error}");
            }
        }
    }

    fn deny_agent_operation(&mut self) {
        let confirmation = match self.operation_confirmation(
            "denied_by_user",
            "operator denied the requested operation before plan staging",
        ) {
            Ok(confirmation) => confirmation,
            Err(error) => {
                self.status = format!("operation denial failed: {error}");
                return;
            }
        };
        let share_id = confirmation.request_id.clone();
        match self
            .context_share_store
            .confirm_operation(&share_id, confirmation.clone())
        {
            Ok(_) => {
                self.operation_confirmation = Some(confirmation);
                self.operation_request = None;
                self.status = "agent operation denied; no Kubernetes plan was staged".to_string();
            }
            Err(error) => {
                self.status = format!("operation denial write failed: {error}");
            }
        }
    }

    fn operation_confirmation(
        &self,
        status: &str,
        note: &str,
    ) -> Result<AgentOperationConfirmation> {
        let share = self
            .context_share
            .as_ref()
            .context("no active context share")?;
        let operation_request = self
            .operation_request
            .as_ref()
            .context("no agent operation request")?;
        let operation = match operation_request.actions.as_slice() {
            [operation] => operation,
            _ => {
                return Err(anyhow!(
                    "agent operation request must contain exactly one operation"
                ));
            }
        };
        Ok(AgentOperationConfirmation {
            request_id: share.id.clone(),
            response_id: operation_request.id.clone(),
            action_index: 0,
            target: operation_target_label(operation),
            uses_current_pty: false,
            generation: share.binding.generation,
            confirmed_at: Utc::now(),
            expires_at: Some(share.expires_at),
            command_summary: operation_summary(operation),
            capability_id: operation_capability_id(operation),
            status: status.to_string(),
            note: note.to_string(),
            redaction: RedactionStatus::Applied,
        })
    }

    fn stage_agent_operation_action(&mut self, action: &AgentOperation) -> Result<()> {
        let AgentOperation::CapabilityCall {
            capability_id,
            input_summary,
            ..
        } = action
        else {
            return Err(anyhow!("agent operation is guidance only"));
        };
        let action_name = input_summary
            .get("action")
            .and_then(Value::as_str)
            .context("Kubernetes operation requires action")?;
        let resource_type = input_summary
            .get("resource_type")
            .and_then(Value::as_str)
            .unwrap_or("pod")
            .to_string();
        let namespace = input_summary
            .get("namespace")
            .and_then(Value::as_str)
            .unwrap_or(&self.active_namespace)
            .to_string();
        let target_name = input_summary
            .get("target_name")
            .and_then(Value::as_str)
            .context("Kubernetes operation requires target_name")?
            .to_string();
        let target_label = format!("{namespace}/{target_name}");
        match (capability_id.as_str(), action_name) {
            ("kubernetes.delete", "delete") => {
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::DeleteResource,
                    resource_type,
                    namespace,
                    target_name,
                    target_label,
                    risk: "destructive",
                    confirmations_required: 2,
                    confirmations: 0,
                });
            }
            ("kubernetes.restart", "restart") => {
                self.operation_plan = Some(OperationPlanView {
                    kind: OperationKind::RestartDeployment,
                    resource_type: "deployment".to_string(),
                    namespace,
                    target_name,
                    target_label,
                    risk: "side_effecting",
                    confirmations_required: 1,
                    confirmations: 0,
                });
            }
            _ => {
                return Err(anyhow!(
                    "unsupported Kubernetes operation {capability_id}/{action_name}"
                ));
            }
        }
        Ok(())
    }

    fn plan_exec(&mut self) {
        let Some(pod) = self.selected_pod() else {
            self.status = "select a pod for exec".to_string();
            return;
        };
        let label = format!("{}/{}", pod.namespace, pod.name);
        self.operation_plan = Some(OperationPlanView {
            kind: OperationKind::ExecPod,
            resource_type: "pod".to_string(),
            namespace: pod.namespace,
            target_name: pod.name,
            target_label: label.clone(),
            risk: "side_effecting",
            confirmations_required: 1,
            confirmations: 0,
        });
        self.status = format!("exec plan staged for {label}; press y");
    }

    fn plan_delete(&mut self) {
        let Some(item) = self.selected_item() else {
            self.status = "nothing selected".to_string();
            return;
        };
        if matches!(item, ResourceItem::Event(_)) {
            self.status = "events are not delete targets".to_string();
            return;
        }
        if matches!(item, ResourceItem::Node(_)) {
            self.status = "node delete is not exposed by this TUI".to_string();
            return;
        }
        let label = item.safe_label();
        self.operation_plan = Some(OperationPlanView {
            kind: OperationKind::DeleteResource,
            resource_type: item.resource_type().to_string(),
            namespace: item
                .namespace()
                .unwrap_or_else(|| self.active_namespace.clone()),
            target_name: item.name(),
            target_label: label.clone(),
            risk: "destructive",
            confirmations_required: 2,
            confirmations: 0,
        });
        self.status = format!("delete plan staged for {label}; press y twice");
    }

    fn plan_restart(&mut self) {
        let Some(ResourceItem::Deployment(deployment)) = self.selected_item() else {
            self.status = "select a deployment for rollout restart".to_string();
            return;
        };
        let label = format!("{}/{}", deployment.namespace, deployment.name);
        self.operation_plan = Some(OperationPlanView {
            kind: OperationKind::RestartDeployment,
            resource_type: "deployment".to_string(),
            namespace: deployment.namespace,
            target_name: deployment.name,
            target_label: label.clone(),
            risk: "side_effecting",
            confirmations_required: 1,
            confirmations: 0,
        });
        self.status = format!("restart plan staged for {label}; press y");
    }

    fn confirm_plan(&mut self) {
        if !self.agent_operation_plan_is_current() {
            return;
        }
        let Some(mut plan) = self.operation_plan.take() else {
            self.status = "no operation plan to confirm".to_string();
            return;
        };
        plan.confirmations = plan.confirmations.saturating_add(1);
        if plan.confirmations < plan.confirmations_required {
            self.status = format!(
                "{} on {} is {}; press y again to execute",
                plan.kind.label(),
                plan.target_label,
                plan.risk
            );
            self.operation_plan = Some(plan);
            return;
        }

        if self.readonly {
            self.status = format!(
                "readonly launch blocked {} on {}",
                plan.kind.label(),
                plan.target_label
            );
            self.operation_plan = Some(plan);
            return;
        }

        if let Some(service) = &self.service {
            match plan.kind {
                OperationKind::DeleteResource => service.send(K8sCommand::DeleteResource {
                    resource_type: plan.resource_type,
                    name: plan.target_name,
                    namespace: plan.namespace,
                }),
                OperationKind::RestartDeployment => service.send(K8sCommand::RestartResource {
                    kind: "deployment".to_string(),
                    name: plan.target_name,
                    namespace: plan.namespace,
                }),
                OperationKind::ExecPod => {
                    self.exec_target = Some(plan.target_label.clone());
                    service.send(K8sCommand::StartExec {
                        pod: plan.target_name,
                        namespace: plan.namespace,
                        container: None,
                    });
                }
            }
            self.status = "operation sent to K8sService".to_string();
        } else {
            self.status = format!(
                "fixture executed {} on {}",
                plan.kind.label(),
                plan.target_label
            );
        }
        self.operation_confirmation = None;
    }

    fn cancel_plan(&mut self) {
        if self.operation_plan.take().is_some() {
            self.operation_confirmation = None;
            self.status = "operation plan cancelled".to_string();
        } else {
            self.mode = Mode::Browser;
            self.status = "browser mode".to_string();
        }
    }

    fn agent_operation_plan_is_current(&mut self) -> bool {
        let Some(confirmation) = self.operation_confirmation.as_ref() else {
            return true;
        };
        let now = Utc::now();
        let current = self.context_share.as_ref().is_some_and(|share| {
            !share.status.is_terminal()
                && share.binding.generation == confirmation.generation
                && share.expires_at > now
                && confirmation
                    .expires_at
                    .is_none_or(|expires_at| expires_at > now)
        });
        if current {
            return true;
        }
        self.operation_plan = None;
        self.operation_confirmation = None;
        self.status = "agent operation plan expired or became stale; plan cleared".to_string();
        false
    }

    fn show_help(&mut self) {
        self.return_mode = self.mode;
        self.mode = Mode::Help;
        self.status = "help open".to_string();
    }

    fn trim_selection(&mut self) {
        self.selected = self
            .selected
            .min(self.filtered_items().len().saturating_sub(1));
    }

    fn filtered_items(&self) -> Vec<ResourceItem> {
        let filter = self.filter.to_lowercase();
        self.all_items()
            .into_iter()
            .filter(|item| filter.is_empty() || item.search_text().contains(&filter))
            .collect()
    }

    fn all_items(&self) -> Vec<ResourceItem> {
        match self.active {
            ResourceKind::Pods => self
                .pods
                .iter()
                .filter(|pod| pod.namespace == self.active_namespace)
                .cloned()
                .map(ResourceItem::Pod)
                .collect(),
            ResourceKind::Deployments => self
                .deployments
                .iter()
                .filter(|deployment| deployment.namespace == self.active_namespace)
                .cloned()
                .map(ResourceItem::Deployment)
                .collect(),
            ResourceKind::Services => self
                .services
                .iter()
                .filter(|service| service.namespace == self.active_namespace)
                .cloned()
                .map(ResourceItem::Service)
                .collect(),
            ResourceKind::Events => self
                .events
                .iter()
                .cloned()
                .map(ResourceItem::Event)
                .collect(),
            ResourceKind::Nodes => self.nodes.iter().cloned().map(ResourceItem::Node).collect(),
        }
    }

    fn selected_item(&self) -> Option<ResourceItem> {
        self.filtered_items().get(self.selected).cloned()
    }

    fn selected_pod(&self) -> Option<PodView> {
        match self.selected_item() {
            Some(ResourceItem::Pod(pod)) => Some(pod),
            _ => None,
        }
    }

    fn filter_label(&self) -> String {
        if self.filter.is_empty() {
            "none".to_string()
        } else {
            self.filter.clone()
        }
    }

    fn build_context_share_snapshot(
        &self,
        policy: &AssistContextPolicy,
    ) -> std::result::Result<AssistContextSnapshot, voidb_core::AssistContractError> {
        policy.validate()?;
        let descriptor = PluginSessionRegistration::new(
            "kubernetes",
            format!("kubernetes-tui:{}", self.profile_label),
            PluginSessionPurpose::InfrastructureClient,
            PluginSessionScope::LocalProcess,
        )
        .with_health(PluginSessionHealth::Ready)
        .with_authenticated(matches!(
            self.source,
            K8sTuiSource::Profile | K8sTuiSource::Connection
        ))
        .with_destructive_capable(!self.readonly)
        .with_stream_capable(true)
        .with_metadata(self.context_share_metadata(), RedactionStatus::Applied)
        .descriptor;
        let status_line = AssistBoundedText::capture(
            &safe_error_summary(&self.status),
            512,
            RedactionStatus::Applied,
        )?;
        let metadata_text =
            serde_json::to_string(&self.context_share_metadata()).map_err(|_| {
                voidb_core::AssistContractError::InvalidRequest(
                    "failed to encode Kubernetes context-share metadata".to_string(),
                )
            })?;
        let transcript_tail = AssistBoundedText::capture(
            &metadata_text,
            policy.metadata_bytes,
            RedactionStatus::Applied,
        )?;
        Ok(AssistContextSnapshot {
            binding: AssistSessionBinding::from_descriptor(&descriptor),
            captured_at: Utc::now(),
            mode: format!("kubernetes:{}", self.active.label()),
            health: PluginSessionHealth::Ready,
            terminal: None,
            visible_screen: None,
            transcript_tail: Some(transcript_tail),
            status_line: Some(status_line),
            withheld_fields: vec![
                AssistWithheldField {
                    field: "kubernetes.client_handle".to_string(),
                    reason: AssistWithholdingReason::Policy,
                },
                AssistWithheldField {
                    field: "kubernetes.kubeconfig".to_string(),
                    reason: AssistWithholdingReason::SecretMaterial,
                },
                AssistWithheldField {
                    field: "kubernetes.secret.data".to_string(),
                    reason: AssistWithholdingReason::SecretMaterial,
                },
            ],
            metadata: self.context_share_metadata(),
            redaction: RedactionStatus::Applied,
        })
    }

    fn context_share_metadata(&self) -> Value {
        let selected = self.selected_item().map(|item| item.safe_label());
        json!({
            "profile_label": self.profile_label,
            "cluster_label": self.cluster_label,
            "connection_kind": self.connection_kind,
            "source": self.source,
            "readonly": self.readonly,
            "active_namespace": self.active_namespace,
            "active_resource": self.active.label(),
            "selected": selected,
            "counts": {
                "namespaces": self.namespaces.len(),
                "pods": self.pods.len(),
                "deployments": self.deployments.len(),
                "services": self.services.len(),
                "events": self.events.len(),
                "nodes": self.nodes.len()
            },
            "streams": {
                "active_log_pod": self.active_log_pod.clone(),
                "active_event_watch": self.active_event_watch,
                "log_lines": self.logs.len(),
                "log_dropped_lines": self.logs.dropped_lines,
                "log_bytes": self.logs.bytes,
                "exec_lines": self.exec_output.len(),
                "exec_dropped_lines": self.exec_output.dropped_lines,
                "yaml_preview_lines": self.yaml_preview.len()
            },
            "withheld": [
                "kubernetes.client_handle",
                "kubernetes.kubeconfig",
                "kubernetes.secret.data",
                "service_account_token"
            ]
        })
    }

    fn draw(&self, frame: &mut Frame) {
        let area = frame.area();
        let vertical = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(12),
                Constraint::Length(8),
                Constraint::Length(3),
            ])
            .split(area);

        frame.render_widget(self.header(), vertical[0]);
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
            .split(vertical[1]);
        frame.render_widget(self.resource_list(body[0]), body[0]);
        frame.render_widget(self.detail_panel(), body[1]);
        frame.render_widget(self.stream_panel(), vertical[2]);
        frame.render_widget(self.status_panel(), vertical[3]);

        match self.mode {
            Mode::Help => self.draw_help(frame, area),
            Mode::Error => self.draw_error(frame, area),
            Mode::Exec => self.draw_exec_banner(frame, area),
            Mode::Browser | Mode::Filter => {}
        }
    }

    fn header(&self) -> Paragraph<'_> {
        let source = format!("{:?}", self.source).to_lowercase();
        let tabs = ResourceKind::ALL
            .iter()
            .map(|kind| {
                if *kind == self.active {
                    format!("[{}]", kind.label())
                } else {
                    kind.label().to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        Paragraph::new(vec![
            Line::from(vec![
                Span::styled(
                    "Kubernetes Operations",
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw(format!(
                    "  profile={} cluster={} namespace={}",
                    self.profile_label, self.cluster_label, self.active_namespace
                )),
            ]),
            Line::from(format!(
                "{}  source={} kind={} purpose={} readonly={} restore={} filter={} watch={}",
                tabs,
                source,
                self.connection_kind,
                self.purpose,
                self.readonly,
                self.restore,
                self.filter_label(),
                self.active_event_watch
            )),
        ])
        .block(Block::default().borders(Borders::ALL))
    }

    fn resource_list(&self, area: Rect) -> Paragraph<'_> {
        let items = self.filtered_items();
        let visible = area.height.saturating_sub(2).max(1) as usize;
        let start = window_start(self.selected, visible, items.len());
        let lines = items
            .iter()
            .enumerate()
            .skip(start)
            .take(visible)
            .map(|(idx, item)| {
                let marker = if idx == self.selected { ">" } else { " " };
                let style = if idx == self.selected {
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Green)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                };
                Line::from(Span::styled(
                    format!("{marker} {}", item.list_label()),
                    style,
                ))
            })
            .collect::<Vec<_>>();
        Paragraph::new(if lines.is_empty() {
            vec![Line::from("No resources loaded. Press r to refresh.")]
        } else {
            lines
        })
        .block(
            Block::default()
                .title(format!(
                    " {} ({}/{}) ",
                    self.active.label(),
                    self.selected.saturating_add(1).min(items.len()),
                    items.len()
                ))
                .borders(Borders::ALL),
        )
        .wrap(Wrap { trim: false })
    }

    fn detail_panel(&self) -> Paragraph<'_> {
        let mut lines = match self.selected_item() {
            Some(ResourceItem::Pod(pod)) => vec![
                Line::from(format!("pod: {}/{}", pod.namespace, pod.name)),
                Line::from(format!("status: {}  ready: {}", pod.status, pod.ready)),
                Line::from(format!("restarts: {}  age: {}", pod.restarts, pod.age)),
                Line::from(format!("node: {}", pod.node.as_deref().unwrap_or("-"))),
                Line::from(format!("containers: {}", pod.containers.join(", "))),
            ],
            Some(ResourceItem::Deployment(deployment)) => vec![
                Line::from(format!(
                    "deployment: {}/{}",
                    deployment.namespace, deployment.name
                )),
                Line::from(format!("ready: {}", deployment.ready)),
                Line::from(format!(
                    "up-to-date: {}  available: {}",
                    deployment.up_to_date, deployment.available
                )),
                Line::from(format!("age: {}", deployment.age)),
            ],
            Some(ResourceItem::Service(service)) => vec![
                Line::from(format!("service: {}/{}", service.namespace, service.name)),
                Line::from(format!("type: {}", service.service_type)),
                Line::from(format!("cluster ip: {}", service.cluster_ip)),
                Line::from(format!("ports: {}", service.ports.join(", "))),
                Line::from(format!("age: {}", service.age)),
            ],
            Some(ResourceItem::Event(event)) => vec![
                Line::from(format!("event: {} {}", event.event_type, event.reason)),
                Line::from(format!("object: {}", event.object)),
                Line::from(format!("count: {}  age: {}", event.count, event.age)),
                Line::from(format!("message: {}", event.message)),
            ],
            Some(ResourceItem::Node(node)) => vec![
                Line::from(format!("node: {}", node.name)),
                Line::from(format!("status: {}", node.status)),
                Line::from(format!("roles: {}", node.roles.join(","))),
                Line::from(format!("version: {}", node.version)),
                Line::from(format!("age: {}", node.age)),
            ],
            None => vec![Line::from("Select a resource to inspect metadata.")],
        };

        if let Some(plan) = &self.operation_plan {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "operation plan",
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(format!("action: {}", plan.kind.label())));
            lines.push(Line::from(format!("target: {}", plan.target_label)));
            lines.push(Line::from(format!("namespace: {}", plan.namespace)));
            lines.push(Line::from(format!("risk: {}", plan.risk)));
            lines.push(Line::from(format!(
                "confirmations: {}/{}",
                plan.confirmations, plan.confirmations_required
            )));
        } else if self.yaml_preview.len() > 0 {
            lines.push(Line::from(""));
            lines.push(Line::from(Span::styled(
                "yaml preview",
                Style::default().fg(Color::Cyan),
            )));
            for line in self.yaml_preview.tail(6) {
                lines.push(Line::from(line));
            }
        }

        Paragraph::new(lines)
            .block(Block::default().title(" Details ").borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn stream_panel(&self) -> Paragraph<'_> {
        let title = if self.mode == Mode::Exec {
            format!(
                " Exec {} ",
                self.exec_target.as_deref().unwrap_or("session")
            )
        } else {
            format!(
                " Logs {} ",
                self.active_log_pod.as_deref().unwrap_or("inactive")
            )
        };
        let source = if self.mode == Mode::Exec {
            &self.exec_output
        } else {
            &self.logs
        };
        let mut lines = source
            .tail(6)
            .into_iter()
            .map(Line::from)
            .collect::<Vec<_>>();
        if lines.is_empty() {
            lines.push(Line::from(
                "No stream data. Press l for pod logs, w for event watch polling, e then y for exec.",
            ));
        }
        if source.dropped_lines > 0 {
            lines.push(Line::from(Span::styled(
                format!(
                    "dropped {} lines due to stream bounds",
                    source.dropped_lines
                ),
                Style::default().fg(Color::Yellow),
            )));
        }
        Paragraph::new(lines)
            .block(Block::default().title(title).borders(Borders::ALL))
            .wrap(Wrap { trim: false })
    }

    fn status_panel(&self) -> Paragraph<'_> {
        Paragraph::new(vec![Line::from(format!(
            "{} | mode={} | namespaces={} | logs {} lines/{} dropped | exec {} lines/{} dropped",
            self.primary_status(),
            mode_label(self.mode),
            self.namespaces.len(),
            self.logs.len(),
            self.logs.dropped_lines,
            self.exec_output.len(),
            self.exec_output.dropped_lines
        ))])
        .block(Block::default().title(" Status ").borders(Borders::ALL))
    }

    fn primary_status(&self) -> String {
        self.operation_request
            .as_ref()
            .map(|request| {
                format!(
                    "agent operation: {} [y stage, d deny]",
                    safe_error_summary(&request.summary)
                )
            })
            .unwrap_or_else(|| self.status.clone())
    }

    fn draw_help(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(72, 58, area);
        frame.render_widget(Clear, popup);
        let help = vec![
            Line::from("Kubernetes operations TUI"),
            Line::from("Tab / Shift+Tab: switch resource type"),
            Line::from("n / N: switch namespace"),
            Line::from("j/k: move  /: filter  r: refresh  i: YAML preview"),
            Line::from("l: follow pod logs  w: event watch polling  c: cancel streams"),
            Line::from("a: share bounded current-view context"),
            Line::from("pending agent operation: y stage plan  d deny"),
            Line::from("e: exec pod  x: delete resource  R: restart deployment"),
            Line::from("y: confirm plan  Esc: cancel plan  q: quit"),
            Line::from("Exec escape: Ctrl+] then q"),
        ];
        frame.render_widget(
            Paragraph::new(help)
                .block(Block::default().title(" Help ").borders(Borders::ALL))
                .alignment(Alignment::Left)
                .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_error(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(72, 40, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Kubernetes target error",
                    Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                )),
                Line::from(self.status.clone()),
                Line::from("Press any key to return to the browser."),
            ])
            .block(Block::default().title(" Error ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
            popup,
        );
    }

    fn draw_exec_banner(&self, frame: &mut Frame, area: Rect) {
        let popup = centered_rect(58, 18, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(
                    "Exec input is active",
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                )),
                Line::from(format!(
                    "target: {}",
                    self.exec_target.as_deref().unwrap_or("pod")
                )),
                Line::from("stdin is forwarded to the pod."),
                Line::from("Escape: Ctrl+] then q"),
            ])
            .block(Block::default().title(" Exec ").borders(Borders::ALL))
            .wrap(Wrap { trim: false }),
            popup,
        );
    }
}

impl BoundedLines {
    fn new(max_lines: usize, max_bytes: usize) -> Self {
        Self {
            lines: VecDeque::new(),
            bytes: 0,
            dropped_lines: 0,
            max_lines,
            max_bytes,
        }
    }

    fn len(&self) -> usize {
        self.lines.len()
    }

    fn clear(&mut self) {
        self.lines.clear();
        self.bytes = 0;
        self.dropped_lines = 0;
    }

    fn push_text(&mut self, text: &str) {
        for line in text.lines() {
            self.push_line(line.to_string());
        }
    }

    fn push_line(&mut self, mut line: String) {
        if line.len() > self.max_bytes {
            line.truncate(self.max_bytes);
        }
        self.bytes = self.bytes.saturating_add(line.len());
        self.lines.push_back(line);
        while self.lines.len() > self.max_lines || self.bytes > self.max_bytes {
            if let Some(removed) = self.lines.pop_front() {
                self.bytes = self.bytes.saturating_sub(removed.len());
                self.dropped_lines = self.dropped_lines.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn tail(&self, count: usize) -> Vec<String> {
        self.lines
            .iter()
            .rev()
            .take(count)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }
}

impl ResourceKind {
    const ALL: [ResourceKind; 5] = [
        ResourceKind::Pods,
        ResourceKind::Deployments,
        ResourceKind::Services,
        ResourceKind::Events,
        ResourceKind::Nodes,
    ];

    fn label(self) -> &'static str {
        match self {
            ResourceKind::Pods => "pods",
            ResourceKind::Deployments => "deployments",
            ResourceKind::Services => "services",
            ResourceKind::Events => "events",
            ResourceKind::Nodes => "nodes",
        }
    }

    fn resource_type(self) -> &'static str {
        self.label()
    }
}

impl ResourceItem {
    fn list_label(&self) -> String {
        match self {
            ResourceItem::Pod(pod) => format!(
                "{:<8} {:<34} {:<7} {:<8} {}",
                pod.status, pod.name, pod.ready, pod.restarts, pod.age
            ),
            ResourceItem::Deployment(deployment) => format!(
                "{:<34} {:<8} {:<8} {:<8} {}",
                deployment.name,
                deployment.ready,
                deployment.up_to_date,
                deployment.available,
                deployment.age
            ),
            ResourceItem::Service(service) => format!(
                "{:<30} {:<12} {:<16} {}",
                service.name,
                service.service_type,
                service.cluster_ip,
                service.ports.join(",")
            ),
            ResourceItem::Event(event) => format!(
                "{:<7} {:<18} {:<28} x{} {}",
                event.event_type, event.reason, event.object, event.count, event.message
            ),
            ResourceItem::Node(node) => format!(
                "{:<34} {:<9} {:<14} {}",
                node.name,
                node.status,
                node.roles.join(","),
                node.version
            ),
        }
    }

    fn safe_label(&self) -> String {
        match self {
            ResourceItem::Pod(pod) => format!("{}/{}", pod.namespace, pod.name),
            ResourceItem::Deployment(deployment) => {
                format!("{}/{}", deployment.namespace, deployment.name)
            }
            ResourceItem::Service(service) => format!("{}/{}", service.namespace, service.name),
            ResourceItem::Event(event) => format!("{} {}", event.reason, event.object),
            ResourceItem::Node(node) => node.name.clone(),
        }
    }

    fn search_text(&self) -> String {
        match self {
            ResourceItem::Pod(pod) => format!(
                "{} {} {} {} {}",
                pod.namespace,
                pod.name,
                pod.status,
                pod.ready,
                pod.containers.join(" ")
            ),
            ResourceItem::Deployment(deployment) => {
                format!(
                    "{} {} {}",
                    deployment.namespace, deployment.name, deployment.ready
                )
            }
            ResourceItem::Service(service) => format!(
                "{} {} {} {} {}",
                service.namespace,
                service.name,
                service.service_type,
                service.cluster_ip,
                service.ports.join(" ")
            ),
            ResourceItem::Event(event) => format!(
                "{} {} {} {}",
                event.event_type, event.reason, event.object, event.message
            ),
            ResourceItem::Node(node) => {
                format!(
                    "{} {} {} {}",
                    node.name,
                    node.status,
                    node.roles.join(" "),
                    node.version
                )
            }
        }
        .to_lowercase()
    }

    fn resource_type(&self) -> &'static str {
        match self {
            ResourceItem::Pod(_) => "pod",
            ResourceItem::Deployment(_) => "deployment",
            ResourceItem::Service(_) => "service",
            ResourceItem::Event(_) => "event",
            ResourceItem::Node(_) => "node",
        }
    }

    fn name(&self) -> String {
        match self {
            ResourceItem::Pod(pod) => pod.name.clone(),
            ResourceItem::Deployment(deployment) => deployment.name.clone(),
            ResourceItem::Service(service) => service.name.clone(),
            ResourceItem::Event(event) => event.object.clone(),
            ResourceItem::Node(node) => node.name.clone(),
        }
    }

    fn namespace(&self) -> Option<String> {
        match self {
            ResourceItem::Pod(pod) => Some(pod.namespace.clone()),
            ResourceItem::Deployment(deployment) => Some(deployment.namespace.clone()),
            ResourceItem::Service(service) => Some(service.namespace.clone()),
            ResourceItem::Event(_) | ResourceItem::Node(_) => None,
        }
    }
}

impl OperationKind {
    fn label(&self) -> &'static str {
        match self {
            OperationKind::DeleteResource => "delete resource",
            OperationKind::RestartDeployment => "rollout restart",
            OperationKind::ExecPod => "exec pod",
        }
    }
}

fn exec_key_bytes(key: KeyEvent) -> Option<Vec<u8>> {
    match key.code {
        KeyCode::Char(ch) if key.modifiers.contains(KeyModifiers::CONTROL) => {
            let lower = ch.to_ascii_lowercase();
            if lower.is_ascii_lowercase() {
                Some(vec![(lower as u8) - b'a' + 1])
            } else {
                None
            }
        }
        KeyCode::Char(ch) => Some(ch.to_string().into_bytes()),
        KeyCode::Enter => Some(vec![b'\n']),
        KeyCode::Backspace => Some(vec![0x7f]),
        KeyCode::Tab => Some(vec![b'\t']),
        KeyCode::Esc => Some(vec![0x1b]),
        _ => None,
    }
}

fn connection_value(config: &K8sConfig) -> Value {
    match &config.connection {
        K8sConnection::Kubeconfig { path: _, context } => json!({
            "kind": "kubeconfig",
            "cluster_label": context.as_deref().unwrap_or("current-context"),
            "kubeconfig_path": "redacted",
            "timeout_seconds": config.timeout,
            "secret_material": "redacted"
        }),
        K8sConnection::Direct {
            api_url,
            auth,
            verify_ssl,
            ca_cert: _,
        } => json!({
            "kind": "direct",
            "cluster_label": redacted_url(api_url),
            "auth": auth_label(auth),
            "verify_ssl": verify_ssl,
            "ca_cert": "redacted",
            "timeout_seconds": config.timeout,
            "secret_material": "redacted"
        }),
    }
}

fn connection_labels(config: &K8sConfig) -> (String, String) {
    match &config.connection {
        K8sConnection::Kubeconfig { context, .. } => (
            "kubeconfig".to_string(),
            context
                .clone()
                .unwrap_or_else(|| "current-context".to_string()),
        ),
        K8sConnection::Direct { api_url, .. } => ("direct".to_string(), redacted_url(api_url)),
    }
}

fn auth_label(auth: &K8sAuth) -> &'static str {
    match auth {
        K8sAuth::Token { token: _ } => "token",
        K8sAuth::ClientCert {
            cert_path: _,
            key_path: _,
        } => "client_cert",
        K8sAuth::InCluster => "in_cluster",
    }
}

fn redacted_url(url: &str) -> String {
    let without_fragment = url.split('#').next().unwrap_or(url);
    let without_query = without_fragment
        .split('?')
        .next()
        .unwrap_or(without_fragment);
    if let Some((scheme, rest)) = without_query.split_once("://")
        && let Some((_, host_path)) = rest.rsplit_once('@')
    {
        return format!("{scheme}://redacted@{host_path}");
    }
    without_query.to_string()
}

fn redact_yaml_preview(yaml: &str) -> String {
    yaml.lines()
        .map(|line| {
            let lower = line.to_lowercase();
            if lower.contains("token:")
                || lower.contains("password:")
                || lower.contains("secret:")
                || lower.contains("client-key-data:")
                || lower.contains("certificate-authority-data:")
            {
                "  redacted: true".to_string()
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn pod_sort(left: &PodView, right: &PodView) -> std::cmp::Ordering {
    pod_rank(&left.status)
        .cmp(&pod_rank(&right.status))
        .then_with(|| left.name.cmp(&right.name))
}

fn pod_rank(status: &str) -> u8 {
    match status {
        "Running" => 0,
        "Pending" => 1,
        "Succeeded" => 2,
        "Failed" => 3,
        _ => 4,
    }
}

fn pod_value(pod: &PodView) -> Value {
    json!({
        "name": pod.name,
        "namespace": pod.namespace,
        "status": pod.status,
        "ready": pod.ready,
        "restarts": pod.restarts,
        "age": pod.age,
        "node": pod.node,
        "containers": pod.containers
    })
}

fn deployment_value(deployment: &DeploymentView) -> Value {
    json!({
        "name": deployment.name,
        "namespace": deployment.namespace,
        "ready": deployment.ready,
        "up_to_date": deployment.up_to_date,
        "available": deployment.available,
        "age": deployment.age
    })
}

fn service_value(service: &ServiceView) -> Value {
    json!({
        "name": service.name,
        "namespace": service.namespace,
        "service_type": service.service_type,
        "cluster_ip": service.cluster_ip,
        "ports": service.ports,
        "age": service.age
    })
}

fn event_value(event: &EventView) -> Value {
    json!({
        "event_type": event.event_type,
        "reason": event.reason,
        "object": event.object,
        "message": event.message,
        "age": event.age,
        "count": event.count
    })
}

fn node_value(node: &NodeView) -> Value {
    json!({
        "name": node.name,
        "status": node.status,
        "roles": node.roles,
        "version": node.version,
        "age": node.age
    })
}

fn safe_error_summary(message: &str) -> String {
    let mut summary = message.replace('\n', " ");
    if summary.len() > 180 {
        summary.truncate(177);
        summary.push_str("...");
    }
    summary
}

fn secret_leak_markers(text: &str) -> Vec<String> {
    [
        "kube_token_value",
        "service_account_token_value",
        "client_key_material",
        "certificate_authority_secret",
        "raw_kubeconfig",
        "secret_data_value",
    ]
    .into_iter()
    .filter(|marker| text.contains(marker))
    .map(str::to_string)
    .collect()
}

fn k8s_context_share_store() -> Result<AgentContextShareStore> {
    AgentContextShareStore::new(
        kubernetes_agent_context_store_root()?,
        AgentContextSharePolicy::non_pty(),
    )
}

pub fn kubernetes_agent_context_store_root() -> Result<std::path::PathBuf> {
    if let Some(path) = std::env::var_os(K8S_AGENT_CONTEXT_STORE_DIR_ENV)
        .or_else(|| std::env::var_os(LEGACY_K8S_ASSIST_STORE_DIR_ENV))
    {
        Ok(std::path::PathBuf::from(path))
    } else {
        Ok(AppConfig::config_dir()
            .map_err(|error| anyhow!(error.to_string()))?
            .join("kubernetes-assist"))
    }
}

fn operation_target_label(action: &AgentOperation) -> String {
    match action {
        AgentOperation::CapabilityCall {
            capability_id,
            input_summary,
            ..
        } => {
            let target = input_summary
                .get("target_name")
                .and_then(Value::as_str)
                .unwrap_or("kubernetes-target");
            format!("capability:{capability_id}:{target}")
        }
        AgentOperation::Guidance { title, .. } => format!("guidance:{title}"),
        AgentOperation::RequestPermission { permission, .. } => {
            format!("permission:{permission:?}")
        }
        AgentOperation::ProposedCommand { target, .. } => format!("command:{target:?}"),
    }
}

fn operation_summary(action: &AgentOperation) -> Option<String> {
    match action {
        AgentOperation::CapabilityCall {
            capability_id,
            input_summary,
            ..
        } => Some(format!(
            "{capability_id} {}",
            safe_error_summary(&input_summary.to_string())
        )),
        AgentOperation::ProposedCommand { command, .. } => Some(safe_error_summary(command)),
        _ => None,
    }
}

fn operation_capability_id(action: &AgentOperation) -> Option<String> {
    match action {
        AgentOperation::CapabilityCall { capability_id, .. } => Some(capability_id.clone()),
        AgentOperation::ProposedCommand {
            target: AgentOperationTarget::Capability { capability_id },
            ..
        } => Some(capability_id.clone()),
        _ => None,
    }
}

fn mode_label(mode: Mode) -> &'static str {
    match mode {
        Mode::Browser => "browser",
        Mode::Filter => "filter",
        Mode::Help => "help",
        Mode::Error => "error",
        Mode::Exec => "exec",
    }
}

fn window_start(selected: usize, visible: usize, len: usize) -> usize {
    if visible == 0 || len <= visible {
        0
    } else if selected >= visible {
        (selected + 1).saturating_sub(visible)
    } else {
        0
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

struct StandaloneK8sTabManager {
    render_tx: mpsc::UnboundedSender<()>,
    should_quit: Arc<AtomicBool>,
}

impl StandaloneK8sTabManager {
    fn new(should_quit: Arc<AtomicBool>) -> Self {
        let (render_tx, _render_rx) = mpsc::unbounded_channel();
        Self {
            render_tx,
            should_quit,
        }
    }

    fn unsupported_tabs_error() -> anyhow::Error {
        anyhow!("Kubernetes TUI does not host plugin tabs; use plugin-owned CLI commands instead")
    }
}

impl TabManager for StandaloneK8sTabManager {
    fn open(&self, _title: String, _plugin_id: String, _context: Value) -> Result<()> {
        Err(Self::unsupported_tabs_error())
    }

    fn close_current(&self) -> Result<()> {
        self.quit()
    }

    fn set_title(&self, _title: String) -> Result<()> {
        Ok(())
    }

    fn request_render(&self) -> Result<()> {
        let _ = self.render_tx.send(());
        Ok(())
    }

    fn list_tabs(&self) -> Result<Vec<TabInfo>> {
        Ok(vec![TabInfo {
            index: 0,
            title: "Kubernetes".to_string(),
            plugin_id: "kubernetes".to_string(),
            context: json!({}),
            is_active: true,
        }])
    }

    fn close_tab(&self, index: usize) -> Result<()> {
        if index == 0 {
            self.quit()
        } else {
            Err(anyhow!("Kubernetes TUI has no tab {index}"))
        }
    }

    fn switch_to(&self, index: usize) -> Result<()> {
        if index == 0 {
            Ok(())
        } else {
            Err(anyhow!("Kubernetes TUI has no tab {index}"))
        }
    }

    fn active_tab_index(&self) -> Result<usize> {
        Ok(0)
    }

    fn quit(&self) -> Result<()> {
        self.should_quit.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_launch(readonly: bool) -> K8sTuiLaunch {
        K8sTuiLaunch {
            profile_label: "fixture".to_string(),
            config: None,
            source: K8sTuiSource::Fixture,
            fixture_path: Some(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("fixtures/kubernetes_tui_operations.json")
                    .display()
                    .to_string(),
            ),
            purpose: "operations".to_string(),
            readonly,
            restore: true,
            launch_plan: None,
        }
    }

    #[test]
    fn preflight_redacts_direct_auth_material() {
        let launch = K8sTuiLaunch {
            profile_label: "direct-k8s".to_string(),
            config: Some(K8sConfig {
                connection: K8sConnection::Direct {
                    api_url: "https://user:kube_token_value@cluster.example.test:6443?token=service_account_token_value".to_string(),
                    auth: K8sAuth::Token {
                        token: "kube_token_value".to_string(),
                    },
                    verify_ssl: true,
                    ca_cert: Some("certificate_authority_secret".to_string()),
                },
                default_namespace: Some("default".to_string()),
                timeout: 20,
            }),
            source: K8sTuiSource::Connection,
            fixture_path: None,
            purpose: "operations".to_string(),
            readonly: false,
            restore: true,
            launch_plan: None,
        };
        let rendered = serde_json::to_string(&preflight_value(&launch)).unwrap();
        assert!(rendered.contains("redacted"));
        assert!(!rendered.contains("kube_token_value"));
        assert!(!rendered.contains("service_account_token_value"));
        assert!(!rendered.contains("certificate_authority_secret"));
    }

    #[test]
    fn fixture_evidence_has_coverage_and_no_secret_markers() {
        let evidence = build_k8s_tui_evidence(&fixture_launch(false)).unwrap();
        let rendered = serde_json::to_string(&evidence).unwrap();
        assert!(rendered.contains("kubernetes_tui_fixture_evidence"));
        assert!(rendered.contains("delete_double_confirmation"));
        assert!(rendered.contains("current_pty_rejected"));
        assert!(!rendered.contains("assist_handoff"));
        assert!(!rendered.contains("response_review"));
        assert_eq!(
            evidence["external_agent_interaction"]["broker_policy"],
            json!("non_pty")
        );
        assert_eq!(evidence["secret_leak_scan"]["passed"], json!(true));
    }

    #[test]
    fn filter_matches_pod_fields() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.filter = "api".to_string();
        let items = app.filtered_items();
        assert_eq!(items.len(), 1);
        assert!(items[0].safe_label().contains("api"));
    }

    #[test]
    fn namespace_cycle_refreshes_active_namespace() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        assert_eq!(app.active_namespace, "default");
        app.next_namespace();
        assert_eq!(app.active_namespace, "ops");
    }

    #[test]
    fn delete_plan_requires_second_confirmation() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.plan_delete();
        app.confirm_plan();
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("press y again"));
        app.confirm_plan();
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed delete resource"));
    }

    #[test]
    fn readonly_launch_blocks_confirmed_plan() {
        let mut app = K8sTuiApp::new(fixture_launch(true)).unwrap();
        app.plan_delete();
        app.confirm_plan();
        app.confirm_plan();
        assert!(app.operation_plan.is_some());
        assert!(app.status.contains("readonly launch blocked"));
    }

    #[test]
    fn external_agent_operation_stages_existing_kubernetes_confirmation_plan() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_pod().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, k8s_operation_request(&request_id, &target))
            .unwrap();

        app.sync_agent_operation();
        assert!(app.status.contains("agent operation ready"));
        assert!(app.primary_status().contains("[y stage, d deny]"));
        app.stage_agent_operation();
        assert!(app.operation_plan.is_some());
        assert!(app.operation_confirmation.is_some());
        let detail = app.context_share_store.detail(&request_id).unwrap();
        assert_eq!(detail.record.action_confirmations.len(), 1);
        assert!(!detail.record.action_confirmations[0].uses_current_pty);

        app.confirm_plan();
        assert!(app.operation_plan.is_some());
        app.confirm_plan();
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("fixture executed delete resource"));
    }

    #[test]
    fn external_agent_operation_can_be_denied_without_staging() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_pod().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, k8s_operation_request(&request_id, &target))
            .unwrap();

        app.sync_agent_operation();
        app.handle_key(KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE));

        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("denied"));
        let detail = app.context_share_store.detail(&request_id).unwrap();
        assert_eq!(
            detail.record.action_confirmations[0].status,
            "denied_by_user"
        );
    }

    #[test]
    fn external_multi_action_request_is_never_partially_staged() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store =
            temp_context_share_store_with_policy(AgentContextSharePolicy::current_pty_capable());
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_pod().unwrap();
        let mut request = k8s_operation_request(&request_id, &target);
        request.actions.push(request.actions[0].clone());
        app.context_share_store
            .post_operation_request(&request_id, request)
            .unwrap();

        assert!(app.sync_agent_operation());
        assert!(app.operation_request.is_none());
        assert!(app.operation_plan.is_none());
        assert!(app.status.contains("exactly one operation"));
        assert!(
            app.context_share_store
                .detail(&request_id)
                .unwrap()
                .record
                .action_confirmations
                .is_empty()
        );
    }

    #[test]
    fn expired_agent_plan_is_cleared_before_service_dispatch() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.context_share_store = temp_context_share_store();
        app.share_agent_context();
        let request_id = app.context_share.as_ref().unwrap().id.clone();
        let target = app.selected_pod().unwrap();
        app.context_share_store
            .post_operation_request(&request_id, k8s_operation_request(&request_id, &target))
            .unwrap();
        app.sync_agent_operation();
        app.stage_agent_operation();
        app.context_share.as_mut().unwrap().expires_at = Utc::now() - chrono::Duration::seconds(1);

        app.confirm_plan();
        assert!(app.operation_plan.is_none());
        assert!(app.operation_confirmation.is_none());
        assert!(app.status.contains("expired or became stale"));
    }

    #[test]
    fn uppercase_a_is_not_an_operation_shortcut() {
        let mut app = K8sTuiApp::new(fixture_launch(false)).unwrap();
        app.handle_key(KeyEvent::new(KeyCode::Char('A'), KeyModifiers::NONE));
        assert!(app.status.contains("a share context"));
        assert!(!app.status.contains("assist"));
        assert!(!app.status.contains("poll"));
    }

    fn temp_context_share_store() -> AgentContextShareStore {
        temp_context_share_store_with_policy(AgentContextSharePolicy::non_pty())
    }

    fn temp_context_share_store_with_policy(
        policy: AgentContextSharePolicy,
    ) -> AgentContextShareStore {
        AgentContextShareStore::new(
            std::env::temp_dir().join(format!(
                "voidb-k8s-agent-context-test-{}",
                uuid::Uuid::new_v4()
            )),
            policy,
        )
        .unwrap()
    }

    fn k8s_operation_request(request_id: &str, target: &PodView) -> AgentOperationRequest {
        AgentOperationRequest {
            id: format!("agent:operation:kubernetes:{}", uuid::Uuid::new_v4()),
            request_id: request_id.to_string(),
            agent: AgentPrincipal {
                client_id: "agent".to_string(),
                task_id: "tw-90".to_string(),
                instance_id: Some("kubernetes-agent-operation-test".to_string()),
            },
            created_at: Utc::now(),
            summary: "Delete the selected pod only after operator review.".to_string(),
            diagnosis: Some("Fixture pod is the target for action review.".to_string()),
            actions: vec![AgentOperation::CapabilityCall {
                capability_id: "kubernetes.delete".to_string(),
                input_summary: json!({
                    "action": "delete",
                    "resource_type": "pod",
                    "namespace": target.namespace.clone(),
                    "target_name": target.name.clone()
                }),
                rationale: "Use the existing Kubernetes delete plan and confirmation gate."
                    .to_string(),
                risk: AgentOperationRisk::Destructive,
                target: AgentOperationTarget::Capability {
                    capability_id: "kubernetes.delete".to_string(),
                },
            }],
            requested_permissions: vec![AssistPermission::ProposeCommands],
            redaction: RedactionStatus::Applied,
        }
    }
}
