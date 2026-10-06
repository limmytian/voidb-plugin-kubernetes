//! Bounded Kubernetes live workflows exposed through the generic agent broker.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use voidb_core::{
    AGENT_LIVE_SESSION_PROTOCOL_VERSION, AgentLiveSessionAuditIdentity,
    AgentLiveSessionBufferOverflow, AgentLiveSessionBufferPolicy, AgentLiveSessionCallCancellation,
    AgentLiveSessionCancelBehavior, AgentLiveSessionCloseEffect, AgentLiveSessionContract,
    AgentLiveSessionControlPolicy, AgentLiveSessionCursor, AgentLiveSessionCursorKind,
    AgentLiveSessionEventBuffer, AgentLiveSessionEventKind, AgentLiveSessionKind,
    AgentLiveSessionOperations, AgentLiveSessionReadRequest, AgentLiveSessionReconnectMode,
    AgentLiveSessionReconnectPolicy, AgentLiveSessionResourceDescriptor,
    AgentLiveSessionResumeMode, AgentLiveSessionStartRequest, AgentSessionCallRequest,
    AgentSessionCallResult, AgentSessionOpenContext, CapabilityRiskLevel, PluginAgentSession,
    PluginAgentSessionFactory, PluginSessionError, PluginSessionErrorCode, PluginSessionHealth,
    PluginSessionPurpose, RedactionStatus, RedactionTarget, collect_redaction_targets,
    redact_text_with_targets,
};

use crate::config::K8sConfig;
use crate::service::agent_live::{
    K8sAgentService, K8sLogSpec, K8sPortForwardControl, K8sPortForwardEvent, K8sTerminalControl,
    K8sTerminalOutput, K8sWatchEvent, K8sWatchSpec,
};

pub(crate) const WATCH_EVENTS_CAPABILITY: &str = "kubernetes.watch_events";
pub(crate) const LOGS_FOLLOW_CAPABILITY: &str = "kubernetes.logs_follow";
pub(crate) const EXEC_READ_CAPABILITY: &str = "kubernetes.exec_read";
pub(crate) const EXEC_INPUT_CAPABILITY: &str = "kubernetes.exec_input";
pub(crate) const EXEC_RESIZE_CAPABILITY: &str = "kubernetes.exec_resize";
pub(crate) const PORT_FORWARD_EVENTS_CAPABILITY: &str = "kubernetes.port_forward_events";

const EXEC_CAPABILITIES: &[&str] = &[
    EXEC_READ_CAPABILITY,
    EXEC_INPUT_CAPABILITY,
    EXEC_RESIZE_CAPABILITY,
];
const DEFAULT_LOG_TAIL: i64 = 100;
const MAX_LOG_TAIL: i64 = 5_000;
const DEFAULT_COLS: u16 = 120;
const DEFAULT_ROWS: u16 = 40;
const MIN_COLS: u16 = 20;
const MAX_COLS: u16 = 500;
const MIN_ROWS: u16 = 5;
const MAX_ROWS: u16 = 200;
const MAX_TERMINAL_WRITE_BYTES: usize = 16 * 1024;
const MAX_EVENT_TEXT_BYTES: usize = 48 * 1024;

pub struct K8sAgentSessionFactory {
    config: K8sConfig,
    redaction_targets: Arc<Vec<RedactionTarget>>,
}

impl K8sAgentSessionFactory {
    pub fn new(config: K8sConfig) -> Self {
        let redaction_targets = serde_json::to_value(&config)
            .map(|value| collect_redaction_targets(&value))
            .unwrap_or_default();
        Self {
            config,
            redaction_targets: Arc::new(redaction_targets),
        }
    }
}

#[async_trait]
impl PluginAgentSessionFactory for K8sAgentSessionFactory {
    fn plugin_id(&self) -> &str {
        "kubernetes"
    }

    async fn open(
        &self,
        context: AgentSessionOpenContext,
    ) -> Result<Arc<dyn PluginAgentSession>, PluginSessionError> {
        let family = K8sLiveFamily::from_binding(&context)?;
        let representative = family.capabilities()[0];
        let (purpose, contract) = kubernetes_live_session_contract(representative)
            .expect("Kubernetes live family contract");
        if context.binding.purpose != purpose {
            return Err(session_error(
                PluginSessionErrorCode::BindingMismatch,
                "The Kubernetes live-session purpose does not match its capability family.",
            ));
        }
        contract.validate(&context.binding.allowed_capabilities)?;
        contract.validate_start(&context.request.input)?;
        let start: AgentLiveSessionStartRequest =
            serde_json::from_value(context.request.input.clone()).map_err(|_| {
                session_error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The Kubernetes live-session start envelope is invalid.",
                )
            })?;
        let policy = start
            .buffer
            .clone()
            .unwrap_or_else(|| contract.buffer.clone());
        let buffer = AgentLiveSessionEventBuffer::new(contract.clone(), policy)?;
        let service = Arc::new(K8sAgentService::connect(&self.config).await.map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "The Kubernetes cluster could not be reached or authenticated.",
            )
        })?);

        let mut terminal = None;
        let mut port_forward = None;
        let producer_buffer = buffer.clone();
        let targets = Arc::clone(&self.redaction_targets);
        let task = match family {
            K8sLiveFamily::Watch => {
                let source = Arc::clone(&service);
                tokio::spawn(async move {
                    run_watch(source, start, producer_buffer.clone(), targets).await;
                    producer_buffer.close_source().await;
                })
            }
            K8sLiveFamily::Logs => {
                let source = Arc::clone(&service);
                tokio::spawn(async move {
                    run_logs(source, start, producer_buffer.clone(), targets).await;
                    producer_buffer.close_source().await;
                })
            }
            K8sLiveFamily::Exec => {
                let parts = open_exec(&service, &start).await?;
                terminal = Some(Arc::clone(&parts.control));
                tokio::spawn(async move {
                    run_exec_output(parts.output, producer_buffer.clone(), targets).await;
                    producer_buffer.close_source().await;
                })
            }
            K8sLiveFamily::PortForward => {
                let parts = open_port_forward(&service, &start).await?;
                port_forward = Some(Arc::clone(&parts.control));
                tokio::spawn(async move {
                    run_port_forward_events(parts.events, producer_buffer.clone()).await;
                    producer_buffer.close_source().await;
                })
            }
        };

        Ok(Arc::new(K8sAgentLiveSession {
            family,
            buffer,
            terminal,
            port_forward,
            cancellations: AgentLiveSessionCallCancellation::default(),
            task: Mutex::new(Some(task)),
            closed: AtomicBool::new(false),
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum K8sLiveFamily {
    Watch,
    Logs,
    Exec,
    PortForward,
}

impl K8sLiveFamily {
    fn from_binding(context: &AgentSessionOpenContext) -> Result<Self, PluginSessionError> {
        let actual = context
            .binding
            .allowed_capabilities
            .iter()
            .map(String::as_str)
            .collect::<HashSet<_>>();
        for family in [Self::Watch, Self::Logs, Self::Exec, Self::PortForward] {
            let expected = family
                .capabilities()
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if actual == expected {
                return Ok(family);
            }
        }
        Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            "A Kubernetes live session requires one complete, unmixed capability family.",
        ))
    }

    fn capabilities(self) -> &'static [&'static str] {
        match self {
            Self::Watch => &[WATCH_EVENTS_CAPABILITY],
            Self::Logs => &[LOGS_FOLLOW_CAPABILITY],
            Self::Exec => EXEC_CAPABILITIES,
            Self::PortForward => &[PORT_FORWARD_EVENTS_CAPABILITY],
        }
    }

    fn read_capability(self) -> &'static str {
        self.capabilities()[0]
    }
}

struct K8sAgentLiveSession {
    family: K8sLiveFamily,
    buffer: AgentLiveSessionEventBuffer,
    terminal: Option<Arc<K8sTerminalControl>>,
    port_forward: Option<Arc<K8sPortForwardControl>>,
    cancellations: AgentLiveSessionCallCancellation,
    task: Mutex<Option<JoinHandle<()>>>,
    closed: AtomicBool,
}

#[async_trait]
impl PluginAgentSession for K8sAgentLiveSession {
    async fn call(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        if !self
            .family
            .capabilities()
            .contains(&request.capability.as_str())
        {
            return Err(session_error(
                PluginSessionErrorCode::PolicyDenied,
                "The Kubernetes call is outside this live-session binding.",
            ));
        }
        match request.capability.as_str() {
            capability if capability == self.family.read_capability() => {
                self.read_events(request).await
            }
            EXEC_INPUT_CAPABILITY => self.write_terminal(request).await,
            EXEC_RESIZE_CAPABILITY => self.resize_terminal(request).await,
            _ => Err(session_error(
                PluginSessionErrorCode::Unsupported,
                "The Kubernetes live-session operation is unsupported.",
            )),
        }
    }

    async fn health(&self) -> Result<PluginSessionHealth, PluginSessionError> {
        Ok(if self.closed.load(Ordering::Acquire) {
            PluginSessionHealth::Closed
        } else {
            PluginSessionHealth::Ready
        })
    }

    async fn cancel(&self, call_id: &str) -> Result<(), PluginSessionError> {
        self.cancellations.cancel(call_id).await;
        Ok(())
    }

    async fn close(&self, _reason: String) -> Result<(), PluginSessionError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.cancellations.close().await;
        let terminal_result = if let Some(terminal) = &self.terminal {
            terminal.close().await.map_err(|_| {
                session_error(
                    PluginSessionErrorCode::OwnerUnavailable,
                    "Kubernetes exec cleanup could not be verified.",
                )
            })
        } else {
            Ok(())
        };
        if let Some(port_forward) = &self.port_forward {
            port_forward.close().await;
        }
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        self.buffer.close_source().await;
        terminal_result
    }
}

impl K8sAgentLiveSession {
    async fn read_events(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let mut read = if request.input.is_null() {
            AgentLiveSessionReadRequest::default()
        } else {
            serde_json::from_value(request.input.clone()).map_err(|_| {
                session_error(
                    PluginSessionErrorCode::PolicyDenied,
                    "The Kubernetes live-session read request is invalid.",
                )
            })?
        };
        read.max_bytes = read.max_bytes.min(request.output_limit_bytes);
        let call_id = request.call_id.clone();
        let batch = self
            .cancellations
            .run(&call_id, self.buffer.read(&read))
            .await?;
        let output = serde_json::to_value(batch).map_err(|_| {
            session_error(
                PluginSessionErrorCode::RedactionFailed,
                "The Kubernetes live-session batch could not be serialized.",
            )
        })?;
        AgentSessionCallResult::bounded(call_id, output, request.output_limit_bytes)
    }

    async fn write_terminal(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let terminal = self.terminal.as_ref().ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::Unsupported,
                "This Kubernetes session has no exec input.",
            )
        })?;
        let bytes = terminal_input(&request.input)?;
        let written = bytes.len();
        terminal.write(&bytes).await.map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "Kubernetes exec input failed.",
            )
        })?;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "written_bytes": written }),
            request.output_limit_bytes,
        )
    }

    async fn resize_terminal(
        &self,
        request: AgentSessionCallRequest,
    ) -> Result<AgentSessionCallResult, PluginSessionError> {
        let terminal = self.terminal.as_ref().ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::Unsupported,
                "This Kubernetes session has no exec resize control.",
            )
        })?;
        let cols = terminal_dimension(&request.input, "cols", None)?;
        let rows = terminal_dimension(&request.input, "rows", None)?;
        terminal.resize(cols, rows).await.map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "Kubernetes exec resize failed.",
            )
        })?;
        AgentSessionCallResult::bounded(
            request.call_id,
            json!({ "cols": cols, "rows": rows }),
            request.output_limit_bytes,
        )
    }
}

pub(crate) fn kubernetes_live_session_contract(
    capability: &str,
) -> Option<(PluginSessionPurpose, AgentLiveSessionContract)> {
    let family = match capability {
        WATCH_EVENTS_CAPABILITY => K8sLiveFamily::Watch,
        LOGS_FOLLOW_CAPABILITY => K8sLiveFamily::Logs,
        EXEC_READ_CAPABILITY | EXEC_INPUT_CAPABILITY | EXEC_RESIZE_CAPABILITY => {
            K8sLiveFamily::Exec
        }
        PORT_FORWARD_EVENTS_CAPABILITY => K8sLiveFamily::PortForward,
        _ => return None,
    };
    let (
        purpose,
        kind,
        resource_type,
        identity_schema,
        identity_fields,
        parameters,
        buffer,
        reconnect,
        close,
        start_risk,
    ) = match family {
        K8sLiveFamily::Watch => (
            PluginSessionPurpose::WatchStream,
            AgentLiveSessionKind::Watch,
            "kubernetes_api_resource",
            json!({
                "type": "object",
                "required": ["api_version", "kind", "plural"],
                "properties": {
                    "api_version": { "type": "string", "minLength": 1, "maxLength": 256 },
                    "kind": { "type": "string", "minLength": 1, "maxLength": 128 },
                    "plural": { "type": "string", "minLength": 1, "maxLength": 128 },
                    "namespace": { "type": "string", "minLength": 1, "maxLength": 253 }
                },
                "additionalProperties": false
            }),
            vec!["/api_version".into(), "/kind".into(), "/plural".into()],
            json!({
                "type": "object",
                "properties": {
                    "label_selector": { "type": "string", "maxLength": 1024 },
                    "field_selector": { "type": "string", "maxLength": 1024 }
                },
                "additionalProperties": false
            }),
            observation_buffer(),
            AgentLiveSessionReconnectPolicy {
                mode: AgentLiveSessionReconnectMode::Transient,
                max_attempts: 32,
                initial_backoff_ms: 250,
                max_backoff_ms: 10_000,
                resume: AgentLiveSessionResumeMode::ExactCursor,
                cursor_kind: Some(AgentLiveSessionCursorKind::ResourceVersion),
            },
            AgentLiveSessionCloseEffect::StopObservation,
            CapabilityRiskLevel::ReadOnly,
        ),
        K8sLiveFamily::Logs => (
            PluginSessionPurpose::LogStream,
            AgentLiveSessionKind::Log,
            "kubernetes_pod_container",
            pod_container_identity_schema(),
            vec!["/namespace".into(), "/pod".into(), "/container".into()],
            json!({
                "type": "object",
                "properties": {
                    "tail": { "type": "integer", "minimum": 1, "maximum": MAX_LOG_TAIL, "default": DEFAULT_LOG_TAIL },
                    "previous": { "type": "boolean", "default": false },
                    "timestamps": { "type": "boolean", "default": true }
                },
                "additionalProperties": false
            }),
            observation_buffer(),
            AgentLiveSessionReconnectPolicy {
                mode: AgentLiveSessionReconnectMode::Transient,
                max_attempts: 8,
                initial_backoff_ms: 250,
                max_backoff_ms: 10_000,
                resume: AgentLiveSessionResumeMode::BestEffortCursor,
                cursor_kind: Some(AgentLiveSessionCursorKind::Timestamp),
            },
            AgentLiveSessionCloseEffect::StopObservation,
            CapabilityRiskLevel::ReadOnly,
        ),
        K8sLiveFamily::Exec => (
            PluginSessionPurpose::InteractiveTerminal,
            AgentLiveSessionKind::Exec,
            "kubernetes_pod_container",
            pod_container_identity_schema(),
            vec!["/namespace".into(), "/pod".into(), "/container".into()],
            exec_parameters_schema(),
            AgentLiveSessionBufferPolicy {
                max_events: 1_000,
                max_bytes: 1024 * 1024,
                overflow: AgentLiveSessionBufferOverflow::DropOldest,
            },
            AgentLiveSessionReconnectPolicy::default(),
            AgentLiveSessionCloseEffect::TerminateRemote,
            CapabilityRiskLevel::ExternalSideEffect,
        ),
        K8sLiveFamily::PortForward => (
            PluginSessionPurpose::PortForward,
            AgentLiveSessionKind::PortForward,
            "kubernetes_pod",
            json!({
                "type": "object",
                "required": ["namespace", "pod"],
                "properties": {
                    "namespace": { "type": "string", "minLength": 1, "maxLength": 253 },
                    "pod": { "type": "string", "minLength": 1, "maxLength": 253 }
                },
                "additionalProperties": false
            }),
            vec!["/namespace".into(), "/pod".into()],
            json!({
                "type": "object",
                "required": ["remote_port"],
                "properties": {
                    "remote_port": { "type": "integer", "minimum": 1, "maximum": 65535 },
                    "local_port": { "type": "integer", "minimum": 0, "maximum": 65535, "default": 0 }
                },
                "additionalProperties": false
            }),
            AgentLiveSessionBufferPolicy {
                max_events: 128,
                max_bytes: 512 * 1024,
                overflow: AgentLiveSessionBufferOverflow::DropOldest,
            },
            AgentLiveSessionReconnectPolicy::default(),
            AgentLiveSessionCloseEffect::DetachRemote,
            CapabilityRiskLevel::ExternalSideEffect,
        ),
    };
    let capabilities = family.capabilities();
    Some((
        purpose,
        AgentLiveSessionContract {
            protocol_version: AGENT_LIVE_SESSION_PROTOCOL_VERSION,
            kind,
            resource: AgentLiveSessionResourceDescriptor {
                resource_type: resource_type.into(),
                identity_schema,
                identity_fields,
                audit_identity: AgentLiveSessionAuditIdentity::Fingerprint,
            },
            start_parameters_schema: parameters,
            event_schema: json!({ "type": "object", "maxProperties": 20 }),
            operations: AgentLiveSessionOperations {
                events: capabilities[0].into(),
                input: capabilities.get(1).map(|capability| (*capability).into()),
                resize: capabilities.get(2).map(|capability| (*capability).into()),
                signal: None,
            },
            buffer,
            reconnect,
            delivery: Default::default(),
            control: AgentLiveSessionControlPolicy {
                cancel: AgentLiveSessionCancelBehavior::CallOnly,
                close,
            },
            start_risk,
        },
    ))
}

fn pod_container_identity_schema() -> Value {
    json!({
        "type": "object",
        "required": ["namespace", "pod", "container"],
        "properties": {
            "namespace": { "type": "string", "minLength": 1, "maxLength": 253 },
            "pod": { "type": "string", "minLength": 1, "maxLength": 253 },
            "container": { "type": "string", "minLength": 1, "maxLength": 253 }
        },
        "additionalProperties": false
    })
}

fn exec_parameters_schema() -> Value {
    json!({
        "type": "object",
        "required": ["command"],
        "properties": {
            "command": {
                "type": "array",
                "minItems": 1,
                "maxItems": 64,
                "items": { "type": "string", "minLength": 1, "maxLength": 4096 }
            },
            "tty": { "type": "boolean", "default": true },
            "cols": { "type": "integer", "minimum": MIN_COLS, "maximum": MAX_COLS, "default": DEFAULT_COLS },
            "rows": { "type": "integer", "minimum": MIN_ROWS, "maximum": MAX_ROWS, "default": DEFAULT_ROWS }
        },
        "additionalProperties": false
    })
}

fn observation_buffer() -> AgentLiveSessionBufferPolicy {
    AgentLiveSessionBufferPolicy {
        max_events: 2_000,
        max_bytes: 2 * 1024 * 1024,
        overflow: AgentLiveSessionBufferOverflow::DropOldest,
    }
}

async fn open_exec(
    service: &K8sAgentService,
    start: &AgentLiveSessionStartRequest,
) -> Result<crate::service::agent_live::K8sTerminalParts, PluginSessionError> {
    let namespace = required_string(&start.resource, "namespace")?;
    let pod = required_string(&start.resource, "pod")?;
    let container = required_string(&start.resource, "container")?;
    let command = required_string_array(&start.parameters, "command")?;
    let tty = optional_bool(&start.parameters, "tty", true)?;
    let cols = terminal_dimension(&start.parameters, "cols", Some(DEFAULT_COLS))?;
    let rows = terminal_dimension(&start.parameters, "rows", Some(DEFAULT_ROWS))?;
    service
        .open_exec(crate::service::agent_live::K8sExecSpec {
            namespace,
            pod,
            container,
            command,
            tty,
            cols,
            rows,
        })
        .await
        .map_err(|_| {
            session_error(
                PluginSessionErrorCode::OwnerUnavailable,
                "The controlled Kubernetes exec session could not be started.",
            )
        })
}

async fn open_port_forward(
    service: &K8sAgentService,
    start: &AgentLiveSessionStartRequest,
) -> Result<crate::service::agent_live::K8sPortForwardParts, PluginSessionError> {
    let namespace = required_string(&start.resource, "namespace")?;
    let pod = required_string(&start.resource, "pod")?;
    let remote_port = required_port(&start.parameters, "remote_port", false)?;
    let local_port = required_port(&start.parameters, "local_port", true)?;
    service
        .open_port_forward(namespace, pod, local_port, remote_port)
        .await
        .map_err(|error| {
            let message = if error
                .to_string()
                .to_ascii_lowercase()
                .contains("address already in use")
            {
                "The requested Kubernetes local forwarding port is already in use."
            } else {
                "The Kubernetes port-forward listener could not be started."
            };
            session_error(PluginSessionErrorCode::OwnerUnavailable, message)
        })
}

async fn run_watch(
    service: Arc<K8sAgentService>,
    start: AgentLiveSessionStartRequest,
    buffer: AgentLiveSessionEventBuffer,
    redaction_targets: Arc<Vec<RedactionTarget>>,
) {
    let Ok(api_version) = required_string(&start.resource, "api_version") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let Ok(kind) = required_string(&start.resource, "kind") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let Ok(plural) = required_string(&start.resource, "plural") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let namespace = optional_string(&start.resource, "namespace").unwrap_or_default();
    let label_selector = optional_string(&start.parameters, "label_selector").unwrap_or_default();
    let field_selector = optional_string(&start.parameters, "field_selector").unwrap_or_default();
    let spec = K8sWatchSpec {
        api_version,
        kind,
        plural,
        namespace,
        label_selector,
        field_selector,
    };
    let mut version = start
        .resume_from
        .as_ref()
        .map(|cursor| cursor.value.clone())
        .unwrap_or_else(|| "0".into());
    loop {
        let mut pump = service.watch(spec.clone(), version.clone());
        let mut reconnect_class = None;
        while let Some(item) = pump.next().await {
            match item {
                Ok(K8sWatchEvent::Applied {
                    event_type,
                    api_version,
                    kind,
                    name,
                    namespace,
                    resource_version,
                    generation,
                    label_keys,
                    annotation_keys,
                }) => {
                    if let Some(resource_version) = &resource_version {
                        version.clone_from(resource_version);
                    }
                    let (data, redaction) = redacted_json(
                        json!({
                            "event_type": event_type,
                            "api_version": api_version,
                            "kind": kind,
                            "name": name,
                            "namespace": namespace,
                            "resource_version": resource_version,
                            "generation": generation,
                            "label_keys": label_keys,
                            "annotation_keys": annotation_keys
                        }),
                        &redaction_targets,
                    );
                    if buffer
                        .push(
                            chrono::Utc::now(),
                            AgentLiveSessionEventKind::Data,
                            data,
                            Some(AgentLiveSessionCursor {
                                kind: AgentLiveSessionCursorKind::ResourceVersion,
                                value: version.clone(),
                                scope: None,
                            }),
                            redaction,
                            false,
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(K8sWatchEvent::Bookmark { resource_version }) => {
                    version = resource_version;
                    if buffer
                        .push(
                            chrono::Utc::now(),
                            AgentLiveSessionEventKind::Progress,
                            json!({ "state": "bookmark" }),
                            Some(AgentLiveSessionCursor {
                                kind: AgentLiveSessionCursorKind::ResourceVersion,
                                value: version.clone(),
                                scope: None,
                            }),
                            RedactionStatus::NotRequired,
                            false,
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Ok(K8sWatchEvent::Error { code: 410, .. }) => {
                    version = "0".into();
                    let _ = buffer
                        .push(
                            chrono::Utc::now(),
                            AgentLiveSessionEventKind::State,
                            json!({ "state": "resume_rejected", "reason": "resource_version_expired" }),
                            None,
                            RedactionStatus::NotRequired,
                            false,
                        )
                        .await;
                    reconnect_class = Some("resource_version_expired");
                    break;
                }
                Ok(K8sWatchEvent::Error { code, reason }) => {
                    let class = watch_error_class(code, reason.as_deref());
                    if matches!(class, "authentication" | "rbac_denied" | "not_found") {
                        push_terminal_error(&buffer, class).await;
                        return;
                    }
                    reconnect_class = Some(class);
                    break;
                }
                Err(error) => {
                    reconnect_class = Some(target_error_class(&error));
                    break;
                }
            }
        }
        drop(pump);
        let class = reconnect_class.unwrap_or("watch_timeout");
        if !retry_after_class(&buffer, class).await {
            return;
        }
    }
}

async fn run_logs(
    service: Arc<K8sAgentService>,
    start: AgentLiveSessionStartRequest,
    buffer: AgentLiveSessionEventBuffer,
    redaction_targets: Arc<Vec<RedactionTarget>>,
) {
    let Ok(namespace) = required_string(&start.resource, "namespace") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let Ok(pod) = required_string(&start.resource, "pod") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let Ok(container) = required_string(&start.resource, "container") else {
        push_terminal_error(&buffer, "invalid_resource").await;
        return;
    };
    let tail = start
        .parameters
        .get("tail")
        .and_then(Value::as_i64)
        .unwrap_or(DEFAULT_LOG_TAIL)
        .clamp(1, MAX_LOG_TAIL);
    let previous = start
        .parameters
        .get("previous")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let timestamps = start
        .parameters
        .get("timestamps")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let mut since_time = start
        .resume_from
        .as_ref()
        .and_then(|cursor| chrono::DateTime::parse_from_rfc3339(&cursor.value).ok())
        .map(|timestamp| timestamp.with_timezone(&chrono::Utc));
    let mut initial = true;
    loop {
        let mut pump = service.logs(K8sLogSpec {
            namespace: namespace.clone(),
            pod: pod.clone(),
            container: container.clone(),
            previous,
            since_time,
            tail_lines: initial.then_some(tail),
            timestamps,
        });
        initial = false;
        let mut retry = None;
        while let Some(item) = pump.next().await {
            match item {
                Ok(bytes) => {
                    let observed = chrono::Utc::now();
                    let text = String::from_utf8_lossy(&bytes);
                    for text in split_text(&text, MAX_EVENT_TEXT_BYTES) {
                        let source_bytes = text.len();
                        let (text, redaction) = redact_text_with_targets(&text, &redaction_targets);
                        if buffer
                            .push(
                                observed,
                                AgentLiveSessionEventKind::Data,
                                json!({ "text": text, "source_bytes": source_bytes }),
                                Some(AgentLiveSessionCursor {
                                    kind: AgentLiveSessionCursorKind::Timestamp,
                                    value: observed.to_rfc3339(),
                                    scope: None,
                                }),
                                redaction,
                                false,
                            )
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    since_time = Some(observed);
                }
                Err(error) => {
                    retry = Some(target_error_class(&error));
                    break;
                }
            }
        }
        drop(pump);
        if let Some(class) = retry {
            if !retry_after_class(&buffer, class).await {
                return;
            }
        } else {
            push_terminal_end(&buffer, "source_closed").await;
            return;
        }
    }
}

async fn run_exec_output(
    mut output: K8sTerminalOutput,
    buffer: AgentLiveSessionEventBuffer,
    redaction_targets: Arc<Vec<RedactionTarget>>,
) {
    while let Some(item) = output.next().await {
        match item {
            Ok(chunk) => {
                let text = String::from_utf8_lossy(&chunk.bytes);
                for text in split_text(&text, MAX_EVENT_TEXT_BYTES) {
                    let source_bytes = text.len();
                    let (text, redaction) = redact_text_with_targets(&text, &redaction_targets);
                    if buffer
                        .push(
                            chrono::Utc::now(),
                            AgentLiveSessionEventKind::Data,
                            json!({
                                "stream": chunk.stream,
                                "text": text,
                                "source_bytes": source_bytes
                            }),
                            None,
                            redaction,
                            false,
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
            Err(error) => {
                push_terminal_error(&buffer, target_error_class(&error)).await;
                return;
            }
        }
    }
    push_terminal_end(&buffer, "process_exited").await;
}

async fn run_port_forward_events(
    mut events: tokio::sync::mpsc::Receiver<K8sPortForwardEvent>,
    buffer: AgentLiveSessionEventBuffer,
) {
    while let Some(event) = events.recv().await {
        let (kind, data) = match event {
            K8sPortForwardEvent::Listening { local_port } => (
                AgentLiveSessionEventKind::State,
                json!({ "state": "listening", "local_host": "127.0.0.1", "local_port": local_port }),
            ),
            K8sPortForwardEvent::ConnectionOpened => (
                AgentLiveSessionEventKind::State,
                json!({ "state": "connection_opened" }),
            ),
            K8sPortForwardEvent::ConnectionClosed {
                to_remote,
                to_local,
            } => (
                AgentLiveSessionEventKind::Progress,
                json!({ "state": "connection_closed", "to_remote_bytes": to_remote, "to_local_bytes": to_local }),
            ),
            K8sPortForwardEvent::Error { class } => (
                AgentLiveSessionEventKind::Warning,
                json!({ "state": "connection_error", "error_class": class }),
            ),
        };
        if buffer
            .push(
                chrono::Utc::now(),
                kind,
                data,
                None,
                RedactionStatus::NotRequired,
                false,
            )
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn retry_after_class(buffer: &AgentLiveSessionEventBuffer, class: &str) -> bool {
    if matches!(class, "authentication" | "rbac_denied" | "not_found") {
        push_terminal_error(buffer, class).await;
        return false;
    }
    let attempt = match buffer.record_reconnect().await {
        Ok(attempt) => attempt,
        Err(_) => {
            push_terminal_error(buffer, "retry_exhausted").await;
            return false;
        }
    };
    if buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::Warning,
            json!({ "state": "reconnecting", "error_class": class, "attempt": attempt }),
            None,
            RedactionStatus::NotRequired,
            false,
        )
        .await
        .is_err()
    {
        return false;
    }
    let backoff = 250u64
        .saturating_mul(1u64 << attempt.saturating_sub(1).min(5))
        .min(10_000);
    tokio::time::sleep(Duration::from_millis(backoff)).await;
    true
}

async fn push_terminal_error(buffer: &AgentLiveSessionEventBuffer, class: &str) {
    let _ = buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::Error,
            json!({ "state": "failed", "error_class": class }),
            None,
            RedactionStatus::NotRequired,
            true,
        )
        .await;
}

async fn push_terminal_end(buffer: &AgentLiveSessionEventBuffer, state: &str) {
    let _ = buffer
        .push(
            chrono::Utc::now(),
            AgentLiveSessionEventKind::End,
            json!({ "state": state }),
            None,
            RedactionStatus::NotRequired,
            true,
        )
        .await;
}

fn target_error_class(error: &anyhow::Error) -> &'static str {
    watch_error_class(0, Some(&error.to_string()))
}

fn watch_error_class(code: u16, reason: Option<&str>) -> &'static str {
    let reason = reason.unwrap_or("").to_ascii_lowercase();
    if code == 401 || reason.contains("unauthorized") {
        "authentication"
    } else if code == 403 || reason.contains("forbidden") {
        "rbac_denied"
    } else if code == 404 || reason.contains("not found") {
        "not_found"
    } else if reason.contains("timeout") || reason.contains("timed out") {
        "timeout"
    } else {
        "transport"
    }
}

fn redacted_json(value: Value, targets: &[RedactionTarget]) -> (Value, RedactionStatus) {
    let Ok(encoded) = serde_json::to_string(&value) else {
        return (Value::Null, RedactionStatus::FailedClosed);
    };
    let (redacted, status) = redact_text_with_targets(&encoded, targets);
    match serde_json::from_str(&redacted) {
        Ok(value) => (value, status),
        Err(_) => (Value::Null, RedactionStatus::FailedClosed),
    }
}

fn terminal_input(input: &Value) -> Result<Vec<u8>, PluginSessionError> {
    let text = input.get("text").and_then(Value::as_str).unwrap_or("");
    if text
        .chars()
        .any(|character| character.is_control() && !matches!(character, '\n' | '\r' | '\t'))
    {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            "Kubernetes exec text contains unsupported control characters.",
        ));
    }
    let enter = input.get("enter").and_then(Value::as_bool).unwrap_or(false);
    let mut bytes = text.as_bytes().to_vec();
    if enter {
        bytes.push(b'\r');
    }
    if bytes.is_empty() || bytes.len() > MAX_TERMINAL_WRITE_BYTES {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Kubernetes exec input must contain 1 to {MAX_TERMINAL_WRITE_BYTES} bytes."),
        ));
    }
    Ok(bytes)
}

fn required_string(value: &Value, key: &str) -> Result<String, PluginSessionError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::PolicyDenied,
                format!("Kubernetes live-session field '{key}' is required."),
            )
        })
}

fn optional_string(value: &Value, key: &str) -> Result<Option<String>, PluginSessionError> {
    value
        .get(key)
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_string)
                .ok_or_else(|| {
                    session_error(
                        PluginSessionErrorCode::PolicyDenied,
                        format!(
                            "Kubernetes live-session field '{key}' must be a non-empty string."
                        ),
                    )
                })
        })
        .transpose()
}

fn required_string_array(value: &Value, key: &str) -> Result<Vec<String>, PluginSessionError> {
    let values = value.get(key).and_then(Value::as_array).ok_or_else(|| {
        session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Kubernetes live-session field '{key}' must be a string array."),
        )
    })?;
    if values.is_empty() || values.len() > 64 {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            "Kubernetes exec command must contain 1 to 64 arguments.",
        ));
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.is_empty() && value.len() <= 4096)
                .map(str::to_string)
                .ok_or_else(|| {
                    session_error(
                        PluginSessionErrorCode::PolicyDenied,
                        "Kubernetes exec command arguments must be non-empty bounded strings.",
                    )
                })
        })
        .collect()
}

fn optional_bool(value: &Value, key: &str, default: bool) -> Result<bool, PluginSessionError> {
    value
        .get(key)
        .map(|value| {
            value.as_bool().ok_or_else(|| {
                session_error(
                    PluginSessionErrorCode::PolicyDenied,
                    format!("Kubernetes live-session field '{key}' must be a boolean."),
                )
            })
        })
        .unwrap_or(Ok(default))
}

fn terminal_dimension(
    value: &Value,
    key: &str,
    default: Option<u16>,
) -> Result<u16, PluginSessionError> {
    let raw = value
        .get(key)
        .and_then(Value::as_u64)
        .or(default.map(u64::from));
    let raw = raw.ok_or_else(|| {
        session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Kubernetes terminal field '{key}' is required."),
        )
    })?;
    let (minimum, maximum) = if key == "cols" {
        (MIN_COLS, MAX_COLS)
    } else {
        (MIN_ROWS, MAX_ROWS)
    };
    if raw < u64::from(minimum) || raw > u64::from(maximum) {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Kubernetes terminal field '{key}' must be between {minimum} and {maximum}."),
        ));
    }
    Ok(raw as u16)
}

fn required_port(value: &Value, key: &str, allow_zero: bool) -> Result<u16, PluginSessionError> {
    let raw = value
        .get(key)
        .and_then(Value::as_u64)
        .or_else(|| (allow_zero && key == "local_port").then_some(0))
        .ok_or_else(|| {
            session_error(
                PluginSessionErrorCode::PolicyDenied,
                format!("Kubernetes port-forward field '{key}' is required."),
            )
        })?;
    let minimum = if allow_zero { 0 } else { 1 };
    if raw < minimum || raw > u64::from(u16::MAX) {
        return Err(session_error(
            PluginSessionErrorCode::PolicyDenied,
            format!("Kubernetes port-forward field '{key}' is outside the valid port range."),
        ));
    }
    Ok(raw as u16)
}

fn split_text(text: &str, max_bytes: usize) -> Vec<String> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if !current.is_empty() && current.len().saturating_add(character.len_utf8()) > max_bytes {
            chunks.push(std::mem::take(&mut current));
        }
        current.push(character);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

fn session_error(code: PluginSessionErrorCode, message: impl Into<String>) -> PluginSessionError {
    PluginSessionError::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_contracts_cover_watch_logs_exec_and_port_forward() {
        for capability in [
            WATCH_EVENTS_CAPABILITY,
            LOGS_FOLLOW_CAPABILITY,
            EXEC_READ_CAPABILITY,
            PORT_FORWARD_EVENTS_CAPABILITY,
        ] {
            let (_, contract) =
                kubernetes_live_session_contract(capability).expect("live contract");
            let family = if capability == EXEC_READ_CAPABILITY {
                EXEC_CAPABILITIES
            } else {
                &[capability]
            };
            contract
                .validate(
                    &family
                        .iter()
                        .map(|value| (*value).into())
                        .collect::<Vec<_>>(),
                )
                .expect("valid contract");
        }
        let (_, watch) = kubernetes_live_session_contract(WATCH_EVENTS_CAPABILITY).unwrap();
        assert_eq!(
            watch.reconnect.resume,
            AgentLiveSessionResumeMode::ExactCursor
        );
        let (_, exec) = kubernetes_live_session_contract(EXEC_READ_CAPABILITY).unwrap();
        assert_eq!(exec.start_risk, CapabilityRiskLevel::ExternalSideEffect);
    }

    #[test]
    fn watch_errors_classify_rbac_and_expired_versions_separately() {
        assert_eq!(watch_error_class(403, Some("Forbidden")), "rbac_denied");
        assert_eq!(watch_error_class(410, Some("Gone")), "transport");
    }

    #[test]
    fn terminal_input_and_ports_are_bounded() {
        assert_eq!(
            terminal_input(&json!({ "text": "echo ok", "enter": true })).unwrap(),
            b"echo ok\r"
        );
        assert!(terminal_input(&json!({ "text": "bad\u{0000}" })).is_err());
        assert_eq!(
            required_port(&json!({ "remote_port": 443 }), "remote_port", false).unwrap(),
            443
        );
        assert!(required_port(&json!({ "remote_port": 0 }), "remote_port", false).is_err());
    }

    #[test]
    fn factory_redaction_targets_withhold_profile_secrets() {
        let secret = "kubernetes-conformance-token";
        let factory = K8sAgentSessionFactory::new(K8sConfig {
            connection: crate::config::K8sConnection::Direct {
                api_url: "https://kubernetes.example.test".into(),
                auth: crate::config::K8sAuth::Token {
                    token: secret.into(),
                },
                verify_ssl: true,
                ca_cert: None,
            },
            default_namespace: Some("default".into()),
            timeout: 1,
        });
        let (redacted, status) = redact_text_with_targets(
            &format!("target returned {secret}"),
            factory.redaction_targets.as_ref(),
        );
        assert_eq!(status, RedactionStatus::Applied);
        assert!(!redacted.contains(secret));
    }
}
