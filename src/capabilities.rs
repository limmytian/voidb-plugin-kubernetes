#![allow(clippy::result_large_err)]

use serde_json::{Value, json};
use tokio::sync::mpsc;
use voidb_core::{
    CapabilityDefinition, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, CapabilityRiskLevel, CredentialClass, InvocationOutputPage,
    InvocationStatus, Pagination, RedactionStatus, TargetSystemFailure,
};

use crate::agent_session::{
    EXEC_INPUT_CAPABILITY, EXEC_READ_CAPABILITY, EXEC_RESIZE_CAPABILITY, LOGS_FOLLOW_CAPABILITY,
    PORT_FORWARD_EVENTS_CAPABILITY, WATCH_EVENTS_CAPABILITY, kubernetes_live_session_contract,
};
use crate::config::{K8sAuth, K8sConfig, K8sConnection};
use crate::service::K8sService;
use crate::types::{
    ConfigMapInfo, CronJobInfo, DaemonSetInfo, DeploymentInfo, EventInfo, IngressInfo, JobInfo,
    K8sEvent, NodeInfo, PodInfo, PvcInfo, SecretInfo, ServiceAccountInfo, ServiceInfo,
    StatefulSetInfo,
};

const PLUGIN_ID: &str = "kubernetes";
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_PAGE_LIMIT: usize = 100;
const MAX_PAGE_LIMIT: usize = 500;
const DEFAULT_LOG_TAIL: i64 = 100;
const MAX_LOG_TAIL: i64 = 5_000;
const DEFAULT_TEXT_LIMIT_BYTES: usize = 64 * 1024;
const MAX_TEXT_LIMIT_BYTES: usize = 1024 * 1024;
const SUPPORTED_RESOURCE_TYPES: &[&str] = &[
    "pods",
    "services",
    "deployments",
    "statefulsets",
    "daemonsets",
    "jobs",
    "cronjobs",
    "pvcs",
    "ingresses",
    "serviceaccounts",
    "configmaps",
    "secrets",
    "nodes",
    "events",
];

pub fn kubernetes_capabilities() -> Vec<CapabilityDefinition> {
    vec![
        capability(
            "diagnostics",
            "Return agent-safe Kubernetes profile diagnostics without opening a cluster connection.",
            empty_input_schema(),
            json!({
                "type": "object",
                "required": [
                    "connection_type",
                    "auth_type",
                    "default_namespace",
                    "timeout_secs",
                    "network_checked"
                ],
                "properties": {
                    "connection_type": { "type": "string" },
                    "auth_type": { "type": ["string", "null"] },
                    "default_namespace": { "type": "string" },
                    "timeout_secs": { "type": "integer", "minimum": 0 },
                    "network_checked": { "type": "boolean" },
                    "kubeconfig_path_present": { "type": "boolean" },
                    "context_present": { "type": "boolean" },
                    "verify_ssl": { "type": ["boolean", "null"] }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "kubernetes.diagnostics"],
            false,
            false,
            false,
        ),
        capability(
            "contexts",
            "List kubeconfig contexts without connecting to the cluster.",
            empty_input_schema(),
            json!({
                "type": "object",
                "required": ["contexts", "context_count"],
                "properties": {
                    "contexts": { "type": "array", "items": { "type": "string" } },
                    "context_count": { "type": "integer", "minimum": 0 }
                },
                "additionalProperties": false
            }),
            vec!["connection.read", "kubernetes.contexts"],
            false,
            false,
            false,
        ),
        capability(
            "namespaces",
            "List Kubernetes namespaces.",
            empty_input_schema(),
            kubernetes_list_schema("namespaces", namespace_schema()),
            vec!["connection.read", "kubernetes.namespaces"],
            false,
            false,
            false,
        ),
        capability(
            "list",
            "List Kubernetes resources with bounded, cursor-based output.",
            json!({
                "type": "object",
                "required": ["resource_type"],
                "properties": {
                    "resource_type": {
                        "type": "string",
                        "enum": SUPPORTED_RESOURCE_TYPES
                    },
                    "namespace": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            kubernetes_list_schema("items", resource_item_schema()),
            vec!["connection.read", "kubernetes.resources.list"],
            false,
            false,
            false,
        ),
        capability(
            "get_yaml",
            "Get one Kubernetes resource as bounded YAML; Secret payloads are not exposed.",
            json!({
                "type": "object",
                "required": ["resource_type", "name"],
                "properties": {
                    "resource_type": { "type": "string", "minLength": 1 },
                    "name": { "type": "string", "minLength": 1 },
                    "namespace": { "type": "string", "minLength": 1 },
                    "max_bytes": text_limit_schema()
                },
                "additionalProperties": false
            }),
            yaml_output_schema(),
            vec!["connection.read", "kubernetes.resources.get"],
            false,
            false,
            false,
        ),
        capability(
            "logs",
            "Read bounded pod logs without following the stream.",
            json!({
                "type": "object",
                "required": ["pod"],
                "properties": {
                    "pod": { "type": "string", "minLength": 1 },
                    "namespace": { "type": "string", "minLength": 1 },
                    "container": { "type": "string", "minLength": 1 },
                    "tail": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_LOG_TAIL,
                        "default": DEFAULT_LOG_TAIL
                    },
                    "max_bytes": text_limit_schema()
                },
                "additionalProperties": false
            }),
            log_output_schema(),
            vec!["connection.read", "kubernetes.pods.logs"],
            false,
            false,
            false,
        ),
        live_capability(
            WATCH_EVENTS_CAPABILITY,
            "Watch one namespaced or cluster-scoped Kubernetes API resource with resource-version continuation.",
            vec!["connection.read", "kubernetes.resources.watch"],
        ),
        live_capability(
            LOGS_FOLLOW_CAPABILITY,
            "Follow one explicit pod container log stream with bounded retention.",
            vec!["connection.read", "kubernetes.pods.logs"],
        ),
        live_capability(
            EXEC_READ_CAPABILITY,
            "Read output from one explicitly configured Kubernetes pod exec.",
            vec!["connection.read", "kubernetes.pods.exec"],
        ),
        live_capability(
            EXEC_INPUT_CAPABILITY,
            "Write bounded text to one controlled Kubernetes pod exec.",
            vec!["connection.write", "kubernetes.pods.exec"],
        ),
        live_capability(
            EXEC_RESIZE_CAPABILITY,
            "Resize one controlled Kubernetes pod exec TTY.",
            vec!["connection.write", "kubernetes.pods.exec"],
        ),
        live_capability(
            PORT_FORWARD_EVENTS_CAPABILITY,
            "Open one loopback-only pod port forward and report bounded connection state.",
            vec!["connection.write", "kubernetes.pods.port_forward"],
        ),
        capability(
            "delete",
            "Delete one namespaced Kubernetes resource.",
            json!({
                "type": "object",
                "required": ["resource_type", "name"],
                "properties": {
                    "resource_type": { "type": "string", "minLength": 1 },
                    "name": { "type": "string", "minLength": 1 },
                    "namespace": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "kubernetes.resources.delete"],
            true,
            false,
            true,
        ),
        capability(
            "scale",
            "Scale a Kubernetes deployment to a requested replica count.",
            json!({
                "type": "object",
                "required": ["name", "replicas"],
                "properties": {
                    "name": { "type": "string", "minLength": 1 },
                    "namespace": { "type": "string", "minLength": 1 },
                    "replicas": { "type": "integer", "minimum": 0, "maximum": 10_000 }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "kubernetes.deployments.scale"],
            true,
            false,
            true,
        ),
        capability(
            "restart",
            "Trigger a Kubernetes deployment rollout restart.",
            json!({
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": { "type": "string", "minLength": 1 },
                    "namespace": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "kubernetes.deployments.restart"],
            true,
            false,
            true,
        ),
        capability(
            "apply",
            "Apply one Kubernetes YAML manifest.",
            json!({
                "type": "object",
                "required": ["yaml"],
                "properties": {
                    "yaml": { "type": "string", "minLength": 1 },
                    "namespace": { "type": "string", "minLength": 1 }
                },
                "additionalProperties": false
            }),
            mutation_output_schema(),
            vec!["connection.write", "kubernetes.resources.apply"],
            true,
            false,
            true,
        ),
    ]
}

pub async fn invoke_kubernetes_capability(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    if invocation.plugin_id != PLUGIN_ID {
        return Err(validation_error(
            "validation.plugin_mismatch",
            "Invocation plugin_id does not match Kubernetes.",
            json!({ "expected": PLUGIN_ID, "actual": invocation.plugin_id }),
        ));
    }

    match invocation.capability_id.as_str() {
        "diagnostics" => Ok(diagnostics_result(config, invocation.id)),
        "contexts" => invoke_contexts(config, invocation).await,
        "namespaces" => invoke_namespaces(config, invocation).await,
        "list" => invoke_list(config, invocation).await,
        "get_yaml" => invoke_get_yaml(config, invocation).await,
        "logs" => invoke_logs(config, invocation).await,
        "watch_events"
        | "logs_follow"
        | "exec_read"
        | "exec_input"
        | "exec_resize"
        | "port_forward_events" => Err(unavailable_error(
            "unavailable.session_required",
            "This Kubernetes live workflow requires a persistent agent session.",
            json!({ "capability_id": invocation.capability_id }),
        )),
        "delete" => invoke_delete(config, invocation).await,
        "scale" => invoke_scale(config, invocation).await,
        "restart" => invoke_restart(config, invocation).await,
        "apply" => invoke_apply(config, invocation).await,
        other => Err(unavailable_error(
            "unavailable.capability_not_found",
            "Kubernetes capability was not found.",
            json!({ "capability_id": other }),
        )),
    }
}

fn live_capability(
    qualified_id: &str,
    description: &str,
    permissions: Vec<&str>,
) -> CapabilityDefinition {
    let id = qualified_id
        .strip_prefix("kubernetes.")
        .expect("Kubernetes live capability ID");
    let (purpose, contract) =
        kubernetes_live_session_contract(qualified_id).expect("Kubernetes live contract");
    let handoff_capabilities = contract
        .operations
        .capabilities()
        .cloned()
        .collect::<Vec<_>>();
    let risk = match qualified_id {
        EXEC_INPUT_CAPABILITY | EXEC_RESIZE_CAPABILITY => CapabilityRiskLevel::ExternalSideEffect,
        _ => CapabilityRiskLevel::ReadOnly,
    };
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema: kubernetes_live_call_schema(qualified_id),
        output_schema: kubernetes_live_output_schema(qualified_id),
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: kubernetes_live_authorization(qualified_id, purpose.clone()),
        risk,
        destructive: false,
        streaming: matches!(
            qualified_id,
            WATCH_EVENTS_CAPABILITY
                | LOGS_FOLLOW_CAPABILITY
                | EXEC_READ_CAPABILITY
                | PORT_FORWARD_EVENTS_CAPABILITY
        ),
        execution_mode: voidb_core::CapabilityExecutionMode::SessionOnly,
        session_handoff: Some(
            voidb_core::CapabilitySessionHandoff::new(purpose, handoff_capabilities)
                .with_live_session(contract),
        ),
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run: false,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn kubernetes_live_authorization(
    capability: &str,
    purpose: voidb_core::PluginSessionPurpose,
) -> voidb_core::CapabilityAuthorizationMetadata {
    let resource = |path: &str, label: &str| {
        voidb_core::CapabilityApprovalField::new(
            path,
            label,
            voidb_core::CapabilityApprovalValueType::ResourceId,
        )
        .required()
    };
    let mut fields = match capability {
        WATCH_EVENTS_CAPABILITY => vec![
            resource("/resource/api_version", "API version"),
            resource("/resource/kind", "Resource kind"),
            resource("/resource/plural", "Resource plural"),
            resource("/resource/namespace", "Namespace"),
        ],
        LOGS_FOLLOW_CAPABILITY
        | EXEC_READ_CAPABILITY
        | EXEC_INPUT_CAPABILITY
        | EXEC_RESIZE_CAPABILITY => vec![
            resource("/resource/namespace", "Namespace"),
            resource("/resource/pod", "Pod"),
            resource("/resource/container", "Container"),
        ],
        PORT_FORWARD_EVENTS_CAPABILITY => vec![
            resource("/resource/namespace", "Namespace"),
            resource("/resource/pod", "Pod"),
        ],
        _ => Vec::new(),
    };
    if matches!(
        capability,
        EXEC_READ_CAPABILITY | EXEC_INPUT_CAPABILITY | EXEC_RESIZE_CAPABILITY
    ) {
        fields.push(
            voidb_core::CapabilityApprovalField::new(
                "/parameters/command",
                "Exec command",
                voidb_core::CapabilityApprovalValueType::CommandArgv,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        );
    }
    if capability == PORT_FORWARD_EVENTS_CAPABILITY {
        fields.extend([
            voidb_core::CapabilityApprovalField::new(
                "/parameters/remote_port",
                "Remote port",
                voidb_core::CapabilityApprovalValueType::Integer,
            )
            .required(),
            voidb_core::CapabilityApprovalField::new(
                "/parameters/local_port",
                "Local port",
                voidb_core::CapabilityApprovalValueType::Integer,
            ),
        ]);
    }
    let mut metadata = voidb_core::CapabilityAuthorizationMetadata::declared()
        .with_session_purposes(vec![purpose])
        .with_note(
            "Kubernetes namespace, resource, pod, container, command, and port scope are revalidated at session open.",
        )
        .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields));
    if matches!(
        capability,
        EXEC_READ_CAPABILITY
            | EXEC_INPUT_CAPABILITY
            | EXEC_RESIZE_CAPABILITY
            | PORT_FORWARD_EVENTS_CAPABILITY
    ) {
        metadata = metadata.with_interactive_execute();
    }
    metadata
}

fn kubernetes_live_call_schema(capability: &str) -> Value {
    match capability {
        EXEC_INPUT_CAPABILITY => json!({
            "type": "object",
            "properties": {
                "text": { "type": "string", "maxLength": 16384 },
                "enter": { "type": "boolean", "default": false }
            },
            "additionalProperties": false
        }),
        EXEC_RESIZE_CAPABILITY => json!({
            "type": "object",
            "required": ["cols", "rows"],
            "properties": {
                "cols": { "type": "integer", "minimum": 20, "maximum": 500 },
                "rows": { "type": "integer", "minimum": 5, "maximum": 200 }
            },
            "additionalProperties": false
        }),
        _ => live_read_schema(),
    }
}

fn kubernetes_live_output_schema(capability: &str) -> Value {
    match capability {
        EXEC_INPUT_CAPABILITY => json!({
            "type": "object",
            "required": ["written_bytes"],
            "properties": { "written_bytes": { "type": "integer", "minimum": 1 } },
            "additionalProperties": false
        }),
        EXEC_RESIZE_CAPABILITY => json!({
            "type": "object",
            "required": ["cols", "rows"],
            "properties": {
                "cols": { "type": "integer" },
                "rows": { "type": "integer" }
            },
            "additionalProperties": false
        }),
        _ => live_batch_schema(),
    }
}

fn live_read_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "after_sequence": { "type": "integer", "minimum": 0 },
            "max_events": { "type": "integer", "minimum": 1, "maximum": 1000 },
            "max_bytes": { "type": "integer", "minimum": 1, "maximum": 1048576 }
        },
        "additionalProperties": false
    })
}

fn live_batch_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "protocol_version",
            "events",
            "next_sequence",
            "source_closed",
            "dropped_events",
            "dropped_bytes",
            "coalesced_events",
            "reconnect_attempts"
        ],
        "properties": {
            "protocol_version": { "type": "integer", "const": 1 },
            "events": { "type": "array", "maxItems": 1000 },
            "next_sequence": { "type": "integer", "minimum": 1 },
            "resume_cursor": { "type": "object" },
            "source_closed": { "type": "boolean" },
            "dropped_events": { "type": "integer", "minimum": 0 },
            "dropped_bytes": { "type": "integer", "minimum": 0 },
            "coalesced_events": { "type": "integer", "minimum": 0 },
            "reconnect_attempts": { "type": "integer", "minimum": 0 }
        },
        "additionalProperties": false
    })
}

#[allow(clippy::too_many_arguments)]
fn capability(
    id: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    permissions: Vec<&str>,
    destructive: bool,
    streaming: bool,
    supports_dry_run: bool,
) -> CapabilityDefinition {
    CapabilityDefinition {
        plugin_id: PLUGIN_ID.to_string(),
        id: id.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        permissions: permissions.into_iter().map(str::to_string).collect(),
        authorization: kubernetes_authorization_metadata(id),
        risk: CapabilityRiskLevel::from_destructive(destructive),
        destructive,
        streaming,
        execution_mode: voidb_core::CapabilityExecutionMode::Stateless,
        session_handoff: None,
        connection_required: true,
        required_secret_classes: Vec::<CredentialClass>::new(),
        supports_dry_run,
        default_timeout_ms: Some(DEFAULT_TIMEOUT_MS),
    }
}

fn kubernetes_authorization_metadata(id: &str) -> voidb_core::CapabilityAuthorizationMetadata {
    let resource = |path: &str, label: &str| {
        voidb_core::CapabilityApprovalField::new(
            path,
            label,
            voidb_core::CapabilityApprovalValueType::ResourceId,
        )
    };
    let fields = match id {
        "list" => vec![
            resource("/resource_type", "Resource type").required(),
            resource("/namespace", "Namespace"),
        ],
        "get_yaml" | "delete" => vec![
            resource("/resource_type", "Resource type").required(),
            resource("/name", "Resource name").required(),
            resource("/namespace", "Namespace"),
        ],
        "logs" => vec![
            resource("/pod", "Pod").required(),
            resource("/namespace", "Namespace"),
            resource("/container", "Container"),
        ],
        "scale" => vec![
            resource("/name", "Deployment").required(),
            resource("/namespace", "Namespace"),
            voidb_core::CapabilityApprovalField::new(
                "/replicas",
                "Maximum replicas",
                voidb_core::CapabilityApprovalValueType::Integer,
            )
            .required()
            .with_constraint(voidb_core::CapabilityConstraintKind::Maximum)
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
        ],
        "restart" => vec![
            resource("/name", "Deployment").required(),
            resource("/namespace", "Namespace"),
        ],
        "apply" => vec![
            voidb_core::CapabilityApprovalField::new(
                "/yaml",
                "Manifest",
                voidb_core::CapabilityApprovalValueType::String,
            )
            .required()
            .with_risk_emphasis(voidb_core::CapabilityApprovalRiskEmphasis::PrivilegeEscalation),
            resource("/namespace", "Namespace"),
        ],
        _ => Vec::new(),
    };
    let metadata = voidb_core::CapabilityAuthorizationMetadata::declared();
    if fields.is_empty() {
        metadata
    } else {
        metadata
            .with_note(if id == "logs" {
                "Pod, namespace, and container are revalidated before bounded log access."
            } else {
                "Kubernetes resource, namespace, and operation fields are revalidated."
            })
            .with_approval_schema(voidb_core::CapabilityApprovalSchema::v1(fields))
    }
}

fn diagnostics_result(config: &K8sConfig, invocation_id: String) -> CapabilityInvocationResult {
    let output = match &config.connection {
        K8sConnection::Kubeconfig { path, context } => json!({
            "connection_type": "kubeconfig",
            "auth_type": Value::Null,
            "default_namespace": config.namespace(),
            "timeout_secs": config.timeout,
            "network_checked": false,
            "kubeconfig_path_present": path.as_deref().is_some_and(|value| !value.trim().is_empty()),
            "context_present": context.as_deref().is_some_and(|value| !value.trim().is_empty()),
            "verify_ssl": Value::Null,
        }),
        K8sConnection::Direct {
            auth, verify_ssl, ..
        } => json!({
            "connection_type": "direct",
            "auth_type": auth_type(auth),
            "default_namespace": config.namespace(),
            "timeout_secs": config.timeout,
            "network_checked": false,
            "kubeconfig_path_present": false,
            "context_present": false,
            "verify_ssl": verify_ssl,
        }),
    };
    result(invocation_id, output.clone(), output, None)
}

async fn invoke_contexts(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let contexts = K8sService::list_contexts_direct(config)
        .map_err(|error| target_error(config, "kubernetes.contexts_failed", error.to_string()))?;
    let output = json!({
        "contexts": contexts,
        "context_count": contexts.len(),
    });
    Ok(result(invocation.id, output.clone(), output, None))
}

async fn invoke_namespaces(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config).await?;
    let namespaces = service
        .list_namespaces_direct()
        .await
        .map_err(|error| target_error(config, "kubernetes.namespaces_failed", error.to_string()))?
        .into_iter()
        .map(|name| json!({ "name": name }))
        .collect::<Vec<_>>();

    Ok(paged_result(
        invocation.id,
        "namespaces",
        namespaces,
        page,
        Value::Null,
    ))
}

async fn invoke_list(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let resource_type =
        normalize_resource_type(&required_string(&invocation.input, "resource_type")?)?;
    let namespace = namespace_input(config, &invocation.input)?;
    let page = page_request(invocation.controls.page.as_ref())?;
    let service = service(config).await?;
    let items = list_resource(config, &service, resource_type, &namespace).await?;

    Ok(paged_result(
        invocation.id,
        "items",
        items,
        page,
        json!({
            "resource_type": resource_type,
            "namespace": if resource_type == "nodes" { Value::Null } else { json!(namespace) },
        }),
    ))
}

async fn invoke_get_yaml(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let resource_type_raw = required_string(&invocation.input, "resource_type")?;
    if is_secret_resource(&resource_type_raw) {
        return Err(policy_error(
            "policy.kubernetes_secret_payload_blocked",
            "Secret YAML payloads are not exposed through generic invoke.",
            json!({ "resource_type": resource_type_raw }),
        ));
    }
    let name = required_string(&invocation.input, "name")?;
    let namespace = namespace_input(config, &invocation.input)?;
    let max_bytes = requested_usize(
        &invocation.input,
        "max_bytes",
        DEFAULT_TEXT_LIMIT_BYTES,
        1,
        MAX_TEXT_LIMIT_BYTES,
    )?;
    let service = service(config).await?;
    let yaml = service
        .get_resource_yaml_direct(&resource_type_raw, &name, &namespace)
        .await
        .map_err(|error| target_error(config, "kubernetes.get_yaml_failed", error.to_string()))?;
    let bounded = bounded_text(&yaml, max_bytes);
    let output = json!({
        "resource_type": resource_type_raw,
        "name": name,
        "namespace": namespace,
        "yaml": bounded.value,
        "bytes_returned": bounded.value.len(),
        "source_bytes": bounded.source_bytes,
        "truncated": bounded.truncated,
        "byte_limit": max_bytes,
    });
    let summary = json!({
        "resource_type": output["resource_type"],
        "name": output["name"],
        "namespace": output["namespace"],
        "bytes_returned": output["bytes_returned"],
        "source_bytes": output["source_bytes"],
        "truncated": output["truncated"],
    });
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_logs(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let pod = required_string(&invocation.input, "pod")?;
    let namespace = namespace_input(config, &invocation.input)?;
    let container = optional_string(&invocation.input, "container")?;
    let tail = requested_i64(&invocation.input, "tail", DEFAULT_LOG_TAIL, 1, MAX_LOG_TAIL)?;
    let max_bytes = requested_usize(
        &invocation.input,
        "max_bytes",
        DEFAULT_TEXT_LIMIT_BYTES,
        1,
        MAX_TEXT_LIMIT_BYTES,
    )?;
    let service = service(config).await?;
    let mut rx = service
        .stream_logs_direct(
            pod.clone(),
            namespace.clone(),
            container.clone(),
            false,
            Some(tail),
        )
        .await;
    let logs = collect_log_output(config, &mut rx, max_bytes).await?;
    let output = json!({
        "pod": pod,
        "namespace": namespace,
        "container": container,
        "text": logs.value,
        "bytes_returned": logs.value.len(),
        "source_bytes": logs.source_bytes,
        "truncated": logs.truncated,
        "tail": tail,
        "byte_limit": max_bytes,
    });
    let summary = json!({
        "pod": output["pod"],
        "namespace": output["namespace"],
        "container": output["container"],
        "bytes_returned": output["bytes_returned"],
        "source_bytes": output["source_bytes"],
        "truncated": output["truncated"],
        "tail": tail,
    });
    Ok(result(invocation.id, output, summary, None))
}

async fn invoke_delete(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let resource_type = required_string(&invocation.input, "resource_type")?;
    let name = required_string(&invocation.input, "name")?;
    let namespace = namespace_input(config, &invocation.input)?;
    let details = json!({
        "resource_type": resource_type,
        "name": name,
        "namespace": namespace,
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "delete", details));
    }

    let service = service(config).await?;
    service
        .delete_resource_direct(
            details["resource_type"].as_str().unwrap_or_default(),
            details["name"].as_str().unwrap_or_default(),
            details["namespace"].as_str().unwrap_or_default(),
        )
        .await
        .map_err(|error| target_error(config, "kubernetes.delete_failed", error.to_string()))?;
    Ok(mutation_result(invocation.id, "delete", details))
}

async fn invoke_scale(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let name = required_string(&invocation.input, "name")?;
    let namespace = namespace_input(config, &invocation.input)?;
    let replicas = requested_u32(&invocation.input, "replicas", 0, 10_000)?;
    let details = json!({
        "resource_type": "deployment",
        "name": name,
        "namespace": namespace,
        "replicas": replicas,
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "scale", details));
    }

    let service = service(config).await?;
    service
        .scale_deployment_direct(
            details["name"].as_str().unwrap_or_default(),
            details["namespace"].as_str().unwrap_or_default(),
            replicas,
        )
        .await
        .map_err(|error| target_error(config, "kubernetes.scale_failed", error.to_string()))?;
    Ok(mutation_result(invocation.id, "scale", details))
}

async fn invoke_restart(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let name = required_string(&invocation.input, "name")?;
    let namespace = namespace_input(config, &invocation.input)?;
    let details = json!({
        "resource_type": "deployment",
        "name": name,
        "namespace": namespace,
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "restart", details));
    }

    let service = service(config).await?;
    service
        .restart_deployment_direct(
            details["name"].as_str().unwrap_or_default(),
            details["namespace"].as_str().unwrap_or_default(),
        )
        .await
        .map_err(|error| target_error(config, "kubernetes.restart_failed", error.to_string()))?;
    Ok(mutation_result(invocation.id, "restart", details))
}

async fn invoke_apply(
    config: &K8sConfig,
    invocation: CapabilityInvocation,
) -> Result<CapabilityInvocationResult, CapabilityError> {
    let yaml = required_string(&invocation.input, "yaml")?;
    let namespace = namespace_input(config, &invocation.input)?;
    let manifest = parse_manifest_summary(&yaml, &namespace)?;
    let details = json!({
        "api_version": manifest.api_version,
        "kind": manifest.kind,
        "name": manifest.name,
        "namespace": manifest.namespace,
    });

    if invocation.controls.dry_run {
        return Ok(dry_run_result(invocation.id, "apply", details));
    }

    let service = service(config).await?;
    let message = service
        .apply_yaml_direct(&yaml, &namespace)
        .await
        .map_err(|error| target_error(config, "kubernetes.apply_failed", error.to_string()))?;
    let mut output_details = details;
    output_details["message"] = json!(message);
    Ok(mutation_result(invocation.id, "apply", output_details))
}

async fn service(config: &K8sConfig) -> Result<K8sService, CapabilityError> {
    K8sService::new_direct(config)
        .await
        .map_err(|error| target_error(config, "kubernetes.connect_failed", error.to_string()))
}

async fn list_resource(
    config: &K8sConfig,
    service: &K8sService,
    resource_type: &str,
    namespace: &str,
) -> Result<Vec<Value>, CapabilityError> {
    match resource_type {
        "pods" => Ok(service
            .list_pods_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(pod_json)
            .collect()),
        "services" => Ok(service
            .list_services_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(service_json)
            .collect()),
        "deployments" => Ok(service
            .list_deployments_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(deployment_json)
            .collect()),
        "statefulsets" => Ok(service
            .list_statefulsets_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(statefulset_json)
            .collect()),
        "daemonsets" => Ok(service
            .list_daemonsets_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(daemonset_json)
            .collect()),
        "jobs" => Ok(service
            .list_jobs_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(job_json)
            .collect()),
        "cronjobs" => Ok(service
            .list_cronjobs_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(cronjob_json)
            .collect()),
        "pvcs" => Ok(service
            .list_pvcs_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(pvc_json)
            .collect()),
        "ingresses" => Ok(service
            .list_ingresses_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(ingress_json)
            .collect()),
        "serviceaccounts" => Ok(service
            .list_service_accounts_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(service_account_json)
            .collect()),
        "configmaps" => Ok(service
            .list_configmaps_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(configmap_json)
            .collect()),
        "secrets" => Ok(service
            .list_secrets_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(secret_json)
            .collect()),
        "nodes" => Ok(service
            .list_nodes_direct()
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(node_json)
            .collect()),
        "events" => Ok(service
            .list_events_direct(namespace)
            .await
            .map_err(|error| target_error(config, "kubernetes.list_failed", error.to_string()))?
            .iter()
            .map(event_json)
            .collect()),
        _ => Err(validation_error(
            "validation.kubernetes_resource_type_invalid",
            "Kubernetes resource type is not supported for list.",
            json!({
                "resource_type": resource_type,
                "supported_resource_types": SUPPORTED_RESOURCE_TYPES,
            }),
        )),
    }
}

fn paged_result(
    invocation_id: String,
    item_key: &str,
    items: Vec<Value>,
    page: PageRequest,
    metadata: Value,
) -> CapabilityInvocationResult {
    let source_count = items.len();
    let end = page.offset.saturating_add(page.limit).min(source_count);
    let page_items = if page.offset >= source_count {
        Vec::new()
    } else {
        items[page.offset..end].to_vec()
    };
    let next_cursor = (end < source_count).then(|| end.to_string());
    let item_count = page_items.len();
    let truncated = next_cursor.is_some();
    let output = json!({
        item_key: page_items,
        "item_count": item_count,
        "source_item_count": source_count,
        "limit": page.limit,
        "cursor": page.cursor,
        "next_cursor": next_cursor,
        "truncated": truncated,
        "metadata": metadata,
    });
    let summary = json!({
        "item_key": item_key,
        "item_count": item_count,
        "source_item_count": source_count,
        "truncated": truncated,
        "next_cursor": output["next_cursor"],
    });
    let output_page = truncated.then(|| InvocationOutputPage {
        next_cursor: output["next_cursor"].as_str().map(str::to_string),
    });
    result(invocation_id, output, summary, output_page)
}

fn dry_run_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": true,
        "would_execute": true,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": true }),
        None,
    )
}

fn mutation_result(
    invocation_id: String,
    operation: &str,
    details: Value,
) -> CapabilityInvocationResult {
    let output = json!({
        "ok": true,
        "operation": operation,
        "dry_run": false,
        "destructive": true,
        "details": details,
    });
    result(
        invocation_id,
        output,
        json!({ "operation": operation, "dry_run": false }),
        None,
    )
}

fn result(
    invocation_id: String,
    output: Value,
    output_summary: Value,
    page: Option<InvocationOutputPage>,
) -> CapabilityInvocationResult {
    CapabilityInvocationResult {
        invocation_id,
        status: InvocationStatus::Succeeded,
        output,
        output_summary,
        page,
    }
}

fn pod_json(pod: &PodInfo) -> Value {
    json!({
        "kind": "Pod",
        "name": pod.name,
        "namespace": pod.namespace,
        "status": pod.status.as_str(),
        "ready": pod.ready,
        "restarts": pod.restarts,
        "age": pod.age,
        "node": pod.node,
        "containers": pod.containers,
    })
}

fn service_json(service: &ServiceInfo) -> Value {
    json!({
        "kind": "Service",
        "name": service.name,
        "namespace": service.namespace,
        "type": service.service_type,
        "cluster_ip": service.cluster_ip,
        "ports": service.ports,
        "age": service.age,
    })
}

fn deployment_json(deployment: &DeploymentInfo) -> Value {
    json!({
        "kind": "Deployment",
        "name": deployment.name,
        "namespace": deployment.namespace,
        "ready": deployment.ready,
        "up_to_date": deployment.up_to_date,
        "available": deployment.available,
        "age": deployment.age,
    })
}

fn statefulset_json(statefulset: &StatefulSetInfo) -> Value {
    json!({
        "kind": "StatefulSet",
        "name": statefulset.name,
        "namespace": statefulset.namespace,
        "ready": statefulset.ready,
        "age": statefulset.age,
    })
}

fn daemonset_json(daemonset: &DaemonSetInfo) -> Value {
    json!({
        "kind": "DaemonSet",
        "name": daemonset.name,
        "namespace": daemonset.namespace,
        "desired": daemonset.desired,
        "current": daemonset.current,
        "ready": daemonset.ready,
        "age": daemonset.age,
    })
}

fn job_json(job: &JobInfo) -> Value {
    json!({
        "kind": "Job",
        "name": job.name,
        "namespace": job.namespace,
        "status": job.status.as_str(),
        "completions": job.completions,
        "duration": job.duration,
        "age": job.age,
    })
}

fn cronjob_json(cronjob: &CronJobInfo) -> Value {
    json!({
        "kind": "CronJob",
        "name": cronjob.name,
        "namespace": cronjob.namespace,
        "schedule": cronjob.schedule,
        "timezone": cronjob.timezone,
        "active": cronjob.active,
        "last_schedule": cronjob.last_schedule,
        "age": cronjob.age,
    })
}

fn pvc_json(pvc: &PvcInfo) -> Value {
    json!({
        "kind": "PersistentVolumeClaim",
        "name": pvc.name,
        "namespace": pvc.namespace,
        "status": pvc.status,
        "volume": pvc.volume,
        "capacity": pvc.capacity,
        "access_modes": pvc.access_modes,
        "storage_class": pvc.storage_class,
        "age": pvc.age,
    })
}

fn ingress_json(ingress: &IngressInfo) -> Value {
    json!({
        "kind": "Ingress",
        "name": ingress.name,
        "namespace": ingress.namespace,
        "class": ingress.class,
        "hosts": ingress.hosts,
        "addresses": ingress.addresses,
        "age": ingress.age,
    })
}

fn service_account_json(account: &ServiceAccountInfo) -> Value {
    json!({
        "kind": "ServiceAccount",
        "name": account.name,
        "namespace": account.namespace,
        "secrets_count": account.secrets_count,
        "age": account.age,
    })
}

fn configmap_json(configmap: &ConfigMapInfo) -> Value {
    json!({
        "kind": "ConfigMap",
        "name": configmap.name,
        "namespace": configmap.namespace,
        "data_count": configmap.data_count,
        "age": configmap.age,
    })
}

fn secret_json(secret: &SecretInfo) -> Value {
    json!({
        "kind": "Secret",
        "name": secret.name,
        "namespace": secret.namespace,
        "secret_type": secret.secret_type,
        "data_count": secret.data_count,
        "age": secret.age,
    })
}

fn node_json(node: &NodeInfo) -> Value {
    json!({
        "kind": "Node",
        "name": node.name,
        "status": node.status,
        "roles": node.roles,
        "version": node.version,
        "age": node.age,
    })
}

fn event_json(event: &EventInfo) -> Value {
    json!({
        "kind": "Event",
        "type": event.event_type,
        "reason": event.reason,
        "object": event.object,
        "message": event.message,
        "age": event.age,
        "count": event.count,
    })
}

fn namespace_input(config: &K8sConfig, input: &Value) -> Result<String, CapabilityError> {
    Ok(optional_string(input, "namespace")?.unwrap_or_else(|| config.namespace().to_string()))
}

fn normalize_resource_type(resource_type: &str) -> Result<&'static str, CapabilityError> {
    match resource_type.to_ascii_lowercase().as_str() {
        "pod" | "pods" | "po" => Ok("pods"),
        "service" | "services" | "svc" => Ok("services"),
        "deployment" | "deployments" | "deploy" => Ok("deployments"),
        "statefulset" | "statefulsets" | "sts" => Ok("statefulsets"),
        "daemonset" | "daemonsets" | "ds" => Ok("daemonsets"),
        "job" | "jobs" => Ok("jobs"),
        "cronjob" | "cronjobs" | "cj" => Ok("cronjobs"),
        "pvc" | "pvcs" | "persistentvolumeclaim" | "persistentvolumeclaims" => Ok("pvcs"),
        "ingress" | "ingresses" | "ing" => Ok("ingresses"),
        "serviceaccount" | "serviceaccounts" | "sa" => Ok("serviceaccounts"),
        "configmap" | "configmaps" | "cm" => Ok("configmaps"),
        "secret" | "secrets" => Ok("secrets"),
        "node" | "nodes" => Ok("nodes"),
        "event" | "events" => Ok("events"),
        other => Err(validation_error(
            "validation.kubernetes_resource_type_invalid",
            "Kubernetes resource type is not supported.",
            json!({
                "resource_type": other,
                "supported_resource_types": SUPPORTED_RESOURCE_TYPES,
            }),
        )),
    }
}

fn is_secret_resource(resource_type: &str) -> bool {
    matches!(
        resource_type.to_ascii_lowercase().as_str(),
        "secret" | "secrets"
    )
}

fn auth_type(auth: &K8sAuth) -> &'static str {
    match auth {
        K8sAuth::Token { .. } => "token",
        K8sAuth::ClientCert { .. } => "client_cert",
        K8sAuth::InCluster => "in_cluster",
    }
}

struct ManifestSummary {
    api_version: String,
    kind: String,
    name: String,
    namespace: String,
}

fn parse_manifest_summary(
    yaml: &str,
    default_namespace: &str,
) -> Result<ManifestSummary, CapabilityError> {
    let value: Value = serde_yaml::from_str(yaml).map_err(|error| {
        validation_error(
            "validation.kubernetes_yaml_invalid",
            "Kubernetes YAML could not be parsed.",
            json!({ "message": error.to_string() }),
        )
    })?;
    let api_version = value["apiVersion"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            validation_error(
                "validation.kubernetes_yaml_missing_field",
                "Kubernetes YAML is missing apiVersion.",
                json!({ "field": "apiVersion" }),
            )
        })?
        .to_string();
    let kind = value["kind"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            validation_error(
                "validation.kubernetes_yaml_missing_field",
                "Kubernetes YAML is missing kind.",
                json!({ "field": "kind" }),
            )
        })?
        .to_string();
    let name = value["metadata"]["name"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            validation_error(
                "validation.kubernetes_yaml_missing_field",
                "Kubernetes YAML is missing metadata.name.",
                json!({ "field": "metadata.name" }),
            )
        })?
        .to_string();
    let namespace = value["metadata"]["namespace"]
        .as_str()
        .filter(|value| !value.is_empty())
        .unwrap_or(default_namespace)
        .to_string();

    Ok(ManifestSummary {
        api_version,
        kind,
        name,
        namespace,
    })
}

struct BoundedText {
    value: String,
    source_bytes: usize,
    truncated: bool,
}

async fn collect_log_output(
    config: &K8sConfig,
    rx: &mut mpsc::UnboundedReceiver<K8sEvent>,
    max_bytes: usize,
) -> Result<BoundedText, CapabilityError> {
    let mut value = String::new();
    let mut source_bytes = 0usize;
    let mut truncated = false;

    while let Some(event) = rx.recv().await {
        match event {
            K8sEvent::LogLine { text } => {
                let line = format!("{}\n", text);
                source_bytes = source_bytes.saturating_add(line.len());
                if truncated {
                    continue;
                }
                if push_bounded_text(&mut value, &line, max_bytes) {
                    truncated = true;
                }
            }
            K8sEvent::Error(message) => {
                return Err(target_error(config, "kubernetes.logs_failed", message));
            }
            _ => {}
        }
    }

    Ok(BoundedText {
        value,
        source_bytes,
        truncated,
    })
}

fn bounded_text(value: &str, max_bytes: usize) -> BoundedText {
    let source_bytes = value.len();
    let mut bounded = String::new();
    let truncated = push_bounded_text(&mut bounded, value, max_bytes);
    BoundedText {
        value: bounded,
        source_bytes,
        truncated,
    }
}

fn push_bounded_text(buffer: &mut String, chunk: &str, max_bytes: usize) -> bool {
    let remaining = max_bytes.saturating_sub(buffer.len());
    if chunk.len() <= remaining {
        buffer.push_str(chunk);
        return false;
    }

    for ch in chunk.chars() {
        let len = ch.len_utf8();
        if buffer.len().saturating_add(len) > max_bytes {
            return true;
        }
        buffer.push(ch);
    }
    false
}

fn empty_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false
    })
}

fn kubernetes_list_schema(item_key: &str, item_schema: Value) -> Value {
    json!({
        "type": "object",
        "required": [
            item_key,
            "item_count",
            "source_item_count",
            "limit",
            "cursor",
            "next_cursor",
            "truncated",
            "metadata"
        ],
        "properties": {
            item_key: { "type": "array", "items": item_schema },
            "item_count": { "type": "integer", "minimum": 0 },
            "source_item_count": { "type": "integer", "minimum": 0 },
            "limit": { "type": "integer", "minimum": 1, "maximum": MAX_PAGE_LIMIT },
            "cursor": { "type": ["string", "null"] },
            "next_cursor": { "type": ["string", "null"] },
            "truncated": { "type": "boolean" },
            "metadata": { "type": ["object", "null"] }
        },
        "additionalProperties": false
    })
}

fn namespace_schema() -> Value {
    json!({
        "type": "object",
        "required": ["name"],
        "properties": {
            "name": { "type": "string" }
        },
        "additionalProperties": false
    })
}

fn resource_item_schema() -> Value {
    json!({
        "type": "object",
        "required": ["kind", "name"],
        "properties": {
            "kind": { "type": "string" },
            "name": { "type": "string" },
            "namespace": { "type": ["string", "null"] }
        },
        "additionalProperties": true
    })
}

fn text_limit_schema() -> Value {
    json!({
        "type": "integer",
        "minimum": 1,
        "maximum": MAX_TEXT_LIMIT_BYTES,
        "default": DEFAULT_TEXT_LIMIT_BYTES
    })
}

fn yaml_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "resource_type",
            "name",
            "namespace",
            "yaml",
            "bytes_returned",
            "source_bytes",
            "truncated",
            "byte_limit"
        ],
        "properties": {
            "resource_type": { "type": "string" },
            "name": { "type": "string" },
            "namespace": { "type": "string" },
            "yaml": { "type": "string" },
            "bytes_returned": { "type": "integer", "minimum": 0 },
            "source_bytes": { "type": "integer", "minimum": 0 },
            "truncated": { "type": "boolean" },
            "byte_limit": { "type": "integer", "minimum": 1, "maximum": MAX_TEXT_LIMIT_BYTES }
        },
        "additionalProperties": false
    })
}

fn log_output_schema() -> Value {
    json!({
        "type": "object",
        "required": [
            "pod",
            "namespace",
            "container",
            "text",
            "bytes_returned",
            "source_bytes",
            "truncated",
            "tail",
            "byte_limit"
        ],
        "properties": {
            "pod": { "type": "string" },
            "namespace": { "type": "string" },
            "container": { "type": ["string", "null"] },
            "text": { "type": "string" },
            "bytes_returned": { "type": "integer", "minimum": 0 },
            "source_bytes": { "type": "integer", "minimum": 0 },
            "truncated": { "type": "boolean" },
            "tail": { "type": "integer", "minimum": 1, "maximum": MAX_LOG_TAIL },
            "byte_limit": { "type": "integer", "minimum": 1, "maximum": MAX_TEXT_LIMIT_BYTES }
        },
        "additionalProperties": false
    })
}

fn mutation_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["ok", "operation", "dry_run", "destructive", "details"],
        "properties": {
            "ok": { "type": "boolean" },
            "operation": { "type": "string" },
            "dry_run": { "type": "boolean" },
            "would_execute": { "type": "boolean" },
            "destructive": { "type": "boolean" },
            "details": { "type": "object" }
        },
        "additionalProperties": false
    })
}

fn required_string(input: &Value, field: &str) -> Result<String, CapabilityError> {
    optional_string(input, field)?.ok_or_else(|| {
        validation_error(
            "validation.input_field_required",
            "Required string input field is missing.",
            json!({ "field": field }),
        )
    })
}

fn optional_string(input: &Value, field: &str) -> Result<Option<String>, CapabilityError> {
    match input.get(field) {
        Some(value) if !value.is_string() => Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be a string.",
            json!({ "field": field }),
        )),
        Some(value) => value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .map(|value| Some(value.to_string()))
            .ok_or_else(|| {
                validation_error(
                    "validation.input_field_required",
                    "String input field cannot be empty.",
                    json!({ "field": field }),
                )
            }),
        None => Ok(None),
    }
}

fn requested_usize(
    input: &Value,
    field: &str,
    default: usize,
    minimum: usize,
    maximum: usize,
) -> Result<usize, CapabilityError> {
    let Some(value) = input.get(field) else {
        return Ok(default);
    };
    let Some(raw) = value.as_u64() else {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be an integer.",
            json!({ "field": field }),
        ));
    };
    if raw < minimum as u64 || raw > maximum as u64 {
        return Err(validation_error(
            "validation.input_field_out_of_range",
            "Input field is outside the supported range.",
            json!({ "field": field, "minimum": minimum, "maximum": maximum }),
        ));
    }
    Ok(raw as usize)
}

fn requested_i64(
    input: &Value,
    field: &str,
    default: i64,
    minimum: i64,
    maximum: i64,
) -> Result<i64, CapabilityError> {
    let Some(value) = input.get(field) else {
        return Ok(default);
    };
    let Some(raw) = value.as_i64() else {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be an integer.",
            json!({ "field": field }),
        ));
    };
    if raw < minimum || raw > maximum {
        return Err(validation_error(
            "validation.input_field_out_of_range",
            "Input field is outside the supported range.",
            json!({ "field": field, "minimum": minimum, "maximum": maximum }),
        ));
    }
    Ok(raw)
}

fn requested_u32(
    input: &Value,
    field: &str,
    minimum: u32,
    maximum: u32,
) -> Result<u32, CapabilityError> {
    let Some(value) = input.get(field) else {
        return Err(validation_error(
            "validation.input_field_required",
            "Required integer input field is missing.",
            json!({ "field": field }),
        ));
    };
    let Some(raw) = value.as_u64() else {
        return Err(validation_error(
            "validation.input_field_invalid",
            "Input field must be an integer.",
            json!({ "field": field }),
        ));
    };
    if raw < minimum as u64 || raw > maximum as u64 {
        return Err(validation_error(
            "validation.input_field_out_of_range",
            "Input field is outside the supported range.",
            json!({ "field": field, "minimum": minimum, "maximum": maximum }),
        ));
    }
    Ok(raw as u32)
}

#[derive(Debug)]
struct PageRequest {
    limit: usize,
    offset: usize,
    cursor: Option<String>,
}

fn page_request(page: Option<&Pagination>) -> Result<PageRequest, CapabilityError> {
    let Some(page) = page else {
        return Ok(PageRequest {
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
            cursor: None,
        });
    };
    let limit = (page.limit as usize).clamp(1, MAX_PAGE_LIMIT);
    let offset = match &page.cursor {
        Some(cursor) => cursor.parse::<usize>().map_err(|_| {
            validation_error(
                "validation.cursor_invalid",
                "Kubernetes list cursor must be a numeric offset.",
                json!({ "cursor": cursor }),
            )
        })?,
        None => 0,
    };
    Ok(PageRequest {
        limit,
        offset,
        cursor: page.cursor.clone(),
    })
}

fn validation_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Validation,
        code,
        message,
        details,
        None,
        false,
    )
}

fn policy_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Policy,
        code,
        message,
        details,
        None,
        false,
    )
}

fn unavailable_error(code: &str, message: &str, details: Value) -> CapabilityError {
    capability_error(
        CapabilityErrorCategory::Unavailable,
        code,
        message,
        details,
        None,
        true,
    )
}

fn target_error(config: &K8sConfig, code: &str, message: String) -> CapabilityError {
    let (message, redaction) = redact_kubernetes_target_message(message, config);
    capability_error_with_redaction(
        CapabilityErrorCategory::TargetSystem,
        code,
        "Kubernetes target operation failed.",
        json!({ "message": message }),
        Some(TargetSystemFailure {
            system: Some("kubernetes".into()),
            code: Some(code.into()),
            message: Some(message),
        }),
        false,
        redaction,
    )
}

fn redact_kubernetes_target_message(
    message: String,
    config: &K8sConfig,
) -> (String, RedactionStatus) {
    let original = message.clone();
    let mut redacted = redact_url_literals(message);

    match &config.connection {
        K8sConnection::Kubeconfig { path, context } => {
            if let Some(path) = path {
                redact_value(&mut redacted, path);
            }
            if let Some(context) = context {
                redact_value(&mut redacted, context);
            }
        }
        K8sConnection::Direct {
            api_url,
            auth,
            ca_cert,
            ..
        } => {
            redact_value(&mut redacted, api_url);
            if let Some(ca_cert) = ca_cert {
                redact_value(&mut redacted, ca_cert);
            }
            match auth {
                K8sAuth::Token { token } => redact_value(&mut redacted, token),
                K8sAuth::ClientCert {
                    cert_path,
                    key_path,
                } => {
                    redact_value(&mut redacted, cert_path);
                    redact_value(&mut redacted, key_path);
                }
                K8sAuth::InCluster => {}
            }
        }
    }

    let status = if redacted != original {
        RedactionStatus::Applied
    } else {
        RedactionStatus::NotRequired
    };
    (redacted, status)
}

fn redact_value(message: &mut String, sensitive: &str) {
    if !sensitive.is_empty() && message.contains(sensitive) {
        *message = message.replace(sensitive, "<redacted>");
    }
}

fn redact_url_literals(mut message: String) -> String {
    for scheme in ["http://", "https://"] {
        let mut search_from = 0usize;
        while let Some(relative_start) = message[search_from..].find(scheme) {
            let start = search_from + relative_start;
            let end = message[start..]
                .find(|ch: char| {
                    ch.is_whitespace() || matches!(ch, '"' | '\'' | ')' | '(' | ',' | ';')
                })
                .map(|relative_end| start + relative_end)
                .unwrap_or(message.len());
            message.replace_range(start..end, "<redacted>");
            search_from = start + "<redacted>".len();
        }
    }
    message
}

fn capability_error(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
) -> CapabilityError {
    capability_error_with_redaction(
        category,
        code,
        message,
        details,
        target,
        retryable,
        RedactionStatus::NotRequired,
    )
}

fn capability_error_with_redaction(
    category: CapabilityErrorCategory,
    code: &str,
    message: &str,
    details: Value,
    target: Option<TargetSystemFailure>,
    retryable: bool,
    redaction: RedactionStatus,
) -> CapabilityError {
    CapabilityError {
        category,
        code: code.to_string(),
        message: message.to_string(),
        details,
        target,
        retryable,
        redaction,
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use voidb_core::{
        ActorRef, ActorType, ConnectionInstancePurpose, ConnectionProfileRef, InstanceReusePolicy,
        InvocationConnectionTarget, InvocationControls,
    };

    use super::*;

    #[test]
    fn catalog_marks_cluster_mutations_as_destructive_dry_run() {
        let capabilities = kubernetes_capabilities();
        for id in ["delete", "scale", "restart", "apply"] {
            let capability = capabilities
                .iter()
                .find(|capability| capability.id == id)
                .expect("mutation capability");
            assert!(capability.destructive);
            assert!(capability.supports_dry_run);
            assert_eq!(
                capability.effective_risk(),
                CapabilityRiskLevel::Destructive
            );
        }

        let list = capabilities
            .iter()
            .find(|capability| capability.id == "list")
            .expect("list capability");
        assert!(!list.destructive);
        assert!(!list.supports_dry_run);
    }

    #[tokio::test]
    async fn apply_dry_run_does_not_open_cluster_connection() {
        let config = unreachable_config();
        let mut invocation = invocation(
            "apply",
            json!({
                "yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: agent-probe\n"
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_kubernetes_capability(&config, invocation)
            .await
            .expect("dry-run");

        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output["details"]["kind"], "ConfigMap");
        assert_eq!(result.output["details"]["name"], "agent-probe");
    }

    #[tokio::test]
    async fn delete_dry_run_does_not_open_cluster_connection() {
        let config = unreachable_config();
        let mut invocation = invocation(
            "delete",
            json!({
                "resource_type": "pod",
                "name": "agent-probe",
                "namespace": "default"
            }),
        );
        invocation.controls.dry_run = true;

        let result = invoke_kubernetes_capability(&config, invocation)
            .await
            .expect("dry-run");

        assert_eq!(result.output["dry_run"], true);
        assert_eq!(result.output["details"]["resource_type"], "pod");
    }

    #[tokio::test]
    async fn get_yaml_blocks_secret_payloads_before_connection() {
        let config = unreachable_config();
        let error = invoke_kubernetes_capability(
            &config,
            invocation(
                "get_yaml",
                json!({
                    "resource_type": "secret",
                    "name": "db-password",
                    "namespace": "default"
                }),
            ),
        )
        .await
        .expect_err("secret payload blocked");

        assert_eq!(error.category, CapabilityErrorCategory::Policy);
        assert_eq!(error.code, "policy.kubernetes_secret_payload_blocked");
    }

    #[tokio::test]
    async fn diagnostics_do_not_open_cluster_connection() {
        let config = unreachable_config();
        let result = invoke_kubernetes_capability(&config, invocation("diagnostics", json!({})))
            .await
            .expect("diagnostics");

        assert_eq!(result.output["connection_type"], "direct");
        assert_eq!(result.output["auth_type"], "token");
        assert_eq!(result.output["network_checked"], false);
    }

    #[test]
    fn diagnostics_treat_blank_kubeconfig_fields_as_unset() {
        let config = K8sConfig {
            connection: K8sConnection::Kubeconfig {
                path: Some("  ".into()),
                context: Some("".into()),
            },
            default_namespace: None,
            timeout: 30,
        };

        let result = diagnostics_result(&config, "diagnostics-test".into());

        assert_eq!(result.output["kubeconfig_path_present"], false);
        assert_eq!(result.output["context_present"], false);
    }

    #[test]
    fn target_errors_redact_kubeconfig_material() {
        let path = "/tmp/voidb-fixture-secret/kubeconfig";
        let context = "kind-voidb-secret-context";
        let config = K8sConfig {
            connection: K8sConnection::Kubeconfig {
                path: Some(path.into()),
                context: Some(context.into()),
            },
            default_namespace: Some("default".into()),
            timeout: 1,
        };

        let error = target_error(
            &config,
            "kubernetes.connect_failed",
            format!("failed to use {path} with context {context} at https://127.0.0.1:6443"),
        );
        let encoded = serde_json::to_string(&error).expect("serialize error");

        assert_eq!(error.redaction, RedactionStatus::Applied);
        assert!(!encoded.contains(path));
        assert!(!encoded.contains(context));
        assert!(!encoded.contains("https://127.0.0.1:6443"));
        assert!(encoded.contains("<redacted>"));
    }

    #[test]
    fn target_errors_redact_direct_auth_material() {
        let api_url = "https://cluster.example.invalid:6443";
        let token = "voidb-kubernetes-secret-token";
        let config = K8sConfig {
            connection: K8sConnection::Direct {
                api_url: api_url.into(),
                auth: K8sAuth::Token {
                    token: token.into(),
                },
                verify_ssl: false,
                ca_cert: Some("/tmp/voidb-ca.pem".into()),
            },
            default_namespace: Some("default".into()),
            timeout: 1,
        };

        let error = target_error(
            &config,
            "kubernetes.connect_failed",
            format!("failed to reach {api_url} with bearer token {token}"),
        );
        let encoded = serde_json::to_string(&error).expect("serialize error");

        assert_eq!(error.redaction, RedactionStatus::Applied);
        assert!(!encoded.contains(api_url));
        assert!(!encoded.contains(token));
        assert!(encoded.contains("<redacted>"));
    }

    fn unreachable_config() -> K8sConfig {
        K8sConfig {
            connection: K8sConnection::Direct {
                api_url: "https://127.0.0.1:1".into(),
                auth: K8sAuth::Token {
                    token: "secret-token".into(),
                },
                verify_ssl: false,
                ca_cert: None,
            },
            default_namespace: Some("default".into()),
            timeout: 1,
        }
    }

    fn invocation(capability_id: &str, input: Value) -> CapabilityInvocation {
        CapabilityInvocation {
            id: format!("invoke-{}", capability_id),
            plugin_id: PLUGIN_ID.into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::FromProfile {
                profile: ConnectionProfileRef::Name("cluster".into()),
                purpose: ConnectionInstancePurpose::CapabilityInvocation,
                reuse: InstanceReusePolicy::Allow,
                options: Value::Null,
            },
            input,
            controls: InvocationControls::default(),
            actor: Some(ActorRef {
                id: "test-agent".into(),
                actor_type: ActorType::Agent,
            }),
            requested_at: Utc::now(),
        }
    }
}
