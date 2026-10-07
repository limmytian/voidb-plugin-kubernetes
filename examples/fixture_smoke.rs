//! Kubernetes fixture-backed capability smoke driver.
//!
//! This example is script-facing. It exercises the Kubernetes plugin
//! capability surface against a disposable local kind fixture using only the
//! generated kubeconfig and scratch namespace from the fixture environment.

#![allow(clippy::result_large_err)]

use anyhow::{Context, Result, bail, ensure};
use chrono::Utc;
use serde_json::{Value, json};
use tokio::time::{Duration, sleep};
use voidb_core::{
    ActorRef, ActorType, CapabilityError, CapabilityErrorCategory, CapabilityInvocation,
    CapabilityInvocationResult, InvocationAcknowledgement, InvocationConnectionTarget,
    InvocationControls, InvocationStatus, Pagination, RedactionStatus,
};
use voidb_plugin_kubernetes::{
    config::{K8sAuth, K8sConfig, K8sConnection},
    invoke_kubernetes_capability,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = config_from_env()?;
    let namespace = required_env("VOIDB_K8S_SMOKE_NAMESPACE")?;
    let configmap = required_env("VOIDB_K8S_SMOKE_CONFIGMAP")?;
    let run_id =
        std::env::var("VOIDB_FIXTURE_RUN_ID").unwrap_or_else(|_| "kubernetes-fixture-smoke".into());
    let safe_id = kubernetes_safe_id(&run_id);
    let applied_configmap = format!("voidb-smoke-applied-{safe_id}");
    let delete_configmap = format!("voidb-smoke-delete-{safe_id}");
    let protected = protected_samples(&config);

    ensure!(
        namespace.starts_with("voidb-smoke-"),
        "refusing Kubernetes smoke outside a generated fixture namespace: {namespace}"
    );
    ensure!(
        configmap == "voidb-agent-probe",
        "refusing Kubernetes smoke outside the generated fixture configmap"
    );

    let diagnostics = invoke_checked(
        &config,
        "diagnostics",
        json!({}),
        false,
        false,
        None,
        "kubernetes.diagnostics",
    )
    .await?;
    ensure_succeeded(&diagnostics, "kubernetes.diagnostics")?;
    ensure!(
        diagnostics.output["connection_type"] == "kubeconfig"
            && diagnostics.output["kubeconfig_path_present"] == true
            && diagnostics.output["network_checked"] == false
            && diagnostics.output["default_namespace"] == namespace,
        "diagnostics should return shape-only kubeconfig metadata: {}",
        diagnostics.output
    );
    ensure_result_excludes(&diagnostics, &protected, "kubernetes.diagnostics")?;

    let contexts = invoke_checked(
        &config,
        "contexts",
        json!({}),
        false,
        false,
        None,
        "kubernetes.contexts",
    )
    .await?;
    ensure_succeeded(&contexts, "kubernetes.contexts")?;
    ensure!(
        contexts.output["context_count"]
            .as_u64()
            .unwrap_or_default()
            >= 1,
        "contexts should include the fixture context: {}",
        contexts.output
    );
    ensure_result_excludes(&contexts, &protected, "kubernetes.contexts")?;

    let paged_namespaces = invoke_checked(
        &config,
        "namespaces",
        json!({}),
        false,
        false,
        Some(Pagination {
            limit: 1,
            cursor: None,
        }),
        "kubernetes.namespaces paged",
    )
    .await?;
    ensure_succeeded(&paged_namespaces, "kubernetes.namespaces paged")?;
    ensure!(
        paged_namespaces.output["item_count"]
            .as_u64()
            .unwrap_or_default()
            <= 1
            && paged_namespaces.output["limit"].as_u64() == Some(1),
        "namespaces should honor pagination: {}",
        paged_namespaces.output
    );

    let all_namespaces = invoke_checked(
        &config,
        "namespaces",
        json!({}),
        false,
        false,
        Some(Pagination {
            limit: 50,
            cursor: None,
        }),
        "kubernetes.namespaces full",
    )
    .await?;
    ensure_namespace_present(&all_namespaces.output, &namespace)?;

    let nodes = invoke_checked(
        &config,
        "list",
        json!({ "resource_type": "nodes" }),
        false,
        false,
        Some(Pagination {
            limit: 1,
            cursor: None,
        }),
        "kubernetes.list nodes",
    )
    .await?;
    ensure_succeeded(&nodes, "kubernetes.list nodes")?;
    ensure!(
        nodes.output["item_count"].as_u64() == Some(1)
            && nodes.output["metadata"]["namespace"].is_null(),
        "node list should be bounded and cluster-scoped: {}",
        nodes.output
    );

    let pods = invoke_checked(
        &config,
        "list",
        json!({ "resource_type": "pods", "namespace": namespace }),
        false,
        false,
        Some(Pagination {
            limit: 5,
            cursor: None,
        }),
        "kubernetes.list pods",
    )
    .await?;
    ensure_succeeded(&pods, "kubernetes.list pods")?;
    ensure!(
        pods.output["metadata"]["namespace"] == namespace,
        "pod list should stay inside the fixture namespace: {}",
        pods.output
    );

    let configmaps = invoke_checked(
        &config,
        "list",
        json!({ "resource_type": "configmaps", "namespace": namespace }),
        false,
        false,
        Some(Pagination {
            limit: 10,
            cursor: None,
        }),
        "kubernetes.list configmaps",
    )
    .await?;
    ensure_succeeded(&configmaps, "kubernetes.list configmaps")?;
    ensure_item_present(&configmaps.output, &configmap)?;

    let bounded_yaml = invoke_checked(
        &config,
        "get_yaml",
        json!({
            "resource_type": "configmap",
            "name": configmap,
            "namespace": namespace,
            "max_bytes": 64
        }),
        false,
        false,
        None,
        "kubernetes.get_yaml bounded",
    )
    .await?;
    ensure_succeeded(&bounded_yaml, "kubernetes.get_yaml bounded")?;
    ensure!(
        bounded_yaml.output["bytes_returned"]
            .as_u64()
            .unwrap_or_default()
            <= 64
            && bounded_yaml.output["source_bytes"]
                .as_u64()
                .unwrap_or_default()
                >= bounded_yaml.output["bytes_returned"]
                    .as_u64()
                    .unwrap_or_default(),
        "get_yaml should enforce max_bytes: {}",
        bounded_yaml.output
    );
    ensure_result_excludes(&bounded_yaml, &protected, "kubernetes.get_yaml bounded")?;

    ensure_policy_error(
        invoke(
            &unavailable_config(),
            "get_yaml",
            json!({
                "resource_type": "secret",
                "name": "fixture-secret",
                "namespace": namespace
            }),
            false,
            false,
            None,
        )
        .await,
        "policy.kubernetes_secret_payload_blocked",
        "kubernetes.get_yaml secret",
    )?;

    let dry_run_secret = "voidb-kubernetes-dry-run-secret";
    let dry_apply = invoke_checked(
        &unavailable_config(),
        "apply",
        json!({
            "namespace": namespace,
            "yaml": configmap_yaml(&namespace, &applied_configmap, dry_run_secret)
        }),
        true,
        false,
        None,
        "kubernetes.apply dry-run",
    )
    .await?;
    ensure_succeeded(&dry_apply, "kubernetes.apply dry-run")?;
    ensure_eq_value(
        &dry_apply.output,
        "dry_run",
        true,
        "kubernetes.apply dry-run",
    )?;
    ensure_result_excludes(
        &dry_apply,
        &[dry_run_secret.into()],
        "kubernetes.apply dry-run",
    )?;

    for (capability, input) in [
        (
            "delete",
            json!({
                "resource_type": "configmap",
                "name": applied_configmap,
                "namespace": namespace
            }),
        ),
        (
            "scale",
            json!({
                "name": "voidb-fixture-deployment",
                "namespace": namespace,
                "replicas": 0
            }),
        ),
        (
            "restart",
            json!({
                "name": "voidb-fixture-deployment",
                "namespace": namespace
            }),
        ),
    ] {
        let dry_result = invoke_checked(
            &unavailable_config(),
            capability,
            input,
            true,
            false,
            None,
            &format!("kubernetes.{capability} dry-run"),
        )
        .await?;
        ensure_succeeded(&dry_result, &format!("kubernetes.{capability} dry-run"))?;
        ensure_eq_value(
            &dry_result.output,
            "dry_run",
            true,
            &format!("kubernetes.{capability} dry-run"),
        )?;
    }

    let applied = invoke_checked(
        &config,
        "apply",
        json!({
            "namespace": namespace,
            "yaml": configmap_yaml(&namespace, &applied_configmap, "applied by fixture smoke")
        }),
        false,
        true,
        None,
        "kubernetes.apply scratch configmap",
    )
    .await?;
    ensure_succeeded(&applied, "kubernetes.apply scratch configmap")?;
    ensure_eq_value(
        &applied.output,
        "dry_run",
        false,
        "kubernetes.apply scratch configmap",
    )?;
    ensure!(
        applied.output["details"]["namespace"] == namespace,
        "apply must stay inside the fixture namespace: {}",
        applied.output
    );
    ensure_configmap_present(&config, &namespace, &applied_configmap).await?;

    let dry_delete_live = invoke_checked(
        &config,
        "delete",
        json!({
            "resource_type": "configmap",
            "name": applied_configmap,
            "namespace": namespace
        }),
        true,
        false,
        None,
        "kubernetes.delete live dry-run",
    )
    .await?;
    ensure_succeeded(&dry_delete_live, "kubernetes.delete live dry-run")?;
    ensure_configmap_present(&config, &namespace, &applied_configmap).await?;

    invoke_checked(
        &config,
        "apply",
        json!({
            "namespace": namespace,
            "yaml": configmap_yaml(&namespace, &delete_configmap, "delete target")
        }),
        false,
        true,
        None,
        "kubernetes.apply delete target",
    )
    .await?;
    invoke_checked(
        &config,
        "delete",
        json!({
            "resource_type": "configmap",
            "name": delete_configmap,
            "namespace": namespace
        }),
        false,
        true,
        None,
        "kubernetes.delete scratch configmap",
    )
    .await?;
    wait_configmap_absent(&config, &namespace, &delete_configmap).await?;

    ensure_missing_pod_error_redacts(&config, &namespace, &protected).await?;
    ensure_unavailable_target_redacts(&protected).await?;
    cleanup_configmap(&config, &namespace, &applied_configmap).await;

    println!("kubernetes fixture capability smoke passed");
    println!(
        "capabilities: diagnostics, contexts, namespaces, list, get_yaml, apply, delete, scale, restart, logs-error"
    );
    println!("fixture_namespace: {namespace}");
    Ok(())
}

fn config_from_env() -> Result<K8sConfig> {
    Ok(K8sConfig {
        connection: K8sConnection::Kubeconfig {
            path: Some(required_env("VOIDB_K8S_SMOKE_KUBECONFIG")?),
            context: Some(required_env("VOIDB_K8S_SMOKE_CONTEXT")?),
        },
        default_namespace: Some(required_env("VOIDB_K8S_SMOKE_NAMESPACE")?),
        timeout: 30,
    })
}

fn unavailable_config() -> K8sConfig {
    K8sConfig {
        connection: K8sConnection::Direct {
            api_url: "https://127.0.0.1:1".into(),
            auth: K8sAuth::Token {
                token: "voidb-kubernetes-unavailable-token".into(),
            },
            verify_ssl: false,
            ca_cert: None,
        },
        default_namespace: Some("default".into()),
        timeout: 1,
    }
}

fn required_env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} is required"))
}

fn kubernetes_safe_id(value: &str) -> String {
    let mut output = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
        } else if ch == '-' || ch == '_' {
            output.push('-');
        }
    }
    let output = output.trim_matches('-');
    if output.is_empty() {
        "run".into()
    } else {
        output.chars().take(24).collect()
    }
}

fn configmap_yaml(namespace: &str, name: &str, value: &str) -> String {
    format!(
        r#"apiVersion: v1
kind: ConfigMap
metadata:
  name: {name}
  namespace: {namespace}
  labels:
    voidb.fixture: "true"
    voidb.fixture.smoke: "true"
data:
  note: "{value}"
"#
    )
}

async fn invoke(
    config: &K8sConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    acknowledged: bool,
    page: Option<Pagination>,
) -> std::result::Result<CapabilityInvocationResult, CapabilityError> {
    invoke_kubernetes_capability(
        config,
        CapabilityInvocation {
            id: format!("kubernetes-fixture-smoke-{capability_id}"),
            plugin_id: "kubernetes".into(),
            capability_id: capability_id.into(),
            connection: InvocationConnectionTarget::Stateless,
            input,
            controls: InvocationControls {
                dry_run,
                acknowledgement: acknowledged.then(acknowledgement),
                page,
                ..InvocationControls::default()
            },
            actor: Some(actor()),
            requested_at: Utc::now(),
        },
    )
    .await
}

async fn invoke_checked(
    config: &K8sConfig,
    capability_id: &str,
    input: Value,
    dry_run: bool,
    acknowledged: bool,
    page: Option<Pagination>,
    label: &str,
) -> Result<CapabilityInvocationResult> {
    invoke(config, capability_id, input, dry_run, acknowledged, page)
        .await
        .map_err(|error| {
            let error_json = serde_json::to_string(&error).unwrap_or_else(|_| format!("{error:?}"));
            anyhow::anyhow!("{label}: {error_json}")
        })
}

fn actor() -> ActorRef {
    ActorRef {
        id: "agent:kubernetes-fixture-smoke".into(),
        actor_type: ActorType::Agent,
    }
}

fn acknowledgement() -> InvocationAcknowledgement {
    InvocationAcknowledgement {
        actor: actor(),
        acknowledged_at: Utc::now(),
        reason: Some("fixture smoke mutation scoped to generated Kubernetes namespace".into()),
        approval_id: Some("kubernetes-fixture-smoke".into()),
    }
}

fn ensure_succeeded(result: &CapabilityInvocationResult, label: &str) -> Result<()> {
    ensure!(
        result.status == InvocationStatus::Succeeded,
        "{label} returned non-success status: {:?}",
        result.status
    );
    Ok(())
}

fn ensure_eq_value(output: &Value, field: &str, expected: bool, label: &str) -> Result<()> {
    ensure!(
        output[field] == expected,
        "{label} should report {field}={expected}: {output}"
    );
    Ok(())
}

fn ensure_result_excludes(
    result: &CapabilityInvocationResult,
    samples: &[String],
    label: &str,
) -> Result<()> {
    let output = serde_json::to_string(&result.output)?;
    let summary = serde_json::to_string(&result.output_summary)?;
    for sample in samples.iter().filter(|sample| !sample.is_empty()) {
        ensure!(
            !output.contains(sample) && !summary.contains(sample),
            "{label} output exposed protected Kubernetes material"
        );
    }
    Ok(())
}

fn protected_samples(config: &K8sConfig) -> Vec<String> {
    let mut samples = Vec::new();
    match &config.connection {
        K8sConnection::Kubeconfig { path, .. } => {
            if let Some(path) = path {
                samples.push(path.clone());
            }
        }
        K8sConnection::Direct {
            api_url,
            auth,
            ca_cert,
            ..
        } => {
            samples.push(api_url.clone());
            if let Some(ca_cert) = ca_cert {
                samples.push(ca_cert.clone());
            }
            match auth {
                K8sAuth::Token { token } => samples.push(token.clone()),
                K8sAuth::ClientCert {
                    cert_path,
                    key_path,
                } => {
                    samples.push(cert_path.clone());
                    samples.push(key_path.clone());
                }
                K8sAuth::InCluster => {}
            }
        }
    }
    if let Ok(value) = std::env::var("VOIDB_K8S_SMOKE_KUBECONFIG") {
        samples.push(value);
    }
    if let Ok(value) = std::env::var("VOIDB_K8S_SMOKE_API_SERVER") {
        samples.push(value);
    }
    samples
}

fn ensure_namespace_present(output: &Value, namespace: &str) -> Result<()> {
    let namespaces = output["namespaces"]
        .as_array()
        .context("namespaces output should include namespaces array")?;
    ensure!(
        namespaces.iter().any(|item| item["name"] == namespace),
        "namespaces did not include fixture namespace {namespace}: {output}"
    );
    Ok(())
}

fn ensure_item_present(output: &Value, name: &str) -> Result<()> {
    let items = output["items"]
        .as_array()
        .context("list output should include items array")?;
    ensure!(
        items.iter().any(|item| item["name"] == name),
        "list output did not include expected item {name}: {output}"
    );
    Ok(())
}

fn ensure_item_absent(output: &Value, name: &str) -> Result<()> {
    let items = output["items"]
        .as_array()
        .context("list output should include items array")?;
    ensure!(
        !items.iter().any(|item| item["name"] == name),
        "list output still included item {name}: {output}"
    );
    Ok(())
}

async fn ensure_configmap_present(config: &K8sConfig, namespace: &str, name: &str) -> Result<()> {
    let configmaps = invoke_checked(
        config,
        "list",
        json!({ "resource_type": "configmaps", "namespace": namespace }),
        false,
        false,
        Some(Pagination {
            limit: 50,
            cursor: None,
        }),
        "kubernetes.list configmaps",
    )
    .await?;
    ensure_item_present(&configmaps.output, name)
}

async fn wait_configmap_absent(config: &K8sConfig, namespace: &str, name: &str) -> Result<()> {
    for _ in 0..20 {
        let configmaps = invoke_checked(
            config,
            "list",
            json!({ "resource_type": "configmaps", "namespace": namespace }),
            false,
            false,
            Some(Pagination {
                limit: 50,
                cursor: None,
            }),
            "kubernetes.list configmaps after delete",
        )
        .await?;
        if ensure_item_absent(&configmaps.output, name).is_ok() {
            return Ok(());
        }
        sleep(Duration::from_millis(100)).await;
    }
    bail!("configmap {name} still exists after delete")
}

async fn cleanup_configmap(config: &K8sConfig, namespace: &str, name: &str) {
    let _ = invoke(
        config,
        "delete",
        json!({
            "resource_type": "configmap",
            "name": name,
            "namespace": namespace
        }),
        false,
        true,
        None,
    )
    .await;
}

fn ensure_policy_error(
    result: std::result::Result<CapabilityInvocationResult, CapabilityError>,
    code: &str,
    label: &str,
) -> Result<()> {
    match result {
        Ok(result) => bail!("{label}: expected policy error, got {}", result.output),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::Policy,
                "{label}: expected policy error, got {:?}",
                error.category
            );
            ensure!(
                error.code == code,
                "{label}: expected {code}, got {}",
                error.code
            );
        }
    }
    Ok(())
}

async fn ensure_missing_pod_error_redacts(
    config: &K8sConfig,
    namespace: &str,
    protected: &[String],
) -> Result<()> {
    match invoke(
        config,
        "logs",
        json!({
            "pod": "voidb-missing-pod",
            "namespace": namespace,
            "tail": 1,
            "max_bytes": 128
        }),
        false,
        false,
        None,
    )
    .await
    {
        Ok(result) => bail!("expected missing pod logs error, got {}", result.output),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "missing pod should return target-system error: {:?}",
                error.category
            );
            ensure_error_excludes(&error, protected, "kubernetes.logs missing pod")?;
        }
    }
    Ok(())
}

async fn ensure_unavailable_target_redacts(protected: &[String]) -> Result<()> {
    let bad = unavailable_config();
    let mut samples = protected.to_vec();
    samples.extend(protected_samples(&bad));
    match invoke(&bad, "namespaces", json!({}), false, false, None).await {
        Ok(result) => bail!("expected unavailable target error, got {}", result.output),
        Err(error) => {
            ensure!(
                error.category == CapabilityErrorCategory::TargetSystem,
                "unavailable target should return target-system error: {:?}",
                error.category
            );
            ensure!(
                matches!(
                    error.redaction,
                    RedactionStatus::Applied | RedactionStatus::NotRequired
                ),
                "target error should not report failed redaction: {:?}",
                error.redaction
            );
            ensure_error_excludes(&error, &samples, "kubernetes.namespaces unavailable")?;
        }
    }
    Ok(())
}

fn ensure_error_excludes(error: &CapabilityError, samples: &[String], label: &str) -> Result<()> {
    let encoded = serde_json::to_string(error)?;
    for sample in samples.iter().filter(|sample| !sample.is_empty()) {
        ensure!(
            !encoded.contains(sample),
            "{label} error exposed protected Kubernetes material: {encoded}"
        );
    }
    Ok(())
}
