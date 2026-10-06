//! Kubernetes API operations wrapping the kube crate

use std::{path::PathBuf, sync::Once, time::Duration};

use anyhow::{Context, Result};
use k8s_openapi::api::apps::v1::{DaemonSet, Deployment, StatefulSet};
use k8s_openapi::api::batch::v1::{CronJob, Job};
use k8s_openapi::api::core::v1::{
    ConfigMap, Event, Namespace, Node, PersistentVolumeClaim, Pod, Secret, Service, ServiceAccount,
};
use k8s_openapi::api::networking::v1::Ingress;
use kube::api::{
    Api, AttachParams, DeleteParams, DynamicObject, ListParams, LogParams, Patch, PatchParams,
};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::{Client, Config, Discovery, ResourceExt};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;

use crate::config::{K8sAuth, K8sConfig, K8sConnection};
use crate::types::*;

static RUSTLS_PROVIDER: Once = Once::new();

fn install_rustls_provider() {
    RUSTLS_PROVIDER.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Build a kube Client from a K8sConfig
pub async fn create_client(config: &K8sConfig) -> Result<Client> {
    install_rustls_provider();

    let mut client_config = match &config.connection {
        K8sConnection::Kubeconfig { path, context } => {
            let kube_config = read_kubeconfig(path.as_deref())?;

            let opts = KubeConfigOptions {
                context: context.clone(),
                ..Default::default()
            };

            Config::from_custom_kubeconfig(kube_config, &opts)
                .await
                .context("Failed to build config from kubeconfig")?
        }
        K8sConnection::Direct {
            api_url,
            auth,
            verify_ssl,
            ca_cert,
        } => {
            let cluster_url = api_url.parse().context("Invalid API server URL")?;
            let mut direct_config = match auth {
                K8sAuth::InCluster => {
                    let mut in_cluster = Config::incluster()
                        .context("Failed to load in-cluster service account configuration")?;
                    in_cluster.cluster_url = cluster_url;
                    in_cluster
                }
                _ => Config::new(cluster_url),
            };
            direct_config.accept_invalid_certs = !verify_ssl;

            if let Some(path) = ca_cert.as_deref().filter(|path| !path.trim().is_empty()) {
                direct_config.root_cert = Some(load_ca_bundle(path)?);
            }

            match auth {
                K8sAuth::Token { token } => {
                    direct_config.auth_info.token = Some(token.clone().into());
                }
                K8sAuth::ClientCert {
                    cert_path,
                    key_path,
                } => {
                    let cert_pem =
                        std::fs::read(cert_path).context("Failed to read client certificate")?;
                    let key_pem = std::fs::read(key_path).context("Failed to read client key")?;
                    use base64::Engine as _;
                    direct_config.auth_info.client_certificate_data =
                        Some(base64::engine::general_purpose::STANDARD.encode(&cert_pem));
                    direct_config.auth_info.client_key_data = Some(
                        base64::engine::general_purpose::STANDARD
                            .encode(&key_pem)
                            .into(),
                    );
                }
                K8sAuth::InCluster => {}
            }

            direct_config
        }
    };

    apply_request_timeout(&mut client_config, config.timeout);
    Client::try_from(client_config).context("Failed to create Kubernetes client")
}

fn read_kubeconfig(configured_path: Option<&str>) -> Result<Kubeconfig> {
    match normalize_kubeconfig_path(configured_path)? {
        Some(path) => Kubeconfig::read_from(&path).context("Failed to read configured kubeconfig"),
        None => Kubeconfig::read().context("Failed to read default kubeconfig"),
    }
}

fn normalize_kubeconfig_path(configured_path: Option<&str>) -> Result<Option<PathBuf>> {
    let Some(path) = configured_path
        .map(str::trim)
        .filter(|path| !path.is_empty())
    else {
        return Ok(None);
    };

    let home_relative = path.strip_prefix("~/").or_else(|| path.strip_prefix("～/"));
    if path == "~" || path == "～" || home_relative.is_some() {
        let home = dirs::home_dir().context("Cannot determine home directory for kubeconfig")?;
        return Ok(Some(match home_relative {
            Some(relative) => home.join(relative),
            None => home,
        }));
    }

    Ok(Some(PathBuf::from(path)))
}

fn apply_request_timeout(config: &mut Config, timeout_secs: u64) {
    let timeout = Some(Duration::from_secs(timeout_secs.max(1)));
    config.connect_timeout = timeout;
    config.read_timeout = timeout;
    config.write_timeout = timeout;
}

fn load_ca_bundle(path: &str) -> Result<Vec<Vec<u8>>> {
    let bytes =
        std::fs::read(path).with_context(|| format!("Failed to read CA certificate '{}'", path))?;
    let certificates = pem::parse_many(bytes)
        .context("Failed to parse CA certificate PEM")?
        .into_iter()
        .filter(|entry| entry.tag() == "CERTIFICATE")
        .map(|entry| entry.into_contents())
        .collect::<Vec<_>>();
    if certificates.is_empty() {
        anyhow::bail!("CA certificate file contains no CERTIFICATE entries");
    }
    Ok(certificates)
}

/// List kubeconfig contexts
pub fn list_contexts(config: &K8sConfig) -> Result<Vec<String>> {
    let kube_config = match &config.connection {
        K8sConnection::Kubeconfig { path, .. } => read_kubeconfig(path.as_deref())?,
        K8sConnection::Direct { .. } => return Ok(vec![]),
    };

    let contexts = kube_config
        .contexts
        .iter()
        .map(|c| c.name.clone())
        .collect();
    Ok(contexts)
}

/// Test connection — returns cluster version string
pub async fn test_connection(client: &Client) -> Result<String> {
    let version = client
        .apiserver_version()
        .await
        .context("Failed to reach API server")?;
    Ok(format!(
        "Kubernetes {} (git: {})",
        version.git_version,
        version.git_commit.chars().take(8).collect::<String>()
    ))
}

/// List namespaces
pub async fn list_namespaces(client: &Client) -> Result<Vec<String>> {
    let ns_api: Api<Namespace> = Api::all(client.clone());
    let ns_list = ns_api
        .list(&ListParams::default())
        .await
        .context("Failed to list namespaces")?;
    let names = ns_list.items.iter().map(|n| n.name_any()).collect();
    Ok(names)
}

/// List pods in a namespace
pub async fn list_pods(client: &Client, namespace: &str) -> Result<Vec<PodInfo>> {
    let pod_api: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let pod_list = pod_api
        .list(&ListParams::default())
        .await
        .context("Failed to list pods")?;

    let pods = pod_list
        .items
        .iter()
        .map(|pod| {
            let name = pod.name_any();
            let ns = pod.namespace().unwrap_or_else(|| namespace.to_string());

            // Determine status
            let (status_str, phase) = if let Some(status) = &pod.status {
                let phase = status
                    .phase
                    .clone()
                    .unwrap_or_else(|| "Unknown".to_string());

                // Check for terminating
                let terminating = pod.metadata.deletion_timestamp.is_some();
                if terminating {
                    ("Terminating".to_string(), phase)
                } else {
                    (phase.clone(), phase)
                }
            } else {
                ("Unknown".to_string(), "Unknown".to_string())
            };

            let status = PodStatus::from_str(&status_str);

            // Ready string: count running containers / total
            let (ready_count, total_count) = if let Some(s) = &pod.status {
                let total = s
                    .container_statuses
                    .as_ref()
                    .map(|cs| cs.len())
                    .unwrap_or(0);
                let ready = s
                    .container_statuses
                    .as_ref()
                    .map(|cs| cs.iter().filter(|c| c.ready).count())
                    .unwrap_or(0);
                (ready, total)
            } else {
                (0, 0)
            };
            let ready = format!("{}/{}", ready_count, total_count);

            // Restart count
            let restarts = pod
                .status
                .as_ref()
                .and_then(|s| s.container_statuses.as_ref())
                .map(|cs| cs.iter().map(|c| c.restart_count as u32).sum::<u32>())
                .unwrap_or(0);

            // Age
            let age = pod
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            // Node
            let node = pod.spec.as_ref().and_then(|s| s.node_name.clone());

            // Container names
            let containers = pod
                .spec
                .as_ref()
                .map(|s| s.containers.iter().map(|c| c.name.clone()).collect())
                .unwrap_or_default();

            let _ = phase; // used above
            PodInfo {
                name,
                namespace: ns,
                status,
                ready,
                restarts,
                age,
                node,
                containers,
            }
        })
        .collect();

    Ok(pods)
}

/// List services in a namespace
pub async fn list_services(client: &Client, namespace: &str) -> Result<Vec<ServiceInfo>> {
    let svc_api: Api<Service> = Api::namespaced(client.clone(), namespace);
    let svc_list = svc_api
        .list(&ListParams::default())
        .await
        .context("Failed to list services")?;

    let services = svc_list
        .items
        .iter()
        .map(|svc| {
            let name = svc.name_any();
            let ns = svc.namespace().unwrap_or_else(|| namespace.to_string());

            let service_type = svc
                .spec
                .as_ref()
                .and_then(|s| s.type_.clone())
                .unwrap_or_else(|| "ClusterIP".to_string());

            let cluster_ip = svc
                .spec
                .as_ref()
                .and_then(|s| s.cluster_ip.clone())
                .unwrap_or_else(|| "None".to_string());

            let ports = svc
                .spec
                .as_ref()
                .and_then(|s| s.ports.as_ref())
                .map(|ps| {
                    ps.iter()
                        .map(|p| {
                            let proto = p.protocol.as_deref().unwrap_or("TCP");
                            if let Some(node_port) = p.node_port {
                                format!("{}:{}/{}", p.port, node_port, proto)
                            } else {
                                format!("{}/{}", p.port, proto)
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let age = svc
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            ServiceInfo {
                name,
                namespace: ns,
                service_type,
                cluster_ip,
                ports,
                age,
            }
        })
        .collect();

    Ok(services)
}

/// List deployments in a namespace
pub async fn list_deployments(client: &Client, namespace: &str) -> Result<Vec<DeploymentInfo>> {
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let deploy_list = deploy_api
        .list(&ListParams::default())
        .await
        .context("Failed to list deployments")?;

    let deployments = deploy_list
        .items
        .iter()
        .map(|d| {
            let name = d.name_any();
            let ns = d.namespace().unwrap_or_else(|| namespace.to_string());

            let desired = d.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
            let ready_count = d
                .status
                .as_ref()
                .and_then(|s| s.ready_replicas)
                .unwrap_or(0);
            let up_to_date = d
                .status
                .as_ref()
                .and_then(|s| s.updated_replicas)
                .unwrap_or(0);
            let available = d
                .status
                .as_ref()
                .and_then(|s| s.available_replicas)
                .unwrap_or(0);

            let ready = format!("{}/{}", ready_count, desired);

            let age = d
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            DeploymentInfo {
                name,
                namespace: ns,
                ready,
                up_to_date: up_to_date as i64,
                available: available as i64,
                age,
            }
        })
        .collect();

    Ok(deployments)
}

/// List statefulsets in a namespace
pub async fn list_statefulsets(client: &Client, namespace: &str) -> Result<Vec<StatefulSetInfo>> {
    let sts_api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
    let sts_list = sts_api
        .list(&ListParams::default())
        .await
        .context("Failed to list statefulsets")?;

    let statefulsets = sts_list
        .items
        .iter()
        .map(|s| {
            let name = s.name_any();
            let ns = s.namespace().unwrap_or_else(|| namespace.to_string());

            let desired = s.spec.as_ref().and_then(|sp| sp.replicas).unwrap_or(0);
            let ready_count = s
                .status
                .as_ref()
                .and_then(|st| st.ready_replicas)
                .unwrap_or(0);

            let ready = format!("{}/{}", ready_count, desired);

            let age = s
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            StatefulSetInfo {
                name,
                namespace: ns,
                ready,
                age,
            }
        })
        .collect();

    Ok(statefulsets)
}

/// List daemonsets in a namespace
pub async fn list_daemonsets(client: &Client, namespace: &str) -> Result<Vec<DaemonSetInfo>> {
    let ds_api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
    let ds_list = ds_api
        .list(&ListParams::default())
        .await
        .context("Failed to list daemonsets")?;

    let daemonsets = ds_list
        .items
        .iter()
        .map(|d| {
            let name = d.name_any();
            let ns = d.namespace().unwrap_or_else(|| namespace.to_string());

            let desired = d
                .status
                .as_ref()
                .map(|s| s.desired_number_scheduled as i64)
                .unwrap_or(0);
            let current = d
                .status
                .as_ref()
                .map(|s| s.current_number_scheduled as i64)
                .unwrap_or(0);
            let ready = d
                .status
                .as_ref()
                .map(|s| s.number_ready as i64)
                .unwrap_or(0);

            let age = d
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            DaemonSetInfo {
                name,
                namespace: ns,
                desired,
                current,
                ready,
                age,
            }
        })
        .collect();

    Ok(daemonsets)
}

/// List jobs in a namespace
pub async fn list_jobs(client: &Client, namespace: &str) -> Result<Vec<JobInfo>> {
    let job_api: Api<Job> = Api::namespaced(client.clone(), namespace);
    let job_list = job_api
        .list(&ListParams::default())
        .await
        .context("Failed to list jobs")?;

    let jobs = job_list
        .items
        .iter()
        .map(|j| {
            let name = j.name_any();
            let ns = j.namespace().unwrap_or_else(|| namespace.to_string());

            let desired = j.spec.as_ref().and_then(|s| s.completions).unwrap_or(1);
            let succeeded = j.status.as_ref().and_then(|s| s.succeeded).unwrap_or(0);

            let completions = format!("{}/{}", succeeded, desired);

            // Determine job status
            let status = if let Some(st) = &j.status {
                if let Some(conditions) = &st.conditions {
                    if conditions
                        .iter()
                        .any(|c| c.type_ == "Complete" && c.status == "True")
                    {
                        JobStatus::Complete
                    } else if conditions
                        .iter()
                        .any(|c| c.type_ == "Failed" && c.status == "True")
                    {
                        JobStatus::Failed
                    } else if conditions
                        .iter()
                        .any(|c| c.type_ == "Suspended" && c.status == "True")
                    {
                        JobStatus::Suspended
                    } else if st.active.unwrap_or(0) > 0 {
                        JobStatus::Running
                    } else {
                        JobStatus::Unknown
                    }
                } else if st.active.unwrap_or(0) > 0 {
                    JobStatus::Running
                } else {
                    JobStatus::Unknown
                }
            } else {
                JobStatus::Unknown
            };

            // Duration from start to completion (or now)
            let duration = if let Some(st) = &j.status {
                let start = st.start_time.as_ref().map(|t| t.0);
                let end = st.completion_time.as_ref().map(|t| t.0);
                match (start, end) {
                    (Some(s), Some(e)) => {
                        let secs = e.signed_duration_since(s).num_seconds();
                        format_duration(secs)
                    }
                    (Some(s), None) => {
                        let secs = chrono::Utc::now().signed_duration_since(s).num_seconds();
                        format_duration(secs)
                    }
                    _ => "-".to_string(),
                }
            } else {
                "-".to_string()
            };

            let age = j
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            JobInfo {
                name,
                namespace: ns,
                completions,
                duration,
                age,
                status,
            }
        })
        .collect();

    Ok(jobs)
}

/// List cronjobs in a namespace
pub async fn list_cronjobs(client: &Client, namespace: &str) -> Result<Vec<CronJobInfo>> {
    let cj_api: Api<CronJob> = Api::namespaced(client.clone(), namespace);
    let cj_list = cj_api
        .list(&ListParams::default())
        .await
        .context("Failed to list cronjobs")?;

    let cronjobs = cj_list
        .items
        .iter()
        .map(|cj| {
            let name = cj.name_any();
            let ns = cj.namespace().unwrap_or_else(|| namespace.to_string());

            let schedule = cj
                .spec
                .as_ref()
                .map(|s| s.schedule.clone())
                .unwrap_or_default();

            let timezone = cj
                .spec
                .as_ref()
                .and_then(|s| s.time_zone.clone())
                .unwrap_or_else(|| "-".to_string());

            let active = cj
                .status
                .as_ref()
                .and_then(|s| s.active.as_ref())
                .map(|a| a.len() as i32)
                .unwrap_or(0);

            let last_schedule = cj
                .status
                .as_ref()
                .and_then(|s| s.last_schedule_time.as_ref())
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "-".to_string());

            let age = cj
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            CronJobInfo {
                name,
                namespace: ns,
                schedule,
                timezone,
                active,
                last_schedule,
                age,
            }
        })
        .collect();

    Ok(cronjobs)
}

/// List persistent volume claims in a namespace
pub async fn list_pvcs(client: &Client, namespace: &str) -> Result<Vec<PvcInfo>> {
    let pvc_api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
    let pvc_list = pvc_api
        .list(&ListParams::default())
        .await
        .context("Failed to list PVCs")?;

    let pvcs = pvc_list
        .items
        .iter()
        .map(|pvc| {
            let name = pvc.name_any();
            let ns = pvc.namespace().unwrap_or_else(|| namespace.to_string());

            let status = pvc
                .status
                .as_ref()
                .and_then(|s| s.phase.clone())
                .unwrap_or_else(|| "Unknown".to_string());

            let volume = pvc
                .spec
                .as_ref()
                .and_then(|s| s.volume_name.clone())
                .unwrap_or_else(|| "-".to_string());

            let capacity = pvc
                .status
                .as_ref()
                .and_then(|s| s.capacity.as_ref())
                .and_then(|c| c.get("storage"))
                .map(|q| q.0.clone())
                .unwrap_or_else(|| "-".to_string());

            let access_modes = pvc
                .spec
                .as_ref()
                .and_then(|s| s.access_modes.as_ref())
                .map(|am| am.join(","))
                .unwrap_or_default();

            let storage_class = pvc
                .spec
                .as_ref()
                .and_then(|s| s.storage_class_name.clone())
                .unwrap_or_else(|| "-".to_string());

            let age = pvc
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            PvcInfo {
                name,
                namespace: ns,
                status,
                volume,
                capacity,
                access_modes,
                storage_class,
                age,
            }
        })
        .collect();

    Ok(pvcs)
}

/// List ingresses in a namespace
pub async fn list_ingresses(client: &Client, namespace: &str) -> Result<Vec<IngressInfo>> {
    let ing_api: Api<Ingress> = Api::namespaced(client.clone(), namespace);
    let ing_list = ing_api
        .list(&ListParams::default())
        .await
        .context("Failed to list ingresses")?;

    let ingresses = ing_list
        .items
        .iter()
        .map(|ing| {
            let name = ing.name_any();
            let ns = ing.namespace().unwrap_or_else(|| namespace.to_string());

            let class = ing
                .spec
                .as_ref()
                .and_then(|s| s.ingress_class_name.clone())
                .unwrap_or_else(|| "-".to_string());

            let hosts: Vec<String> = ing
                .spec
                .as_ref()
                .and_then(|s| s.rules.as_ref())
                .map(|rules| rules.iter().filter_map(|r| r.host.clone()).collect())
                .unwrap_or_default();

            let addresses: Vec<String> = ing
                .status
                .as_ref()
                .and_then(|s| s.load_balancer.as_ref())
                .and_then(|lb| lb.ingress.as_ref())
                .map(|ingresses| {
                    ingresses
                        .iter()
                        .filter_map(|i| i.ip.clone().or_else(|| i.hostname.clone()))
                        .collect()
                })
                .unwrap_or_default();

            let age = ing
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            IngressInfo {
                name,
                namespace: ns,
                class,
                hosts,
                addresses,
                age,
            }
        })
        .collect();

    Ok(ingresses)
}

/// List service accounts in a namespace
pub async fn list_service_accounts(
    client: &Client,
    namespace: &str,
) -> Result<Vec<ServiceAccountInfo>> {
    let sa_api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
    let sa_list = sa_api
        .list(&ListParams::default())
        .await
        .context("Failed to list service accounts")?;

    let sas = sa_list
        .items
        .iter()
        .map(|sa| {
            let name = sa.name_any();
            let ns = sa.namespace().unwrap_or_else(|| namespace.to_string());

            let secrets_count = sa.secrets.as_ref().map(|s| s.len()).unwrap_or(0);

            let age = sa
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            ServiceAccountInfo {
                name,
                namespace: ns,
                secrets_count,
                age,
            }
        })
        .collect();

    Ok(sas)
}

/// List configmaps in a namespace
pub async fn list_configmaps(client: &Client, namespace: &str) -> Result<Vec<ConfigMapInfo>> {
    let cm_api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
    let cm_list = cm_api
        .list(&ListParams::default())
        .await
        .context("Failed to list configmaps")?;

    let cms = cm_list
        .items
        .iter()
        .map(|cm| {
            let name = cm.name_any();
            let ns = cm.namespace().unwrap_or_else(|| namespace.to_string());
            let data_count = cm.data.as_ref().map(|d| d.len()).unwrap_or(0);
            let age = cm
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());
            ConfigMapInfo {
                name,
                namespace: ns,
                data_count,
                age,
            }
        })
        .collect();

    Ok(cms)
}

/// List secrets in a namespace
pub async fn list_secrets(client: &Client, namespace: &str) -> Result<Vec<SecretInfo>> {
    let secret_api: Api<Secret> = Api::namespaced(client.clone(), namespace);
    let secret_list = secret_api
        .list(&ListParams::default())
        .await
        .context("Failed to list secrets")?;

    let secrets = secret_list
        .items
        .iter()
        .map(|s| {
            let name = s.name_any();
            let ns = s.namespace().unwrap_or_else(|| namespace.to_string());
            let secret_type = s.type_.clone().unwrap_or_else(|| "Opaque".to_string());
            let data_count = s.data.as_ref().map(|d| d.len()).unwrap_or(0);
            let age = s
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());
            SecretInfo {
                name,
                namespace: ns,
                secret_type,
                data_count,
                age,
            }
        })
        .collect();

    Ok(secrets)
}

/// List nodes
pub async fn list_nodes(client: &Client) -> Result<Vec<NodeInfo>> {
    let node_api: Api<Node> = Api::all(client.clone());
    let node_list = node_api
        .list(&ListParams::default())
        .await
        .context("Failed to list nodes")?;

    let nodes = node_list
        .items
        .iter()
        .map(|n| {
            let name = n.name_any();

            // Status: Ready condition
            let status = n
                .status
                .as_ref()
                .and_then(|s| s.conditions.as_ref())
                .and_then(|cs| {
                    cs.iter().find(|c| c.type_ == "Ready").map(|c| {
                        if c.status == "True" {
                            "Ready".to_string()
                        } else {
                            "NotReady".to_string()
                        }
                    })
                })
                .unwrap_or_else(|| "Unknown".to_string());

            // Roles from labels
            let roles: Vec<String> = n
                .metadata
                .labels
                .as_ref()
                .map(|lbls| {
                    lbls.keys()
                        .filter_map(|k| {
                            if k.starts_with("node-role.kubernetes.io/") {
                                Some(k.trim_start_matches("node-role.kubernetes.io/").to_string())
                            } else {
                                None
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let version = n
                .status
                .as_ref()
                .and_then(|s| s.node_info.as_ref())
                .map(|i| i.kubelet_version.clone())
                .unwrap_or_default();

            let age = n
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());

            NodeInfo {
                name,
                status,
                roles,
                version,
                age,
            }
        })
        .collect();

    Ok(nodes)
}

/// List events in a namespace
pub async fn list_events(client: &Client, namespace: &str) -> Result<Vec<EventInfo>> {
    let event_api: Api<Event> = Api::namespaced(client.clone(), namespace);
    let event_list = event_api
        .list(&ListParams::default())
        .await
        .context("Failed to list events")?;

    let mut events: Vec<EventInfo> = event_list
        .items
        .iter()
        .map(|e| {
            let event_type = e.type_.clone().unwrap_or_else(|| "Normal".to_string());
            let reason = e.reason.clone().unwrap_or_default();
            let object = e.involved_object.name.clone().unwrap_or_default();
            let message = e.message.clone().unwrap_or_default();
            let age = e
                .metadata
                .creation_timestamp
                .as_ref()
                .map(|t| format_age(&t.0))
                .unwrap_or_else(|| "?".to_string());
            let count = e.count.unwrap_or(1) as u32;

            EventInfo {
                event_type,
                reason,
                object,
                message,
                age,
                count,
            }
        })
        .collect();

    // Sort: warnings first, then by age (newest first)
    events.sort_by(|a, b| {
        let a_warn = a.event_type == "Warning";
        let b_warn = b.event_type == "Warning";
        b_warn.cmp(&a_warn)
    });

    Ok(events)
}

/// Get resource YAML by type and name
pub async fn get_resource_yaml(
    client: &Client,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<String> {
    let value: serde_json::Value = match resource_type.to_lowercase().as_str() {
        "pod" | "pods" | "po" => {
            let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get pod")?;
            serde_json::to_value(obj)?
        }
        "service" | "services" | "svc" => {
            let api: Api<Service> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get service")?;
            serde_json::to_value(obj)?
        }
        "deployment" | "deployments" | "deploy" => {
            let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get deployment")?;
            serde_json::to_value(obj)?
        }
        "statefulset" | "statefulsets" | "sts" => {
            let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get statefulset")?;
            serde_json::to_value(obj)?
        }
        "daemonset" | "daemonsets" | "ds" => {
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get daemonset")?;
            serde_json::to_value(obj)?
        }
        "job" | "jobs" => {
            let api: Api<Job> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get job")?;
            serde_json::to_value(obj)?
        }
        "cronjob" | "cronjobs" | "cj" => {
            let api: Api<CronJob> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get cronjob")?;
            serde_json::to_value(obj)?
        }
        "pvc" | "pvcs" | "persistentvolumeclaim" => {
            let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get PVC")?;
            serde_json::to_value(obj)?
        }
        "ingress" | "ingresses" | "ing" => {
            let api: Api<Ingress> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get ingress")?;
            serde_json::to_value(obj)?
        }
        "serviceaccount" | "serviceaccounts" | "sa" => {
            let api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
            let obj = api
                .get(name)
                .await
                .context("Failed to get service account")?;
            serde_json::to_value(obj)?
        }
        "configmap" | "configmaps" | "cm" => {
            let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get configmap")?;
            serde_json::to_value(obj)?
        }
        "secret" | "secrets" => {
            let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
            let obj = api.get(name).await.context("Failed to get secret")?;
            serde_json::to_value(obj)?
        }
        "node" | "nodes" => {
            let api: Api<Node> = Api::all(client.clone());
            let obj = api.get(name).await.context("Failed to get node")?;
            serde_json::to_value(obj)?
        }
        _ => return Err(anyhow::anyhow!("Unknown resource type: {}", resource_type)),
    };

    serde_yaml::to_string(&value).context("Failed to convert to YAML")
}

/// Delete a resource by type and name
pub async fn delete_resource(
    client: &Client,
    resource_type: &str,
    name: &str,
    namespace: &str,
) -> Result<()> {
    let dp = DeleteParams::default();

    match resource_type.to_lowercase().as_str() {
        "pod" | "pods" | "po" => {
            let api: Api<Pod> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete pod")?;
        }
        "service" | "services" | "svc" => {
            let api: Api<Service> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete service")?;
        }
        "deployment" | "deployments" | "deploy" => {
            let api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete deployment")?;
        }
        "statefulset" | "statefulsets" | "sts" => {
            let api: Api<StatefulSet> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete statefulset")?;
        }
        "daemonset" | "daemonsets" | "ds" => {
            let api: Api<DaemonSet> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete daemonset")?;
        }
        "job" | "jobs" => {
            let api: Api<Job> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete job")?;
        }
        "cronjob" | "cronjobs" | "cj" => {
            let api: Api<CronJob> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete cronjob")?;
        }
        "pvc" | "pvcs" | "persistentvolumeclaim" => {
            let api: Api<PersistentVolumeClaim> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete PVC")?;
        }
        "ingress" | "ingresses" | "ing" => {
            let api: Api<Ingress> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete ingress")?;
        }
        "serviceaccount" | "serviceaccounts" | "sa" => {
            let api: Api<ServiceAccount> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete service account")?;
        }
        "configmap" | "configmaps" | "cm" => {
            let api: Api<ConfigMap> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete configmap")?;
        }
        "secret" | "secrets" => {
            let api: Api<Secret> = Api::namespaced(client.clone(), namespace);
            api.delete(name, &dp)
                .await
                .context("Failed to delete secret")?;
        }
        _ => {
            return Err(anyhow::anyhow!(
                "Cannot delete resource type: {}",
                resource_type
            ));
        }
    }

    Ok(())
}

/// Scale a deployment to the given number of replicas
pub async fn scale_deployment(
    client: &Client,
    name: &str,
    namespace: &str,
    replicas: u32,
) -> Result<()> {
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({
        "spec": { "replicas": replicas }
    });
    deploy_api
        .patch(name, &PatchParams::apply("voidb"), &Patch::Merge(&patch))
        .await
        .context("Failed to scale deployment")?;
    Ok(())
}

/// Restart a deployment by updating the restart annotation
pub async fn restart_deployment(client: &Client, name: &str, namespace: &str) -> Result<()> {
    let deploy_api: Api<Deployment> = Api::namespaced(client.clone(), namespace);
    let now = chrono::Utc::now().to_rfc3339();
    let patch = serde_json::json!({
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": now
                    }
                }
            }
        }
    });
    deploy_api
        .patch(name, &PatchParams::apply("voidb"), &Patch::Merge(&patch))
        .await
        .context("Failed to restart deployment")?;
    Ok(())
}

/// Stream pod logs, sending lines to a channel
pub async fn stream_pod_logs(
    client: Client,
    pod: String,
    namespace: String,
    container: Option<String>,
    follow: bool,
    tail_lines: Option<i64>,
    tx: mpsc::UnboundedSender<K8sEvent>,
) {
    let pod_api: Api<Pod> = Api::namespaced(client, &namespace);
    let params = LogParams {
        container: container.clone(),
        follow,
        tail_lines,
        timestamps: true,
        ..Default::default()
    };

    if follow {
        match pod_api.log_stream(&pod, &params).await {
            Ok(stream) => {
                // log_stream returns impl futures::AsyncBufRead
                use futures::StreamExt as FStreamExt;
                use futures::io::AsyncBufReadExt as FAsyncBufRead;
                let reader = futures::io::BufReader::new(stream);
                let mut log_lines = FAsyncBufRead::lines(reader);
                while let Some(result) = FStreamExt::next(&mut log_lines).await {
                    match result {
                        Ok(line) => {
                            if tx.send(K8sEvent::LogLine { text: line }).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(K8sEvent::Error(format!("Log stream error: {}", e)));
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(K8sEvent::Error(format!(
                    "Failed to start log stream: {}",
                    e
                )));
            }
        }
    } else {
        match pod_api.logs(&pod, &params).await {
            Ok(logs) => {
                for line in logs.lines() {
                    if tx
                        .send(K8sEvent::LogLine {
                            text: line.to_string(),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            }
            Err(e) => {
                let _ = tx.send(K8sEvent::Error(format!("Failed to get logs: {}", e)));
            }
        }
    }
}

/// Start an interactive exec session in a pod container.
/// Returns a sender for stdin input; stdout/stderr bytes are sent via output_tx.
pub async fn exec_pod(
    client: Client,
    pod: String,
    namespace: String,
    container: Option<String>,
    output_tx: mpsc::UnboundedSender<Vec<u8>>,
) -> Result<mpsc::UnboundedSender<Vec<u8>>> {
    let pod_api: Api<Pod> = Api::namespaced(client, &namespace);

    let mut ap = AttachParams::interactive_tty()
        .stdin(true)
        .stdout(true)
        .stderr(false); // stderr not available with TTY

    if let Some(container_name) = container {
        ap = ap.container(container_name);
    }

    let mut attached = pod_api
        .exec(&pod, vec!["sh"], &ap)
        .await
        .context("Failed to exec into pod")?;

    // Stdin writer
    let stdin_writer = attached
        .stdin()
        .ok_or_else(|| anyhow::anyhow!("No stdin"))?;

    // Stdout/stderr reader using futures::AsyncRead
    let mut stdout_stream = attached
        .stdout()
        .ok_or_else(|| anyhow::anyhow!("No stdout"))?;

    // Spawn stdout reader task
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 4096];
        loop {
            match stdout_stream.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => {
                    if output_tx.send(buf[..n].to_vec()).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        // Wait for the attached session to end
        let _ = attached.join().await;
    });

    // Stdin channel
    let (input_tx, mut input_rx) = mpsc::unbounded_channel::<Vec<u8>>();

    tokio::spawn(async move {
        let mut writer = stdin_writer;
        while let Some(data) = input_rx.recv().await {
            if writer.write_all(&data).await.is_err() {
                break;
            }
            if writer.flush().await.is_err() {
                break;
            }
        }
    });

    Ok(input_tx)
}

/// Apply a YAML manifest (server-side apply via patch)
pub async fn apply_yaml(
    client: &Client,
    yaml_str: &str,
    default_namespace: &str,
) -> Result<String> {
    // Parse the YAML document
    let value: serde_json::Value =
        serde_yaml::from_str(yaml_str).context("Failed to parse YAML")?;

    // Extract GVK and name (owned Strings so value can be moved later)
    let api_version = value["apiVersion"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing apiVersion in YAML"))?
        .to_string();
    let kind = value["kind"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing kind in YAML"))?
        .to_string();
    let name = value["metadata"]["name"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Missing metadata.name in YAML"))?
        .to_string();
    let namespace = value["metadata"]["namespace"]
        .as_str()
        .unwrap_or(default_namespace)
        .to_string();

    // Split apiVersion into group and version
    let (group, version) = if api_version.contains('/') {
        let parts: Vec<&str> = api_version.splitn(2, '/').collect();
        (parts[0].to_string(), parts[1].to_string())
    } else {
        // Core group (e.g. "v1")
        (String::new(), api_version.to_string())
    };

    // Discover the API resource for this GVK
    let discovery = Discovery::new(client.clone())
        .run()
        .await
        .context("Failed to run API discovery")?;

    let (ar, caps) = discovery
        .groups()
        .find_map(|g| {
            let group_name = g.name();
            let matches_group = if group.is_empty() {
                group_name == "core" || group_name.is_empty()
            } else {
                group_name == group
            };
            if !matches_group {
                return None;
            }
            g.versioned_resources(&version)
                .into_iter()
                .find(|(ar, _caps)| ar.kind == kind)
        })
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Unknown resource kind '{}' in group '{}' version '{}'",
                kind,
                group,
                version
            )
        })?;

    let patch_params = PatchParams::apply("voidb").force();

    let result = if caps.scope == kube::discovery::Scope::Namespaced {
        let api: Api<DynamicObject> = Api::namespaced_with(client.clone(), &namespace, &ar);
        let obj: DynamicObject =
            serde_json::from_value(value).context("Failed to deserialize resource")?;
        api.patch(&name, &patch_params, &Patch::Apply(&obj))
            .await
            .context("Failed to apply resource")?;
        format!("{}/{} configured (namespace: {})", kind, name, namespace)
    } else {
        let api: Api<DynamicObject> = Api::all_with(client.clone(), &ar);
        let obj: DynamicObject =
            serde_json::from_value(value).context("Failed to deserialize resource")?;
        api.patch(&name, &patch_params, &Patch::Apply(&obj))
            .await
            .context("Failed to apply resource")?;
        format!("{}/{} configured", kind, name)
    };

    Ok(result)
}

// ── Metrics API (metrics.k8s.io/v1beta1) ────────────────────────────────

/// Serde structs for metrics.k8s.io (not in k8s-openapi)
mod metrics {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct MetricsList<T> {
        pub items: Vec<T>,
    }

    #[derive(Deserialize)]
    pub struct PodMetricsItem {
        pub metadata: Metadata,
        pub containers: Vec<ContainerMetrics>,
    }

    #[derive(Deserialize)]
    pub struct NodeMetricsItem {
        pub metadata: Metadata,
        pub usage: ResourceUsage,
    }

    #[derive(Deserialize)]
    pub struct Metadata {
        pub name: String,
    }

    #[derive(Deserialize)]
    pub struct ContainerMetrics {
        pub usage: ResourceUsage,
    }

    #[derive(Deserialize)]
    pub struct ResourceUsage {
        pub cpu: String,
        pub memory: String,
    }
}

/// Format Kubernetes CPU quantity (e.g. "250m", "1500000000n") to millicores
fn format_cpu(raw: &str) -> String {
    if let Some(nanos) = raw.strip_suffix('n')
        && let Ok(n) = nanos.parse::<u64>()
    {
        return format!("{}m", n / 1_000_000);
    }
    if let Some(millis) = raw.strip_suffix('m') {
        return format!("{}m", millis);
    }
    // Bare number = cores
    if let Ok(cores) = raw.parse::<f64>() {
        return format!("{}m", (cores * 1000.0) as u64);
    }
    raw.to_string()
}

/// Format Kubernetes memory quantity (e.g. "123456Ki") to Mi
fn format_memory(raw: &str) -> String {
    if let Some(ki) = raw.strip_suffix("Ki")
        && let Ok(k) = ki.parse::<u64>()
    {
        return format!("{}Mi", k / 1024);
    }
    if let Some(mi) = raw.strip_suffix("Mi") {
        return format!("{}Mi", mi);
    }
    if let Some(gi) = raw.strip_suffix("Gi") {
        return format!("{}Gi", gi);
    }
    // Bare bytes
    if let Ok(b) = raw.parse::<u64>() {
        return format!("{}Mi", b / (1024 * 1024));
    }
    raw.to_string()
}

/// Fetch pod metrics from metrics.k8s.io API
/// Returns Vec<(pod_name, cpu_formatted, memory_formatted)>
pub async fn fetch_pod_metrics(
    client: &Client,
    namespace: &str,
) -> Result<Vec<(String, String, String)>> {
    let url = format!("/apis/metrics.k8s.io/v1beta1/namespaces/{}/pods", namespace);
    let request = http::Request::get(&url)
        .body(Vec::new())
        .context("Failed to build metrics request")?;

    let response = client
        .request::<metrics::MetricsList<metrics::PodMetricsItem>>(request)
        .await
        .context("Failed to fetch pod metrics")?;

    let result = response
        .items
        .iter()
        .map(|item| {
            // Sum CPU and memory across all containers
            let total_cpu_nanos: u64 = item
                .containers
                .iter()
                .map(|c| parse_cpu_nanos(&c.usage.cpu))
                .sum();
            let total_mem_ki: u64 = item
                .containers
                .iter()
                .map(|c| parse_memory_ki(&c.usage.memory))
                .sum();

            let cpu = format!("{}m", total_cpu_nanos / 1_000_000);
            let mem = format!("{}Mi", total_mem_ki / 1024);
            (item.metadata.name.clone(), cpu, mem)
        })
        .collect();

    Ok(result)
}

/// Fetch node metrics from metrics.k8s.io API
pub async fn fetch_node_metrics(client: &Client) -> Result<Vec<(String, String, String)>> {
    let request = http::Request::get("/apis/metrics.k8s.io/v1beta1/nodes")
        .body(Vec::new())
        .context("Failed to build node metrics request")?;

    let response = client
        .request::<metrics::MetricsList<metrics::NodeMetricsItem>>(request)
        .await
        .context("Failed to fetch node metrics")?;

    let result = response
        .items
        .iter()
        .map(|item| {
            let cpu = format_cpu(&item.usage.cpu);
            let mem = format_memory(&item.usage.memory);
            (item.metadata.name.clone(), cpu, mem)
        })
        .collect();

    Ok(result)
}

/// Parse CPU string to nanocores
fn parse_cpu_nanos(raw: &str) -> u64 {
    if let Some(nanos) = raw.strip_suffix('n') {
        return nanos.parse::<u64>().unwrap_or(0);
    }
    if let Some(millis) = raw.strip_suffix('m') {
        return millis.parse::<u64>().unwrap_or(0) * 1_000_000;
    }
    // Bare number = cores
    if let Ok(cores) = raw.parse::<f64>() {
        return (cores * 1_000_000_000.0) as u64;
    }
    0
}

/// Parse memory string to Ki
fn parse_memory_ki(raw: &str) -> u64 {
    if let Some(ki) = raw.strip_suffix("Ki") {
        return ki.parse::<u64>().unwrap_or(0);
    }
    if let Some(mi) = raw.strip_suffix("Mi") {
        return mi.parse::<u64>().unwrap_or(0) * 1024;
    }
    if let Some(gi) = raw.strip_suffix("Gi") {
        return gi.parse::<u64>().unwrap_or(0) * 1024 * 1024;
    }
    // Bare bytes
    if let Ok(b) = raw.parse::<u64>() {
        return b / 1024;
    }
    0
}

/// Format seconds into a human-readable duration string
fn format_duration(secs: i64) -> String {
    let secs = secs.max(0);
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m{}s", secs / 60, secs % 60)
    } else {
        format!("{}h{}m", secs / 3600, (secs % 3600) / 60)
    }
}

/// Format a chrono DateTime into a relative age string
pub fn format_age(dt: &chrono::DateTime<chrono::Utc>) -> String {
    let now = chrono::Utc::now();
    let diff = now.signed_duration_since(*dt);
    let secs = diff.num_seconds();

    if secs < 60 {
        format!("{}s", secs.max(0))
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else if secs < 86400 {
        format!("{}h", secs / 3600)
    } else {
        format!("{}d", secs / 86400)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_timeout_applies_to_all_client_phases() {
        let mut config = Config::new("https://cluster.example.test".parse().unwrap());
        apply_request_timeout(&mut config, 7);

        let expected = Some(Duration::from_secs(7));
        assert_eq!(config.connect_timeout, expected);
        assert_eq!(config.read_timeout, expected);
        assert_eq!(config.write_timeout, expected);
    }

    #[test]
    fn request_timeout_never_becomes_unbounded() {
        let mut config = Config::new("https://cluster.example.test".parse().unwrap());
        apply_request_timeout(&mut config, 0);

        assert_eq!(config.connect_timeout, Some(Duration::from_secs(1)));
        assert_eq!(config.read_timeout, Some(Duration::from_secs(1)));
        assert_eq!(config.write_timeout, Some(Duration::from_secs(1)));
    }

    #[test]
    fn ca_bundle_keeps_only_certificate_entries() {
        let path = std::env::temp_dir().join(format!(
            "voidb-kubernetes-ca-test-{}.pem",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(
            &path,
            "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n\
             -----BEGIN PRIVATE KEY-----\nBAUG\n-----END PRIVATE KEY-----\n",
        )
        .unwrap();

        let certificates = load_ca_bundle(path.to_str().unwrap()).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(certificates, vec![vec![1, 2, 3]]);
    }

    #[test]
    fn kubeconfig_path_treats_blank_values_as_default_discovery() {
        assert_eq!(normalize_kubeconfig_path(None).unwrap(), None);
        assert_eq!(normalize_kubeconfig_path(Some("")).unwrap(), None);
        assert_eq!(normalize_kubeconfig_path(Some("  \t ")).unwrap(), None);
    }

    #[test]
    fn kubeconfig_path_expands_ascii_and_fullwidth_tildes() {
        let home = dirs::home_dir().expect("home directory");
        for path in ["~/.kube/config", "～/.kube/config"] {
            assert_eq!(
                normalize_kubeconfig_path(Some(path)).unwrap(),
                Some(home.join(".kube/config"))
            );
        }
    }

    #[test]
    fn explicit_absolute_kubeconfig_path_is_read() {
        let path = std::env::temp_dir().join(format!(
            "voidb-kubernetes-config-test-{}.yaml",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(
            &path,
            "apiVersion: v1\nkind: Config\ncurrent-context: local\nclusters: []\ncontexts: []\nusers: []\n",
        )
        .unwrap();

        let padded = format!("  {}  ", path.display());
        let config = read_kubeconfig(Some(&padded)).unwrap();
        let _ = std::fs::remove_file(&path);

        assert_eq!(config.current_context.as_deref(), Some("local"));
    }
}
