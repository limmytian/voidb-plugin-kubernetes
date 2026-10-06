//! Kubernetes CLI plugin for voidb-cli.
//!
//! All commands go through `K8sService::new_direct()` — no direct k8s_ops calls.

use std::fs;

use async_trait::async_trait;
use chrono::Utc;
use clap::{Arg, ArgAction, ArgMatches, Command};
use voidb_core::plugin::cli::{CliContext, CliPlugin};
use voidb_core::{ConnectionProfileRef, TuiLaunchRequest, VoidbError, build_tui_launch_plan};

use crate::config::K8sConfig;
use crate::service::K8sService;
use crate::tui::{
    K8sTuiLaunch, K8sTuiSource, build_k8s_tui_evidence, run_k8s_tui, write_k8s_tui_preflight,
};
use crate::types::K8sEvent;

pub struct K8sCliPlugin;

pub fn create_k8s_cli_plugin() -> Box<dyn CliPlugin> {
    Box::new(K8sCliPlugin)
}

#[async_trait]
impl CliPlugin for K8sCliPlugin {
    fn plugin_id(&self) -> &str {
        "kubernetes"
    }

    fn name(&self) -> &str {
        "Kubernetes"
    }

    fn commands(&self) -> Vec<Command> {
        let conn_arg = Arg::new("connection")
            .short('c')
            .long("connection")
            .required(true)
            .help("Connection name");

        let ns_arg = Arg::new("namespace")
            .short('n')
            .long("namespace")
            .help("Kubernetes namespace (defaults to the connection profile)");

        let confirm_arg = Arg::new("confirm")
            .long("confirm")
            .action(ArgAction::SetTrue)
            .help("Skip the interactive confirmation prompt");

        vec![
            Command::new("test")
                .about("Test Kubernetes connection and show cluster info")
                .arg(conn_arg.clone()),
            Command::new("contexts")
                .about("List contexts from kubeconfig")
                .arg(conn_arg.clone()),
            Command::new("namespaces")
                .about("List all namespaces")
                .arg(conn_arg.clone()),
            Command::new("pods")
                .about("List pods in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("services")
                .about("List services in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("deployments")
                .about("List deployments in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("configmaps")
                .about("List configmaps in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("secrets")
                .about("List secrets in a namespace (values not shown)")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("nodes")
                .about("List cluster nodes")
                .arg(conn_arg.clone()),
            Command::new("statefulsets")
                .about("List statefulsets in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("daemonsets")
                .about("List daemonsets in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("jobs")
                .about("List jobs in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("cronjobs")
                .about("List cronjobs in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("pvcs")
                .about("List persistent volume claims in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("ingresses")
                .about("List ingresses in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("serviceaccounts")
                .about("List service accounts in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("events")
                .about("List events in a namespace")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone()),
            Command::new("get")
                .about("Get a resource as YAML")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone())
                .arg(
                    Arg::new("type")
                        .required(true)
                        .help("Resource type (pod, service, deployment, ...)")
                        .index(1),
                )
                .arg(
                    Arg::new("name")
                        .required(true)
                        .help("Resource name")
                        .index(2),
                )
                .arg(
                    Arg::new("show-secrets")
                        .long("show-secrets")
                        .action(ArgAction::SetTrue)
                        .help("Allow Secret YAML payloads to be printed"),
                ),
            Command::new("delete")
                .about("Delete a resource")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone())
                .arg(
                    Arg::new("type")
                        .required(true)
                        .help("Resource type")
                        .index(1),
                )
                .arg(
                    Arg::new("name")
                        .required(true)
                        .help("Resource name")
                        .index(2),
                )
                .arg(confirm_arg.clone()),
            Command::new("logs")
                .about("Print or follow pod logs")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone())
                .arg(Arg::new("pod").required(true).help("Pod name").index(1))
                .arg(
                    Arg::new("container")
                        .short('C')
                        .long("container")
                        .help("Container name"),
                )
                .arg(
                    Arg::new("follow")
                        .short('f')
                        .long("follow")
                        .action(clap::ArgAction::SetTrue)
                        .help("Follow log output"),
                )
                .arg(
                    Arg::new("tail")
                        .long("tail")
                        .default_value("100")
                        .value_parser(clap::value_parser!(i64).range(1..=5_000))
                        .help("Number of lines from the end of logs"),
                ),
            Command::new("apply")
                .about("Apply a YAML manifest file")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone())
                .arg(
                    Arg::new("file")
                        .short('f')
                        .long("file")
                        .required(true)
                        .help("Path to YAML file"),
                )
                .arg(confirm_arg.clone()),
            Command::new("scale")
                .about("Scale a deployment")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone())
                .arg(
                    Arg::new("deployment")
                        .required(true)
                        .help("Deployment name")
                        .index(1),
                )
                .arg(
                    Arg::new("replicas")
                        .required(true)
                        .value_parser(clap::value_parser!(u32).range(0..=10_000))
                        .help("Number of replicas")
                        .index(2),
                )
                .arg(confirm_arg.clone()),
            Command::new("restart")
                .about("Restart a deployment (rolling restart)")
                .arg(conn_arg.clone())
                .arg(ns_arg.clone())
                .arg(
                    Arg::new("deployment")
                        .required(true)
                        .help("Deployment name")
                        .index(1),
                )
                .arg(confirm_arg),
            Command::new("tui")
                .about("Launch the standalone Kubernetes operations TUI")
                .arg(
                    Arg::new("profile")
                        .long("profile")
                        .value_name("PROFILE")
                        .conflicts_with("connection")
                        .help("Profile name, id:<id>, or name:<name>"),
                )
                .arg(
                    Arg::new("connection")
                        .short('c')
                        .long("connection")
                        .value_name("CONNECTION")
                        .conflicts_with("profile")
                        .help("Legacy connection name"),
                )
                .arg(Arg::new("fixture").long("fixture").value_name("PATH").help(
                    "Load deterministic Kubernetes TUI fixture JSON instead of opening a cluster",
                ))
                .arg(
                    Arg::new("purpose")
                        .long("purpose")
                        .value_name("PURPOSE")
                        .default_value("operations")
                        .help("Launch purpose, for example operations or triage"),
                )
                .arg(
                    Arg::new("readonly")
                        .long("readonly")
                        .action(ArgAction::SetTrue)
                        .help("Stage operation plans but block mutating service commands"),
                )
                .arg(
                    Arg::new("no-restore")
                        .long("no-restore")
                        .action(ArgAction::SetTrue)
                        .help("Start without restoring plugin-owned UI state"),
                )
                .arg(
                    Arg::new("format")
                        .long("format")
                        .value_parser(["json"])
                        .help("Emit secret-free preflight JSON and exit"),
                )
                .arg(
                    Arg::new("evidence")
                        .long("evidence")
                        .value_name("PATH")
                        .help(
                            "Write fixture-backed standalone Kubernetes TUI evidence JSON and exit",
                        ),
                ),
        ]
    }

    async fn execute(
        &self,
        command: &str,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        match command {
            "contexts" => self.handle_contexts(matches, ctx).await,
            "test" => self.handle_test(matches, ctx).await,
            "namespaces" => self.handle_namespaces(matches, ctx).await,
            "pods" => self.handle_pods(matches, ctx).await,
            "services" => self.handle_services(matches, ctx).await,
            "deployments" => self.handle_deployments(matches, ctx).await,
            "statefulsets" => self.handle_statefulsets(matches, ctx).await,
            "daemonsets" => self.handle_daemonsets(matches, ctx).await,
            "jobs" => self.handle_jobs(matches, ctx).await,
            "cronjobs" => self.handle_cronjobs(matches, ctx).await,
            "pvcs" => self.handle_pvcs(matches, ctx).await,
            "ingresses" => self.handle_ingresses(matches, ctx).await,
            "serviceaccounts" => self.handle_service_accounts(matches, ctx).await,
            "configmaps" => self.handle_configmaps(matches, ctx).await,
            "secrets" => self.handle_secrets(matches, ctx).await,
            "nodes" => self.handle_nodes(matches, ctx).await,
            "events" => self.handle_events(matches, ctx).await,
            "get" => self.handle_get(matches, ctx).await,
            "delete" => self.handle_delete(matches, ctx).await,
            "logs" => self.handle_logs(matches, ctx).await,
            "apply" => self.handle_apply(matches, ctx).await,
            "scale" => self.handle_scale(matches, ctx).await,
            "restart" => self.handle_restart(matches, ctx).await,
            "tui" => self.handle_tui(matches, ctx).await,
            _ => Err(VoidbError::Plugin(format!(
                "Unknown kubernetes command: {}",
                command
            ))),
        }
    }
}

impl K8sCliPlugin {
    fn parse_config(conn_name: &str, ctx: &CliContext) -> Result<K8sConfig, VoidbError> {
        let conn = ctx
            .find_connection(conn_name)
            .ok_or_else(|| VoidbError::Plugin(format!("Connection '{}' not found", conn_name)))?;

        if conn.effective_plugin_id() != "kubernetes" {
            return Err(VoidbError::Plugin(format!(
                "Connection '{}' is not a Kubernetes connection (plugin: {})",
                conn_name,
                conn.effective_plugin_id()
            )));
        }

        conn.plugin_config
            .as_ref()
            .ok_or_else(|| VoidbError::Plugin("Missing plugin_config".to_string()))
            .and_then(|v| {
                serde_json::from_value(v.clone())
                    .map_err(|e| VoidbError::Plugin(format!("Invalid Kubernetes config: {}", e)))
            })
    }

    fn parse_tui_launch(
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<K8sTuiLaunch, VoidbError> {
        let fixture_path = matches.get_one::<String>("fixture").cloned();
        let purpose = matches
            .get_one::<String>("purpose")
            .cloned()
            .unwrap_or_else(|| "operations".to_string());
        let readonly = matches.get_flag("readonly");
        let restore = !matches.get_flag("no-restore");

        if let Some(profile_ref) = matches.get_one::<String>("profile") {
            let (profile, connection) =
                ctx.resolve_profile_connection(profile_ref, Some("kubernetes"))?;
            let config = connection
                .plugin_config
                .as_ref()
                .ok_or_else(|| VoidbError::Connection("Missing plugin_config".to_string()))
                .and_then(|pc| {
                    serde_json::from_value(pc.clone()).map_err(|e| {
                        VoidbError::Connection(format!("Invalid Kubernetes config: {}", e))
                    })
                })?;
            let profile_arg = profile_ref_arg(profile_ref, &profile.id);
            let request = TuiLaunchRequest::new("kubernetes", profile_arg, purpose.clone())
                .readonly(readonly)
                .restore(restore)
                .raw_input(false);
            let launch_plan = build_tui_launch_plan(&profile, request, "voidb-cli", Utc::now())?;

            return Ok(K8sTuiLaunch {
                profile_label: profile.name,
                config: Some(config),
                source: K8sTuiSource::Profile,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: Some(launch_plan),
            });
        }

        if let Some(conn_name) = matches.get_one::<String>("connection") {
            return Ok(K8sTuiLaunch {
                profile_label: conn_name.clone(),
                config: Some(Self::parse_config(conn_name, ctx)?),
                source: K8sTuiSource::Connection,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        if fixture_path.is_some() {
            return Ok(K8sTuiLaunch {
                profile_label: "fixture".to_string(),
                config: None,
                source: K8sTuiSource::Fixture,
                fixture_path,
                purpose,
                readonly,
                restore,
                launch_plan: None,
            });
        }

        Err(VoidbError::Plugin(
            "kubernetes tui requires --profile, --connection, or --fixture".to_string(),
        ))
    }

    async fn handle_tui(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let launch = Self::parse_tui_launch(matches, ctx)?;
        if let Some(path) = matches.get_one::<String>("evidence") {
            let evidence = build_k8s_tui_evidence(&launch).map_err(|e| {
                VoidbError::Plugin(format!("Kubernetes TUI evidence failed: {}", e))
            })?;
            let rendered = serde_json::to_string_pretty(&evidence).map_err(|e| {
                VoidbError::Plugin(format!(
                    "Kubernetes TUI evidence serialization failed: {}",
                    e
                ))
            })?;
            if let Some(parent) = std::path::Path::new(path).parent()
                && !parent.as_os_str().is_empty()
            {
                fs::create_dir_all(parent).map_err(|e| {
                    VoidbError::Plugin(format!(
                        "Failed to create Kubernetes TUI evidence directory '{}': {}",
                        parent.display(),
                        e
                    ))
                })?;
            }
            fs::write(path, rendered).map_err(|e| {
                VoidbError::Plugin(format!(
                    "Failed to write Kubernetes TUI evidence '{}': {}",
                    path, e
                ))
            })?;
            println!("Wrote Kubernetes TUI evidence to {path}");
            return Ok(());
        }

        if matches
            .get_one::<String>("format")
            .is_some_and(|format| format == "json")
        {
            write_k8s_tui_preflight(&launch).map_err(|e| {
                VoidbError::Plugin(format!("Kubernetes TUI preflight failed: {}", e))
            })?;
            return Ok(());
        }

        run_k8s_tui(launch)
            .await
            .map_err(|e| VoidbError::Plugin(format!("Kubernetes TUI failed: {}", e)))
    }

    /// Extract and deserialize `K8sConfig` from the named connection.
    fn get_k8s_config(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<K8sConfig, VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap();
        Self::parse_config(conn_name, ctx)
    }

    fn effective_namespace(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<String, VoidbError> {
        if let Some(namespace) = matches.get_one::<String>("namespace") {
            return Ok(namespace.clone());
        }
        Ok(self.get_k8s_config(matches, ctx)?.namespace().to_string())
    }

    /// Build a `K8sService` in Direct mode for the named connection.
    async fn connect(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<K8sService, VoidbError> {
        let config = self.get_k8s_config(matches, ctx)?;
        K8sService::new_direct(&config)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))
    }

    async fn handle_test(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let conn_name = matches.get_one::<String>("connection").unwrap().clone();
        let svc = self.connect(matches, ctx).await?;

        let info = svc
            .test_connection_direct()
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Connection: {}", conn_name);
        println!("{}", info);
        Ok(())
    }

    async fn handle_contexts(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let config = self.get_k8s_config(matches, ctx)?;
        let contexts = K8sService::list_contexts_direct(&config)
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        if contexts.is_empty() {
            println!("No contexts found");
            return Ok(());
        }

        println!("{:<40}", "CONTEXT");
        println!("{}", "-".repeat(40));
        for ctx in &contexts {
            println!("{}", ctx);
        }
        Ok(())
    }

    async fn handle_namespaces(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let svc = self.connect(matches, ctx).await?;

        let namespaces = svc
            .list_namespaces_direct()
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{:<40}", "NAMESPACE");
        println!("{}", "-".repeat(40));
        for ns in &namespaces {
            println!("{}", ns);
        }
        Ok(())
    }

    async fn handle_pods(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let pods = svc
            .list_pods_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<8}  {:<40}  {:<7}  {:<9}  {:<6}",
            "STATUS", "NAME", "READY", "RESTARTS", "AGE"
        );
        println!("{}", "-".repeat(80));
        for pod in &pods {
            println!(
                "{:<8}  {:<40}  {:<7}  {:<9}  {:<6}",
                pod.status.as_str(),
                pod.name,
                pod.ready,
                pod.restarts,
                pod.age,
            );
        }
        Ok(())
    }

    async fn handle_services(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let services = svc
            .list_services_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<12}  {:<16}  {:<20}  {:<6}",
            "NAME", "TYPE", "CLUSTER-IP", "PORTS", "AGE"
        );
        println!("{}", "-".repeat(90));
        for s in &services {
            println!(
                "{:<30}  {:<12}  {:<16}  {:<20}  {:<6}",
                s.name,
                s.service_type,
                s.cluster_ip,
                s.ports.join(","),
                s.age,
            );
        }
        Ok(())
    }

    async fn handle_deployments(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let deployments = svc
            .list_deployments_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<8}  {:<10}  {:<11}  {:<6}",
            "NAME", "READY", "UP-TO-DATE", "AVAILABLE", "AGE"
        );
        println!("{}", "-".repeat(75));
        for d in &deployments {
            println!(
                "{:<30}  {:<8}  {:<10}  {:<11}  {:<6}",
                d.name, d.ready, d.up_to_date, d.available, d.age,
            );
        }
        Ok(())
    }

    async fn handle_statefulsets(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let statefulsets = svc
            .list_statefulsets_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{:<30}  {:<8}  {:<6}", "NAME", "READY", "AGE");
        println!("{}", "-".repeat(50));
        for s in &statefulsets {
            println!("{:<30}  {:<8}  {:<6}", s.name, s.ready, s.age);
        }
        Ok(())
    }

    async fn handle_daemonsets(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let daemonsets = svc
            .list_daemonsets_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<8}  {:<8}  {:<6}  {:<6}",
            "NAME", "DESIRED", "CURRENT", "READY", "AGE"
        );
        println!("{}", "-".repeat(65));
        for d in &daemonsets {
            println!(
                "{:<30}  {:<8}  {:<8}  {:<6}  {:<6}",
                d.name, d.desired, d.current, d.ready, d.age
            );
        }
        Ok(())
    }

    async fn handle_jobs(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let jobs = svc
            .list_jobs_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<8}  {:<30}  {:<12}  {:<10}  {:<6}",
            "STATUS", "NAME", "COMPLETIONS", "DURATION", "AGE"
        );
        println!("{}", "-".repeat(75));
        for j in &jobs {
            println!(
                "{:<8}  {:<30}  {:<12}  {:<10}  {:<6}",
                j.status.as_str(),
                j.name,
                j.completions,
                j.duration,
                j.age
            );
        }
        Ok(())
    }

    async fn handle_cronjobs(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let cronjobs = svc
            .list_cronjobs_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<18}  {:<8}  {:<6}  {:<8}  {:<6}",
            "NAME", "SCHEDULE", "TZ", "ACTIVE", "LAST", "AGE"
        );
        println!("{}", "-".repeat(85));
        for cj in &cronjobs {
            println!(
                "{:<30}  {:<18}  {:<8}  {:<6}  {:<8}  {:<6}",
                cj.name, cj.schedule, cj.timezone, cj.active, cj.last_schedule, cj.age
            );
        }
        Ok(())
    }

    async fn handle_pvcs(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let pvcs = svc
            .list_pvcs_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<8}  {:<10}  {:<12}  {:<12}  {:<6}",
            "NAME", "STATUS", "CAPACITY", "ACCESS", "CLASS", "AGE"
        );
        println!("{}", "-".repeat(85));
        for pvc in &pvcs {
            println!(
                "{:<30}  {:<8}  {:<10}  {:<12}  {:<12}  {:<6}",
                pvc.name, pvc.status, pvc.capacity, pvc.access_modes, pvc.storage_class, pvc.age
            );
        }
        Ok(())
    }

    async fn handle_ingresses(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let ingresses = svc
            .list_ingresses_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<12}  {:<30}  {:<16}  {:<6}",
            "NAME", "CLASS", "HOSTS", "ADDRESSES", "AGE"
        );
        println!("{}", "-".repeat(100));
        for ing in &ingresses {
            println!(
                "{:<30}  {:<12}  {:<30}  {:<16}  {:<6}",
                ing.name,
                ing.class,
                ing.hosts.join(","),
                ing.addresses.join(","),
                ing.age
            );
        }
        Ok(())
    }

    async fn handle_service_accounts(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let sas = svc
            .list_service_accounts_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{:<40}  {:<8}  {:<6}", "NAME", "SECRETS", "AGE");
        println!("{}", "-".repeat(58));
        for sa in &sas {
            println!("{:<40}  {:<8}  {:<6}", sa.name, sa.secrets_count, sa.age);
        }
        Ok(())
    }

    async fn handle_configmaps(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let cms = svc
            .list_configmaps_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{:<40}  {:<5}  {:<6}", "NAME", "KEYS", "AGE");
        println!("{}", "-".repeat(55));
        for cm in &cms {
            println!("{:<40}  {:<5}  {:<6}", cm.name, cm.data_count, cm.age);
        }
        Ok(())
    }

    async fn handle_secrets(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let secrets = svc
            .list_secrets_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<40}  {:<20}  {:<5}  {:<6}",
            "NAME", "TYPE", "KEYS", "AGE"
        );
        println!("{}", "-".repeat(75));
        for s in &secrets {
            println!(
                "{:<40}  {:<20}  {:<5}  {:<6}",
                s.name, s.secret_type, s.data_count, s.age
            );
        }
        Ok(())
    }

    async fn handle_nodes(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let svc = self.connect(matches, ctx).await?;

        let nodes = svc
            .list_nodes_direct()
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "{:<30}  {:<10}  {:<20}  {:<6}",
            "NAME", "STATUS", "VERSION", "AGE"
        );
        println!("{}", "-".repeat(72));
        for n in &nodes {
            println!(
                "{:<30}  {:<10}  {:<20}  {:<6}",
                n.name, n.status, n.version, n.age
            );
        }
        Ok(())
    }

    async fn handle_events(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let svc = self.connect(matches, ctx).await?;

        let events = svc
            .list_events_direct(&ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{:<8}  {:<15}  {:<20}  MESSAGE", "TYPE", "REASON", "OBJECT");
        println!("{}", "-".repeat(90));
        for ev in &events {
            let msg = truncate_chars(&ev.message, 50);
            println!(
                "{:<8}  {:<15}  {:<20}  {}",
                ev.event_type, ev.reason, ev.object, msg
            );
        }
        Ok(())
    }

    async fn handle_get(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let resource_type = matches.get_one::<String>("type").unwrap().clone();
        let name = matches.get_one::<String>("name").unwrap().clone();
        if is_secret_resource(&resource_type) && !matches.get_flag("show-secrets") {
            return Err(VoidbError::Plugin(
                "Secret YAML is blocked by default; pass --show-secrets to print payloads"
                    .to_string(),
            ));
        }
        let svc = self.connect(matches, ctx).await?;

        let yaml = svc
            .get_resource_yaml_direct(&resource_type, &name, &ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        print!("{}", yaml);
        Ok(())
    }

    async fn handle_delete(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let resource_type = matches.get_one::<String>("type").unwrap().clone();
        let name = matches.get_one::<String>("name").unwrap().clone();
        if !confirm_operation(
            matches,
            &format!("Delete {resource_type} '{name}' in namespace '{ns}'?"),
        ) {
            return Ok(());
        }

        let svc = self.connect(matches, ctx).await?;

        svc.delete_resource_direct(&resource_type, &name, &ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{} '{}' deleted", resource_type, name);
        Ok(())
    }

    async fn handle_logs(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let pod = matches.get_one::<String>("pod").unwrap().clone();
        let container = matches.get_one::<String>("container").cloned();
        let follow = matches.get_flag("follow");
        let tail = *matches.get_one::<i64>("tail").unwrap();

        let svc = self.connect(matches, ctx).await?;

        let mut rx = svc
            .stream_logs_direct(pod, ns, container, follow, Some(tail))
            .await;

        while let Some(event) = rx.recv().await {
            match event {
                K8sEvent::LogLine { text } => println!("{}", text),
                K8sEvent::Error(e) => {
                    eprintln!("Error: {}", e);
                    break;
                }
                _ => {}
            }
        }
        Ok(())
    }

    async fn handle_apply(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let file = matches.get_one::<String>("file").unwrap().clone();

        let yaml_content = std::fs::read_to_string(&file)
            .map_err(|e| VoidbError::Plugin(format!("Failed to read file '{}': {}", file, e)))?;

        if !confirm_operation(
            matches,
            &format!("Apply manifest '{file}' with default namespace '{ns}'?"),
        ) {
            return Ok(());
        }

        let svc = self.connect(matches, ctx).await?;

        let result = svc
            .apply_yaml_direct(&yaml_content, &ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("{}", result);
        Ok(())
    }

    async fn handle_scale(&self, matches: &ArgMatches, ctx: &CliContext) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let deployment = matches.get_one::<String>("deployment").unwrap().clone();
        let replicas = *matches.get_one::<u32>("replicas").unwrap();

        if !confirm_operation(
            matches,
            &format!("Scale deployment '{deployment}' in namespace '{ns}' to {replicas} replicas?"),
        ) {
            return Ok(());
        }

        let svc = self.connect(matches, ctx).await?;

        svc.scale_deployment_direct(&deployment, &ns, replicas)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!(
            "Scaled deployment '{}' to {} replicas",
            deployment, replicas
        );
        Ok(())
    }

    async fn handle_restart(
        &self,
        matches: &ArgMatches,
        ctx: &CliContext,
    ) -> Result<(), VoidbError> {
        let ns = self.effective_namespace(matches, ctx)?;
        let deployment = matches.get_one::<String>("deployment").unwrap().clone();

        if !confirm_operation(
            matches,
            &format!("Restart deployment '{deployment}' in namespace '{ns}'?"),
        ) {
            return Ok(());
        }

        let svc = self.connect(matches, ctx).await?;

        svc.restart_deployment_direct(&deployment, &ns)
            .await
            .map_err(|e| VoidbError::Plugin(e.to_string()))?;

        println!("Deployment '{}' restart triggered", deployment);
        Ok(())
    }
}

fn confirm_operation(matches: &ArgMatches, prompt: &str) -> bool {
    if matches.get_flag("confirm") {
        return true;
    }

    print!("{prompt} [y/N] ");
    use std::io::Write;
    let _ = std::io::stdout().flush();
    let mut input = String::new();
    let confirmed =
        std::io::stdin().read_line(&mut input).is_ok() && input.trim().eq_ignore_ascii_case("y");
    if !confirmed {
        println!("Aborted.");
    }
    confirmed
}

fn is_secret_resource(resource_type: &str) -> bool {
    matches!(
        resource_type.to_ascii_lowercase().as_str(),
        "secret" | "secrets"
    )
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }
    if max_chars <= 3 {
        return ".".repeat(max_chars);
    }
    let mut truncated = value.chars().take(max_chars - 3).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn profile_ref_arg(input: &str, profile_id: &str) -> ConnectionProfileRef {
    if let Some(id) = input.strip_prefix("id:") {
        ConnectionProfileRef::Id(id.to_string())
    } else if let Some(name) = input
        .strip_prefix("name:")
        .or_else(|| input.strip_prefix("alias:"))
    {
        ConnectionProfileRef::Name(name.to_string())
    } else if input == profile_id {
        ConnectionProfileRef::Id(input.to_string())
    } else {
        ConnectionProfileRef::Name(input.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn command() -> Command {
        Command::new("kubernetes").subcommands(K8sCliPlugin.commands())
    }

    #[test]
    fn namespace_defaults_to_profile_instead_of_clap() {
        let matches = command()
            .try_get_matches_from(["kubernetes", "pods", "--connection", "cluster"])
            .unwrap();
        let (_, pods) = matches.subcommand().unwrap();

        assert!(pods.get_one::<String>("namespace").is_none());
    }

    #[test]
    fn mutation_commands_expose_explicit_confirmation() {
        for args in [
            vec![
                "kubernetes",
                "delete",
                "pod",
                "web",
                "-c",
                "cluster",
                "--confirm",
            ],
            vec![
                "kubernetes",
                "apply",
                "-f",
                "app.yaml",
                "-c",
                "cluster",
                "--confirm",
            ],
            vec![
                "kubernetes",
                "scale",
                "web",
                "3",
                "-c",
                "cluster",
                "--confirm",
            ],
            vec!["kubernetes", "restart", "web", "-c", "cluster", "--confirm"],
        ] {
            let matches = command().try_get_matches_from(args).unwrap();
            let (_, operation) = matches.subcommand().unwrap();
            assert!(operation.get_flag("confirm"));
        }
    }

    #[test]
    fn secret_yaml_requires_an_explicit_opt_in_flag() {
        assert!(is_secret_resource("secret"));
        assert!(is_secret_resource("Secrets"));

        let matches = command()
            .try_get_matches_from([
                "kubernetes",
                "get",
                "secret",
                "registry",
                "-c",
                "cluster",
                "--show-secrets",
            ])
            .unwrap();
        let (_, get) = matches.subcommand().unwrap();
        assert!(get.get_flag("show-secrets"));
    }

    #[test]
    fn event_messages_are_truncated_on_character_boundaries() {
        let message = "容器启动失败，需要检查镜像和凭据";
        let truncated = truncate_chars(message, 10);

        assert_eq!(truncated.chars().count(), 10);
        assert!(truncated.ends_with("..."));
    }

    #[test]
    fn cli_bounds_tail_and_replica_counts() {
        assert!(
            command()
                .try_get_matches_from([
                    "kubernetes",
                    "logs",
                    "web",
                    "-c",
                    "cluster",
                    "--tail",
                    "5001",
                ])
                .is_err()
        );
        assert!(
            command()
                .try_get_matches_from(["kubernetes", "scale", "web", "10001", "-c", "cluster",])
                .is_err()
        );
    }
}
