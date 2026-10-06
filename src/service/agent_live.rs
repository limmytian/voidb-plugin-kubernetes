//! Direct Kubernetes handles used by persistent agent sessions.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use futures::{SinkExt, StreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::api::{
    Api, AttachParams, AttachedProcess, DynamicObject, LogParams, TerminalSize, WatchEvent,
    WatchParams,
};
use kube::core::{ApiResource, GroupVersionKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, mpsc, watch};
use tokio::task::JoinHandle;

use crate::config::K8sConfig;
use crate::k8s_ops;

const PUMP_CAPACITY: usize = 64;
const IO_CHUNK_BYTES: usize = 32 * 1024;

#[derive(Clone)]
pub struct K8sAgentService {
    client: kube::Client,
}

pub struct K8sExecSpec {
    pub namespace: String,
    pub pod: String,
    pub container: String,
    pub command: Vec<String>,
    pub tty: bool,
    pub cols: u16,
    pub rows: u16,
}

impl K8sAgentService {
    pub async fn connect(config: &K8sConfig) -> Result<Self> {
        let client = k8s_ops::create_client(config).await?;
        k8s_ops::test_connection(&client).await?;
        Ok(Self { client })
    }

    pub fn watch(&self, spec: K8sWatchSpec, resource_version: String) -> K8sWatchPump {
        let client = self.client.clone();
        let (sender, receiver) = mpsc::channel(PUMP_CAPACITY);
        let task = tokio::spawn(async move {
            let resource = match api_resource(&spec.api_version, &spec.kind, &spec.plural) {
                Ok(resource) => resource,
                Err(error) => {
                    let _ = sender.send(Err(error)).await;
                    return;
                }
            };
            let api: Api<DynamicObject> = match &spec.namespace {
                Some(namespace) => Api::namespaced_with(client, namespace, &resource),
                None => Api::all_with(client, &resource),
            };
            let params = WatchParams {
                label_selector: spec.label_selector,
                field_selector: spec.field_selector,
                timeout: Some(60),
                bookmarks: true,
                send_initial_events: false,
            };
            let mut stream = match api.watch(&params, &resource_version).await {
                Ok(stream) => Box::pin(stream),
                Err(error) => {
                    let _ = sender
                        .send(Err(anyhow!(error).context("Kubernetes watch open failed")))
                        .await;
                    return;
                }
            };
            while let Some(item) = stream.next().await {
                let mapped = item
                    .map_err(|error| anyhow!(error).context("Kubernetes watch stream failed"))
                    .map(map_watch_event);
                if sender.send(mapped).await.is_err() {
                    return;
                }
            }
        });
        K8sWatchPump {
            receiver,
            task: Some(task),
        }
    }

    pub fn logs(&self, spec: K8sLogSpec) -> K8sBytePump {
        let client = self.client.clone();
        let (sender, receiver) = mpsc::channel(PUMP_CAPACITY);
        let task = tokio::spawn(async move {
            let pods: Api<Pod> = Api::namespaced(client, &spec.namespace);
            let params = LogParams {
                container: Some(spec.container),
                follow: true,
                limit_bytes: None,
                pretty: false,
                previous: spec.previous,
                since_seconds: None,
                since_time: spec.since_time,
                tail_lines: spec.tail_lines,
                timestamps: spec.timestamps,
            };
            let reader = match pods.log_stream(&spec.pod, &params).await {
                Ok(reader) => reader,
                Err(error) => {
                    let _ = sender
                        .send(Err(
                            anyhow!(error).context("Kubernetes log stream open failed")
                        ))
                        .await;
                    return;
                }
            };
            pump_reader(reader, "Kubernetes log stream failed", sender).await;
        });
        K8sBytePump {
            receiver,
            task: Some(task),
        }
    }

    pub async fn open_exec(&self, spec: K8sExecSpec) -> Result<K8sTerminalParts> {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &spec.namespace);
        let params = AttachParams {
            container: Some(spec.container),
            stdin: true,
            stdout: true,
            stderr: !spec.tty,
            tty: spec.tty,
            max_stdin_buf_size: Some(64 * 1024),
            max_stdout_buf_size: Some(256 * 1024),
            max_stderr_buf_size: Some(256 * 1024),
        };
        let mut process = pods
            .exec(&spec.pod, spec.command, &params)
            .await
            .context("Kubernetes exec could not be started")?;
        let stdin = process
            .stdin()
            .map(|writer| Box::pin(writer) as Pin<Box<dyn AsyncWrite + Send>>)
            .ok_or_else(|| anyhow!("Kubernetes exec stdin is unavailable"))?;
        let stdout = process
            .stdout()
            .map(|reader| Box::pin(reader) as Pin<Box<dyn AsyncRead + Send>>)
            .ok_or_else(|| anyhow!("Kubernetes exec stdout is unavailable"))?;
        let stderr = process
            .stderr()
            .map(|reader| Box::pin(reader) as Pin<Box<dyn AsyncRead + Send>>);
        let terminal_size = process.terminal_size();
        let (sender, receiver) = mpsc::channel(PUMP_CAPACITY);
        let mut reader_tasks = vec![spawn_terminal_reader("stdout", stdout, sender.clone())];
        if let Some(stderr) = stderr {
            reader_tasks.push(spawn_terminal_reader("stderr", stderr, sender.clone()));
        }
        drop(sender);
        let control = Arc::new(K8sTerminalControl {
            process: Mutex::new(Some(process)),
            stdin: Mutex::new(Some(stdin)),
            terminal_size: Mutex::new(terminal_size),
            reader_tasks: Mutex::new(reader_tasks),
            tty: spec.tty,
        });
        if spec.tty {
            control.resize(spec.cols, spec.rows).await?;
        }
        Ok(K8sTerminalParts {
            output: K8sTerminalOutput { receiver },
            control,
        })
    }

    pub async fn open_port_forward(
        &self,
        namespace: String,
        pod: String,
        local_port: u16,
        remote_port: u16,
    ) -> Result<K8sPortForwardParts> {
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, local_port))
            .await
            .context("Kubernetes local port could not be bound")?;
        let bound = listener.local_addr()?.port();
        let client = self.client.clone();
        let (event_sender, event_receiver) = mpsc::channel(PUMP_CAPACITY);
        let (cancel_sender, mut cancel_receiver) = watch::channel(false);
        let task = tokio::spawn(async move {
            if event_sender
                .send(K8sPortForwardEvent::Listening { local_port: bound })
                .await
                .is_err()
            {
                return;
            }
            loop {
                let accepted = tokio::select! {
                    changed = cancel_receiver.changed() => {
                        let _ = changed;
                        return;
                    }
                    accepted = listener.accept() => accepted,
                };
                let (mut local, _) = match accepted {
                    Ok(value) => value,
                    Err(_) => {
                        let _ = event_sender
                            .send(K8sPortForwardEvent::Error { class: "local_io" })
                            .await;
                        return;
                    }
                };
                if event_sender
                    .send(K8sPortForwardEvent::ConnectionOpened)
                    .await
                    .is_err()
                {
                    return;
                }
                let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace);
                let mut forwarder = match pods.portforward(&pod, &[remote_port]).await {
                    Ok(forwarder) => forwarder,
                    Err(error) => {
                        let class = kube_error_class(&error.to_string());
                        let _ = event_sender
                            .send(K8sPortForwardEvent::Error { class })
                            .await;
                        continue;
                    }
                };
                let Some(mut remote) = forwarder.take_stream(remote_port) else {
                    let _ = event_sender
                        .send(K8sPortForwardEvent::Error { class: "protocol" })
                        .await;
                    forwarder.abort();
                    continue;
                };
                let copy_result = tokio::select! {
                    changed = cancel_receiver.changed() => {
                        let _ = changed;
                        None
                    }
                    copied = tokio::io::copy_bidirectional(&mut local, &mut remote) => Some(copied),
                };
                forwarder.abort();
                match copy_result {
                    None => return,
                    Some(Ok((to_remote, to_local))) => {
                        if event_sender
                            .send(K8sPortForwardEvent::ConnectionClosed {
                                to_remote,
                                to_local,
                            })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Some(Err(_)) => {
                        let _ = event_sender
                            .send(K8sPortForwardEvent::Error { class: "transport" })
                            .await;
                    }
                }
            }
        });
        Ok(K8sPortForwardParts {
            events: event_receiver,
            control: Arc::new(K8sPortForwardControl {
                cancel: cancel_sender,
                task: Mutex::new(Some(task)),
            }),
            local_port: bound,
        })
    }
}

#[derive(Debug, Clone)]
pub struct K8sWatchSpec {
    pub api_version: String,
    pub kind: String,
    pub plural: String,
    pub namespace: Option<String>,
    pub label_selector: Option<String>,
    pub field_selector: Option<String>,
}

#[derive(Debug)]
pub enum K8sWatchEvent {
    Applied {
        event_type: &'static str,
        api_version: Option<String>,
        kind: Option<String>,
        name: Option<String>,
        namespace: Option<String>,
        resource_version: Option<String>,
        generation: Option<i64>,
        label_keys: Vec<String>,
        annotation_keys: Vec<String>,
    },
    Bookmark {
        resource_version: String,
    },
    Error {
        code: u16,
        reason: Option<String>,
    },
}

pub struct K8sWatchPump {
    receiver: mpsc::Receiver<Result<K8sWatchEvent>>,
    task: Option<JoinHandle<()>>,
}

impl K8sWatchPump {
    pub async fn next(&mut self) -> Option<Result<K8sWatchEvent>> {
        self.receiver.recv().await
    }
}

impl Drop for K8sWatchPump {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Debug, Clone)]
pub struct K8sLogSpec {
    pub namespace: String,
    pub pod: String,
    pub container: String,
    pub previous: bool,
    pub since_time: Option<chrono::DateTime<chrono::Utc>>,
    pub tail_lines: Option<i64>,
    pub timestamps: bool,
}

pub struct K8sBytePump {
    receiver: mpsc::Receiver<Result<Vec<u8>>>,
    task: Option<JoinHandle<()>>,
}

impl K8sBytePump {
    pub async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        self.receiver.recv().await
    }
}

impl Drop for K8sBytePump {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

pub struct K8sTerminalParts {
    pub output: K8sTerminalOutput,
    pub control: Arc<K8sTerminalControl>,
}

pub struct K8sTerminalOutput {
    receiver: mpsc::Receiver<Result<K8sTerminalChunk>>,
}

impl K8sTerminalOutput {
    pub async fn next(&mut self) -> Option<Result<K8sTerminalChunk>> {
        self.receiver.recv().await
    }
}

pub struct K8sTerminalChunk {
    pub stream: &'static str,
    pub bytes: Vec<u8>,
}

type K8sInput = Pin<Box<dyn AsyncWrite + Send>>;

pub struct K8sTerminalControl {
    process: Mutex<Option<AttachedProcess>>,
    stdin: Mutex<Option<K8sInput>>,
    terminal_size: Mutex<Option<futures::channel::mpsc::Sender<TerminalSize>>>,
    reader_tasks: Mutex<Vec<JoinHandle<()>>>,
    tty: bool,
}

impl K8sTerminalControl {
    pub async fn write(&self, data: &[u8]) -> Result<()> {
        let mut input = self.stdin.lock().await;
        let writer = input
            .as_mut()
            .ok_or_else(|| anyhow!("Kubernetes exec stdin is closed"))?;
        writer
            .as_mut()
            .write_all(data)
            .await
            .context("Kubernetes exec stdin write failed")?;
        writer
            .as_mut()
            .flush()
            .await
            .context("Kubernetes exec stdin flush failed")
    }

    pub async fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        if !self.tty {
            return Err(anyhow!("Kubernetes exec resize requires tty=true"));
        }
        let mut sender = self.terminal_size.lock().await;
        sender
            .as_mut()
            .ok_or_else(|| anyhow!("Kubernetes exec resize channel is unavailable"))?
            .send(TerminalSize {
                width: cols,
                height: rows,
            })
            .await
            .context("Kubernetes exec resize failed")
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(mut input) = self.stdin.lock().await.take() {
            let _ = input.as_mut().shutdown().await;
        }
        self.terminal_size.lock().await.take();
        if let Some(process) = self.process.lock().await.take() {
            process.abort();
            let _ = process.join().await;
        }
        for task in self.reader_tasks.lock().await.drain(..) {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
}

pub struct K8sPortForwardParts {
    pub events: mpsc::Receiver<K8sPortForwardEvent>,
    pub control: Arc<K8sPortForwardControl>,
    pub local_port: u16,
}

#[derive(Debug)]
pub enum K8sPortForwardEvent {
    Listening { local_port: u16 },
    ConnectionOpened,
    ConnectionClosed { to_remote: u64, to_local: u64 },
    Error { class: &'static str },
}

pub struct K8sPortForwardControl {
    cancel: watch::Sender<bool>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl K8sPortForwardControl {
    pub async fn close(&self) {
        let _ = self.cancel.send(true);
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

fn api_resource(api_version: &str, kind: &str, plural: &str) -> Result<ApiResource> {
    let (group, version) = api_version
        .split_once('/')
        .map(|(group, version)| (group.to_string(), version.to_string()))
        .unwrap_or_else(|| (String::new(), api_version.to_string()));
    if version.is_empty() || kind.is_empty() || plural.is_empty() {
        return Err(anyhow!(
            "Kubernetes dynamic resource identity is incomplete"
        ));
    }
    Ok(ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk(&group, &version, kind),
        plural,
    ))
}

fn map_watch_event(event: WatchEvent<DynamicObject>) -> K8sWatchEvent {
    match event {
        WatchEvent::Added(object) => map_object_event("added", object),
        WatchEvent::Modified(object) => map_object_event("modified", object),
        WatchEvent::Deleted(object) => map_object_event("deleted", object),
        WatchEvent::Bookmark(bookmark) => K8sWatchEvent::Bookmark {
            resource_version: bookmark.metadata.resource_version,
        },
        WatchEvent::Error(error) => K8sWatchEvent::Error {
            code: error.code,
            reason: (!error.reason.is_empty()).then_some(error.reason),
        },
    }
}

fn map_object_event(event_type: &'static str, object: DynamicObject) -> K8sWatchEvent {
    let label_keys = bounded_keys(object.metadata.labels.as_ref());
    let annotation_keys = bounded_keys(object.metadata.annotations.as_ref());
    K8sWatchEvent::Applied {
        event_type,
        api_version: object.types.as_ref().map(|types| types.api_version.clone()),
        kind: object.types.as_ref().map(|types| types.kind.clone()),
        name: object.metadata.name,
        namespace: object.metadata.namespace,
        resource_version: object.metadata.resource_version,
        generation: object.metadata.generation,
        label_keys,
        annotation_keys,
    }
}

fn bounded_keys(values: Option<&BTreeMap<String, String>>) -> Vec<String> {
    values
        .map(|values| values.keys().take(64).cloned().collect())
        .unwrap_or_default()
}

async fn pump_reader<R>(mut reader: R, context: &'static str, sender: mpsc::Sender<Result<Vec<u8>>>)
where
    R: futures::AsyncRead + Unpin,
{
    let mut bytes = vec![0u8; IO_CHUNK_BYTES];
    loop {
        match futures::AsyncReadExt::read(&mut reader, &mut bytes).await {
            Ok(0) => return,
            Ok(read) => {
                if sender.send(Ok(bytes[..read].to_vec())).await.is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = sender.send(Err(anyhow!(error).context(context))).await;
                return;
            }
        }
    }
}

fn spawn_terminal_reader(
    stream: &'static str,
    mut reader: Pin<Box<dyn AsyncRead + Send>>,
    sender: mpsc::Sender<Result<K8sTerminalChunk>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut bytes = vec![0u8; IO_CHUNK_BYTES];
        loop {
            match reader.as_mut().read(&mut bytes).await {
                Ok(0) => return,
                Ok(read) => {
                    if sender
                        .send(Ok(K8sTerminalChunk {
                            stream,
                            bytes: bytes[..read].to_vec(),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(error) => {
                    let _ = sender
                        .send(Err(anyhow!(error).context("Kubernetes exec output failed")))
                        .await;
                    return;
                }
            }
        }
    })
}

fn kube_error_class(message: &str) -> &'static str {
    let message = message.to_ascii_lowercase();
    if message.contains("401") || message.contains("unauthorized") {
        "authentication"
    } else if message.contains("403") || message.contains("forbidden") {
        "rbac_denied"
    } else if message.contains("404") || message.contains("not found") {
        "not_found"
    } else if message.contains("timeout") || message.contains("timed out") {
        "timeout"
    } else {
        "transport"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dynamic_resource_identity_handles_core_and_grouped_versions() {
        let core = api_resource("v1", "Pod", "pods").unwrap();
        assert_eq!(core.group, "");
        assert_eq!(core.version, "v1");
        let apps = api_resource("apps/v1", "Deployment", "deployments").unwrap();
        assert_eq!(apps.group, "apps");
        assert_eq!(apps.version, "v1");
    }

    #[test]
    fn target_errors_distinguish_rbac_from_transport() {
        assert_eq!(
            kube_error_class("ApiError: Forbidden (ErrorResponse { code: 403 })"),
            "rbac_denied"
        );
        assert_eq!(kube_error_class("connection timed out"), "timeout");
    }
}
