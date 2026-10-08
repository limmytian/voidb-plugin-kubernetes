# VoidB Kubernetes Plugin (`voidb-plugin-kubernetes`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Kubernetes clusters and workloads.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
- **Tri-Modal Operation**:
  - `test`: Automated connectivity verification.
  - `tui`: Standalone full-screen interactive terminal interface.
  - `serve`: `stdio-jsonrpc` protocol server for AI Agents and automated orchestration.
  - Domain commands: Rich autonomous cluster inspection and management commands.
- **Capability Surface**:
  - `diagnostics`: Return agent-safe profile diagnostics without connecting to cluster.
  - `contexts`: Enumerate kubeconfig contexts.
  - `namespaces`: List cluster namespaces.
  - `list`: Cursor-based listing across standard resources (pods, services, deployments, statefulsets, daemonsets, jobs, cronjobs, pvcs, ingresses, serviceaccounts, configmaps, secrets, nodes, events).
  - `get_yaml`: Bounded YAML retrieval with automatic Secret payload suppression.
  - `logs`: Tail pod container logs.
  - `watch_events`: Watch resource changes with resource-version continuation.
  - `logs_follow`: Follow container logs in real time.
  - `exec_read`, `exec_input`, `exec_resize`: Controlled, interactive pod container execution.
  - `port_forward_events`: Loopback-bound local port forwarding.
  - `delete`: Delete namespaced resources (with dry-run support).
  - `scale`: Scale deployments to desired replica count (with dry-run support).
  - `restart`: Trigger rolling restart of deployments (with dry-run support).
  - `apply`: Apply Kubernetes YAML manifests (with dry-run support).

## Usage & Operational Modes

When installed, `voidb-cli` automatically discovers this plugin and provides seamless transparent dispatch:

### 1. Human Operations (CLI & TUI)

```bash
# Connectivity test
voidb-cli kubernetes test --profile my-k8s-cluster

# Launch standalone interactive fullscreen TUI
voidb-cli kubernetes tui --profile my-k8s-cluster

# Cluster resource inspection
voidb-cli kubernetes namespaces --profile my-k8s-cluster
voidb-cli kubernetes pods -n default --profile my-k8s-cluster
voidb-cli kubernetes deployments --profile my-k8s-cluster
voidb-cli kubernetes services --profile my-k8s-cluster

# Pod logs and workloads
voidb-cli kubernetes logs my-pod -c app --tail 100 --profile my-k8s-cluster
voidb-cli kubernetes scale deployment/my-app --replicas 3 --profile my-k8s-cluster
voidb-cli kubernetes restart deployment/my-app --profile my-k8s-cluster
```

### 2. Machine Communication (AI Agents & JSON-RPC)

```bash
# Run the stdio-jsonrpc server directly or via voidb-cli
voidb-cli kubernetes serve
# Or directly run the binary:
./bin/voidb-plugin-kubernetes serve
```

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory (`~/Library/Application Support/voidb/plugins/kubernetes` on macOS, or `~/.local/share/voidb/plugins/kubernetes` on Linux):

```bash
mkdir -p "$HOME/Library/Application Support/voidb/plugins/kubernetes"
cp -r plugin.toml bin schemas "$HOME/Library/Application Support/voidb/plugins/kubernetes/"
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli kubernetes --help
```

### Development & Build

```bash
cargo build --release
mkdir -p bin
cp target/release/voidb-plugin-kubernetes bin/
```

## Protocol Specifications

Complies with the [VoidB Process Plugin Protocol](https://github.com/limmytian/voidb/blob/main/docs/quickstart-process-plugin.md) specification (v1.0).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
