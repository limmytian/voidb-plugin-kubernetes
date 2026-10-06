# VoidB Kubernetes Plugin (`voidb-plugin-kubernetes`)

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Independent process plugin for [VoidB](https://github.com/limmytian/voidb) to connect, inspect, and manage Kubernetes clusters and workloads.

## Features

- **Autonomous Process Architecture**: Runs in an isolated OS process communicating with VoidB via `stdio-jsonrpc`.
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
- **Dual Mode**: Can run as a JSON-RPC worker server (`voidb-plugin-kubernetes serve`) or standalone interactive TUI.

## Quick Start

### Installation

Place this plugin directory or a packaged release archive under your VoidB plugins directory:

```bash
mkdir -p ~/.config/voidb/plugins/kubernetes
cp -r plugin.toml bin schemas ~/.config/voidb/plugins/kubernetes/
```

Verify discovery via `voidb`:

```bash
voidb-cli plugin list
voidb-cli plugin describe kubernetes
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
