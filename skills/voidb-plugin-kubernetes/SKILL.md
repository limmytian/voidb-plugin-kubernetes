---
name: voidb-plugin-kubernetes
description: Guide for the VoidB Kubernetes plugin. Use when modifying crates/plugins/voidb-plugin-kubernetes, kubernetes.* capabilities, Kubernetes CLI commands, K8s service operations, kubeconfig/auth handling, standalone Kubernetes TUI, logs/apply/scale/restart/delete policy, or fixture smoke behavior.
---

# VoidB Kubernetes Plugin

## Start Here

Primary crate: `crates/plugins/voidb-plugin-kubernetes`.

Inspect:

- `src/config.rs` for `K8sConfig`, connection modes, and auth variants.
- `src/k8s_ops.rs` for low-level Kubernetes operations.
- `src/service/` for commands, events, and service facade.
- `src/capabilities.rs` for `kubernetes.*` metadata and invocation.
- `src/cli_plugin.rs` for `voidb-cli kubernetes ...`.
- `src/tui.rs` for standalone Kubernetes TUI.
- `docs/kubernetes-release-readiness.md` and `docs/kubernetes-tui-evidence-2026-07-07.md`.

## Boundaries

- Keep `kube`, `k8s-openapi`, and auth details inside the plugin crate.
- Never expose secret values from Kubernetes Secret resources; list metadata only unless a policy explicitly allows more.
- Treat `delete`, `scale`, `restart`, and `apply` as policy-sensitive.
- Keep kubeconfig, bearer token, certificate, and TLS fields redacted in diagnostics.
- Keep live cluster fixture tests opt-in; deterministic tests should not require a cluster.

## CLI And Capabilities

- CLI commands include `test`, `contexts`, `namespaces`, workload/resource list commands, `get`, `delete`, `logs`, `apply`, `scale`, `restart`, and `tui`.
- Capabilities: `kubernetes.diagnostics`, `kubernetes.contexts`, `kubernetes.namespaces`, `kubernetes.list`, `kubernetes.get_yaml`, `kubernetes.logs`, `kubernetes.delete`, `kubernetes.scale`, `kubernetes.restart`, `kubernetes.apply`.

## Validation

- Focused gate: `cargo test -p voidb-plugin-kubernetes`.
- Add `cargo test -p voidb-cli invoke` for capability or generic invoke changes.
- Fixture gate when feasible: `scripts/kubernetes-fixture-smoke.sh`.
- Always run `git diff --check`.
