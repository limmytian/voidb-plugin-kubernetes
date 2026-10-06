use std::fs;
use std::path::Path;
use voidb_plugin_kubernetes::kubernetes_capabilities;

fn main() -> anyhow::Result<()> {
    let schemas_dir = Path::new("schemas");
    fs::create_dir_all(schemas_dir)?;

    let capabilities = kubernetes_capabilities();

    // Export capability schemas
    for cap in &capabilities {
        let input_path = schemas_dir.join(format!("{}-input.schema.json", cap.id));
        let output_path = schemas_dir.join(format!("{}-output.schema.json", cap.id));

        fs::write(&input_path, serde_json::to_string_pretty(&cap.input_schema)? + "\n")?;
        fs::write(&output_path, serde_json::to_string_pretty(&cap.output_schema)? + "\n")?;
        println!("Exported schemas for capability: {}", cap.id);
    }

    // Export profile schema matching K8sConfig
    let profile_schema = serde_json::json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "KubernetesConnectionProfile",
        "type": "object",
        "required": ["connection"],
        "properties": {
            "connection": {
                "type": "object",
                "required": ["type"],
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["Kubeconfig", "Direct"]
                    },
                    "path": {
                        "type": ["string", "null"],
                        "description": "Path to kubeconfig file (None = ~/.kube/config or KUBECONFIG env)"
                    },
                    "context": {
                        "type": ["string", "null"],
                        "description": "Context to use (None = current context)"
                    },
                    "api_url": {
                        "type": "string",
                        "description": "API server URL for direct connection, e.g. https://k8s.example.com:6443"
                    },
                    "auth": {
                        "type": "object",
                        "required": ["type"],
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["Token", "ClientCert", "InCluster"]
                            },
                            "token": {
                                "type": "string",
                                "description": "Bearer token"
                            },
                            "cert_path": {
                                "type": "string",
                                "description": "Client certificate path"
                            },
                            "key_path": {
                                "type": "string",
                                "description": "Client private key path"
                            }
                        }
                    },
                    "verify_ssl": {
                        "type": "boolean",
                        "default": true,
                        "description": "Whether to verify TLS certificates"
                    },
                    "ca_cert": {
                        "type": ["string", "null"],
                        "description": "Path to CA certificate (PEM)"
                    }
                }
            },
            "default_namespace": {
                "type": ["string", "null"],
                "description": "Default namespace (None = 'default')"
            },
            "timeout": {
                "type": "integer",
                "minimum": 1,
                "default": 30,
                "description": "Request timeout in seconds"
            }
        },
        "additionalProperties": false
    });

    let profile_path = schemas_dir.join("profile.schema.json");
    fs::write(&profile_path, serde_json::to_string_pretty(&profile_schema)? + "\n")?;
    println!("Exported profile schema to {}", profile_path.display());

    Ok(())
}
