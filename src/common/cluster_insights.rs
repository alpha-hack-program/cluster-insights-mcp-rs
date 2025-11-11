use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use kube::{Api, Client};
use k8s_openapi::api::core::v1::{Node, Pod, Namespace};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;

use super::metrics::{increment_requests, increment_errors, RequestTimer};

use rmcp::{
    ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{ServerCapabilities, ServerInfo, CallToolResult, Content},
    ErrorData as McpError,
    schemars, tool, tool_handler, tool_router,
};

// =================== DATA STRUCTURES ===================

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct ClusterCapacityResponse {
    #[schemars(description = "Total CPU in cores")]
    pub total_cpu_cores: f64,
    #[schemars(description = "Total memory in GB")]
    pub total_memory_gb: f64,
    #[schemars(description = "Allocated CPU (requests) in cores")]
    pub allocated_cpu_cores: f64,
    #[schemars(description = "Allocated memory (requests) in GB")]
    pub allocated_memory_gb: f64,
    #[schemars(description = "Available CPU in cores")]
    pub available_cpu_cores: f64,
    #[schemars(description = "Available memory in GB")]
    pub available_memory_gb: f64,
    #[schemars(description = "Number of nodes")]
    pub node_count: usize,
    #[schemars(description = "Explanation of capacity calculation")]
    pub explanation: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct CheckResourceFitParams {
    #[schemars(description = "Required CPU in cores")]
    pub cpu_cores: f64,
    #[schemars(description = "Required memory in GB")]
    pub memory_gb: f64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct CheckResourceFitResponse {
    #[schemars(description = "Whether resources fit in cluster")]
    pub fits: bool,
    #[schemars(description = "Available CPU in cores")]
    pub available_cpu_cores: f64,
    #[schemars(description = "Available memory in GB")]
    pub available_memory_gb: f64,
    #[schemars(description = "CPU utilization percentage")]
    pub cpu_utilization_percent: f64,
    #[schemars(description = "Memory utilization percentage")]
    pub memory_utilization_percent: f64,
    #[schemars(description = "Explanation of fit check")]
    pub explanation: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct NodeInfo {
    #[schemars(description = "Node name")]
    pub name: String,
    #[schemars(description = "Total CPU in cores")]
    pub total_cpu_cores: f64,
    #[schemars(description = "Total memory in GB")]
    pub total_memory_gb: f64,
    #[schemars(description = "Allocated CPU (requests) in cores")]
    pub allocated_cpu_cores: f64,
    #[schemars(description = "Allocated memory (requests) in GB")]
    pub allocated_memory_gb: f64,
    #[schemars(description = "Available CPU in cores")]
    pub available_cpu_cores: f64,
    #[schemars(description = "Available memory in GB")]
    pub available_memory_gb: f64,
    #[schemars(description = "Number of pods on node")]
    pub pod_count: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct NodeBreakdownResponse {
    #[schemars(description = "List of nodes with their resource information")]
    pub nodes: Vec<NodeInfo>,
    #[schemars(description = "Total number of nodes")]
    pub total_nodes: usize,
    #[schemars(description = "Explanation of node breakdown")]
    pub explanation: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct NamespaceUsage {
    #[schemars(description = "Namespace name")]
    pub namespace: String,
    #[schemars(description = "CPU requests in cores")]
    pub cpu_requests_cores: f64,
    #[schemars(description = "Memory requests in GB")]
    pub memory_requests_gb: f64,
    #[schemars(description = "CPU limits in cores")]
    pub cpu_limits_cores: f64,
    #[schemars(description = "Memory limits in GB")]
    pub memory_limits_gb: f64,
    #[schemars(description = "Number of pods in namespace")]
    pub pod_count: usize,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct NamespaceUsageResponse {
    #[schemars(description = "List of namespaces with their resource usage")]
    pub namespaces: Vec<NamespaceUsage>,
    #[schemars(description = "Total number of namespaces")]
    pub total_namespaces: usize,
    #[schemars(description = "Explanation of namespace usage")]
    pub explanation: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, schemars::JsonSchema)]
pub struct PodResourceInfo {
    #[schemars(description = "Pod name")]
    pub name: String,
    #[schemars(description = "Namespace")]
    pub namespace: String,
    #[schemars(description = "CPU requests in millicores")]
    pub cpu_requests_millicores: i64,
    #[schemars(description = "Memory requests in MB")]
    pub memory_requests_mb: i64,
    #[schemars(description = "CPU limits in millicores")]
    pub cpu_limits_millicores: i64,
    #[schemars(description = "Memory limits in MB")]
    pub memory_limits_mb: i64,
    #[schemars(description = "Node name")]
    pub node: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct PodResourceStatsResponse {
    #[schemars(description = "Top pods by resource consumption")]
    pub top_pods: Vec<PodResourceInfo>,
    #[schemars(description = "Total number of pods")]
    pub total_pods: usize,
    #[schemars(description = "Sort criteria used")]
    pub sorted_by: String,
    #[schemars(description = "Explanation of pod resource stats")]
    pub explanation: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct CheckReplicaCapacityParams {
    #[schemars(description = "Application or pod name pattern to find")]
    pub app_name: String,
    #[schemars(description = "Namespace to search in")]
    pub namespace: String,
    #[schemars(description = "Number of additional replicas needed")]
    pub replica_count: i32,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, schemars::JsonSchema)]
pub struct CheckReplicaCapacityResponse {
    #[schemars(description = "Whether replicas can fit in cluster")]
    pub fits: bool,
    #[schemars(description = "Name of the reference pod used for calculations")]
    pub reference_pod: String,
    #[schemars(description = "CPU required per replica in cores")]
    pub cpu_per_replica_cores: f64,
    #[schemars(description = "Memory required per replica in GB")]
    pub memory_per_replica_gb: f64,
    #[schemars(description = "Total CPU required for all replicas in cores")]
    pub total_cpu_required_cores: f64,
    #[schemars(description = "Total memory required for all replicas in GB")]
    pub total_memory_required_gb: f64,
    #[schemars(description = "Available CPU in cluster in cores")]
    pub available_cpu_cores: f64,
    #[schemars(description = "Available memory in cluster in GB")]
    pub available_memory_gb: f64,
    #[schemars(description = "Current number of matching pods")]
    pub current_pod_count: usize,
    #[schemars(description = "CPU utilization percentage after adding replicas")]
    pub projected_cpu_utilization_percent: f64,
    #[schemars(description = "Memory utilization percentage after adding replicas")]
    pub projected_memory_utilization_percent: f64,
    #[schemars(description = "Detailed explanation of capacity check")]
    pub explanation: String,
}

// =================== HELPER FUNCTIONS ===================

/// Parse Kubernetes quantity to cores (CPU)
fn quantity_to_cores(quantity: &Quantity) -> f64 {
    let s = &quantity.0;
    if s.is_empty() {
        return 0.0;
    }
    
    // Handle millicores (e.g., "100m")
    if s.ends_with('m') {
        if let Ok(millicores) = s[..s.len() - 1].parse::<f64>() {
            return millicores / 1000.0;
        }
    }
    
    // Handle cores (e.g., "2", "0.5")
    if let Ok(cores) = s.parse::<f64>() {
        return cores;
    }
    
    0.0
}

/// Parse Kubernetes quantity to GB (memory)
fn quantity_to_gb(quantity: &Quantity) -> f64 {
    let s = &quantity.0;
    if s.is_empty() {
        return 0.0;
    }
    
    // Handle various memory units
    let (value, unit) = if s.ends_with("Ki") {
        (s[..s.len() - 2].parse::<f64>().ok(), 1024.0)
    } else if s.ends_with("Mi") {
        (s[..s.len() - 2].parse::<f64>().ok(), 1024.0 * 1024.0)
    } else if s.ends_with("Gi") {
        (s[..s.len() - 2].parse::<f64>().ok(), 1024.0 * 1024.0 * 1024.0)
    } else if s.ends_with("Ti") {
        (s[..s.len() - 2].parse::<f64>().ok(), 1024.0 * 1024.0 * 1024.0 * 1024.0)
    } else if s.ends_with("K") {
        (s[..s.len() - 1].parse::<f64>().ok(), 1000.0)
    } else if s.ends_with("M") {
        (s[..s.len() - 1].parse::<f64>().ok(), 1000.0 * 1000.0)
    } else if s.ends_with("G") {
        (s[..s.len() - 1].parse::<f64>().ok(), 1000.0 * 1000.0 * 1000.0)
    } else if s.ends_with("T") {
        (s[..s.len() - 1].parse::<f64>().ok(), 1000.0 * 1000.0 * 1000.0 * 1000.0)
    } else {
        // Assume bytes
        (s.parse::<f64>().ok(), 1.0)
    };
    
    if let Some(v) = value {
        v * unit / (1024.0 * 1024.0 * 1024.0) // Convert to GB
    } else {
        0.0
    }
}

/// Parse Kubernetes quantity to MB (memory)
fn quantity_to_mb(quantity: &Quantity) -> i64 {
    (quantity_to_gb(quantity) * 1024.0) as i64
}

/// Parse Kubernetes quantity to millicores (CPU)
fn quantity_to_millicores(quantity: &Quantity) -> i64 {
    (quantity_to_cores(quantity) * 1000.0) as i64
}

// =================== CLUSTER INSIGHTS ===================

#[derive(Debug, Clone)]
pub struct ClusterInsights {
    tool_router: ToolRouter<Self>,
}

impl ClusterInsights {
    /// Get cluster capacity
    async fn get_cluster_capacity_internal() -> Result<ClusterCapacityResponse, String> {
        let client = Client::try_default().await
            .map_err(|e| format!("Failed to create Kubernetes client: {}", e))?;
        
        let nodes_api: Api<Node> = Api::all(client.clone());
        let pods_api: Api<Pod> = Api::all(client.clone());
        
        let nodes = nodes_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list nodes: {}", e))?;
        
        let pods = pods_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list pods: {}", e))?;
        
        let mut total_cpu_cores = 0.0;
        let mut total_memory_gb = 0.0;
        
        for node in &nodes.items {
            if let Some(status) = &node.status {
                if let Some(capacity) = &status.capacity {
                    if let Some(cpu) = capacity.get("cpu") {
                        total_cpu_cores += quantity_to_cores(cpu);
                    }
                    if let Some(memory) = capacity.get("memory") {
                        total_memory_gb += quantity_to_gb(memory);
                    }
                }
            }
        }
        
        let mut allocated_cpu_cores = 0.0;
        let mut allocated_memory_gb = 0.0;
        
        for pod in &pods.items {
            if let Some(spec) = &pod.spec {
                for container in &spec.containers {
                    if let Some(resources) = &container.resources {
                        if let Some(requests) = &resources.requests {
                            if let Some(cpu) = requests.get("cpu") {
                                allocated_cpu_cores += quantity_to_cores(cpu);
                            }
                            if let Some(memory) = requests.get("memory") {
                                allocated_memory_gb += quantity_to_gb(memory);
                            }
                        }
                    }
                }
            }
        }
        
        let available_cpu_cores = total_cpu_cores - allocated_cpu_cores;
        let available_memory_gb = total_memory_gb - allocated_memory_gb;
        
        let node_count = nodes.items.len();
        
        let explanation = format!(
            "Cluster has {} nodes. Total capacity: {:.2} CPU cores, {:.2} GB memory. \
             Allocated (requests): {:.2} CPU cores ({:.1}%), {:.2} GB memory ({:.1}%). \
             Available: {:.2} CPU cores, {:.2} GB memory.",
            node_count,
            total_cpu_cores, total_memory_gb,
            allocated_cpu_cores, (allocated_cpu_cores / total_cpu_cores * 100.0),
            allocated_memory_gb, (allocated_memory_gb / total_memory_gb * 100.0),
            available_cpu_cores, available_memory_gb
        );
        
        Ok(ClusterCapacityResponse {
            total_cpu_cores,
            total_memory_gb,
            allocated_cpu_cores,
            allocated_memory_gb,
            available_cpu_cores,
            available_memory_gb,
            node_count,
            explanation,
        })
    }
    
    /// Check if resources fit
    async fn check_resource_fit_internal(cpu_cores: f64, memory_gb: f64) -> Result<CheckResourceFitResponse, String> {
        let capacity = Self::get_cluster_capacity_internal().await?;
        
        let fits = capacity.available_cpu_cores >= cpu_cores && capacity.available_memory_gb >= memory_gb;
        
        let cpu_utilization_percent = if capacity.total_cpu_cores > 0.0 {
            (capacity.allocated_cpu_cores + cpu_cores) / capacity.total_cpu_cores * 100.0
        } else {
            0.0
        };
        
        let memory_utilization_percent = if capacity.total_memory_gb > 0.0 {
            (capacity.allocated_memory_gb + memory_gb) / capacity.total_memory_gb * 100.0
        } else {
            0.0
        };
        
        let explanation = if fits {
            format!(
                "Resources FIT in cluster. Requested: {:.2} CPU cores, {:.2} GB memory. \
                 Available: {:.2} CPU cores, {:.2} GB memory. \
                 After allocation, cluster would be at {:.1}% CPU and {:.1}% memory utilization.",
                cpu_cores, memory_gb,
                capacity.available_cpu_cores, capacity.available_memory_gb,
                cpu_utilization_percent, memory_utilization_percent
            )
        } else {
            let cpu_shortage = if capacity.available_cpu_cores < cpu_cores {
                format!("CPU shortage: {:.2} cores needed but only {:.2} available. ", 
                    cpu_cores - capacity.available_cpu_cores, capacity.available_cpu_cores)
            } else {
                String::new()
            };
            let memory_shortage = if capacity.available_memory_gb < memory_gb {
                format!("Memory shortage: {:.2} GB needed but only {:.2} GB available.",
                    memory_gb - capacity.available_memory_gb, capacity.available_memory_gb)
            } else {
                String::new()
            };
            
            format!(
                "Resources DO NOT FIT in cluster. Requested: {:.2} CPU cores, {:.2} GB memory. \
                 Available: {:.2} CPU cores, {:.2} GB memory. {}{}",
                cpu_cores, memory_gb,
                capacity.available_cpu_cores, capacity.available_memory_gb,
                cpu_shortage, memory_shortage
            )
        };
        
        Ok(CheckResourceFitResponse {
            fits,
            available_cpu_cores: capacity.available_cpu_cores,
            available_memory_gb: capacity.available_memory_gb,
            cpu_utilization_percent,
            memory_utilization_percent,
            explanation,
        })
    }
    
    /// Get node breakdown
    async fn get_node_breakdown_internal() -> Result<NodeBreakdownResponse, String> {
        let client = Client::try_default().await
            .map_err(|e| format!("Failed to create Kubernetes client: {}", e))?;
        
        let nodes_api: Api<Node> = Api::all(client.clone());
        let pods_api: Api<Pod> = Api::all(client.clone());
        
        let nodes = nodes_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list nodes: {}", e))?;
        
        let pods = pods_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list pods: {}", e))?;
        
        // Build node resource map
        let mut node_infos = Vec::new();
        
        for node in &nodes.items {
            let name = node.metadata.name.clone().unwrap_or_default();
            
            let mut total_cpu_cores = 0.0;
            let mut total_memory_gb = 0.0;
            
            if let Some(status) = &node.status {
                if let Some(capacity) = &status.capacity {
                    if let Some(cpu) = capacity.get("cpu") {
                        total_cpu_cores = quantity_to_cores(cpu);
                    }
                    if let Some(memory) = capacity.get("memory") {
                        total_memory_gb = quantity_to_gb(memory);
                    }
                }
            }
            
            let mut allocated_cpu_cores = 0.0;
            let mut allocated_memory_gb = 0.0;
            let mut pod_count = 0;
            
            for pod in &pods.items {
                if let Some(spec) = &pod.spec {
                    if spec.node_name.as_deref() == Some(&name) {
                        pod_count += 1;
                        
                        for container in &spec.containers {
                            if let Some(resources) = &container.resources {
                                if let Some(requests) = &resources.requests {
                                    if let Some(cpu) = requests.get("cpu") {
                                        allocated_cpu_cores += quantity_to_cores(cpu);
                                    }
                                    if let Some(memory) = requests.get("memory") {
                                        allocated_memory_gb += quantity_to_gb(memory);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            
            let available_cpu_cores = total_cpu_cores - allocated_cpu_cores;
            let available_memory_gb = total_memory_gb - allocated_memory_gb;
            
            node_infos.push(NodeInfo {
                name,
                total_cpu_cores,
                total_memory_gb,
                allocated_cpu_cores,
                allocated_memory_gb,
                available_cpu_cores,
                available_memory_gb,
                pod_count,
            });
        }
        
        let explanation = format!(
            "Cluster has {} nodes. Each node shows total capacity, allocated resources (requests), \
             available resources, and pod count.",
            node_infos.len()
        );
        
        Ok(NodeBreakdownResponse {
            total_nodes: node_infos.len(),
            nodes: node_infos,
            explanation,
        })
    }
    
    /// Get namespace usage
    async fn get_namespace_usage_internal() -> Result<NamespaceUsageResponse, String> {
        let client = Client::try_default().await
            .map_err(|e| format!("Failed to create Kubernetes client: {}", e))?;
        
        let namespaces_api: Api<Namespace> = Api::all(client.clone());
        let pods_api: Api<Pod> = Api::all(client.clone());
        
        let namespaces = namespaces_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list namespaces: {}", e))?;
        
        let pods = pods_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list pods: {}", e))?;
        
        let mut namespace_usage_map: HashMap<String, NamespaceUsage> = HashMap::new();
        
        // Initialize namespace usage
        for ns in &namespaces.items {
            let name = ns.metadata.name.clone().unwrap_or_default();
            namespace_usage_map.insert(name.clone(), NamespaceUsage {
                namespace: name,
                cpu_requests_cores: 0.0,
                memory_requests_gb: 0.0,
                cpu_limits_cores: 0.0,
                memory_limits_gb: 0.0,
                pod_count: 0,
            });
        }
        
        // Aggregate pod resources by namespace
        for pod in &pods.items {
            let ns_name = pod.metadata.namespace.clone().unwrap_or_else(|| "default".to_string());
            
            let usage = namespace_usage_map.entry(ns_name.clone()).or_insert_with(|| NamespaceUsage {
                namespace: ns_name.clone(),
                cpu_requests_cores: 0.0,
                memory_requests_gb: 0.0,
                cpu_limits_cores: 0.0,
                memory_limits_gb: 0.0,
                pod_count: 0,
            });
            
            usage.pod_count += 1;
            
            if let Some(spec) = &pod.spec {
                for container in &spec.containers {
                    if let Some(resources) = &container.resources {
                        if let Some(requests) = &resources.requests {
                            if let Some(cpu) = requests.get("cpu") {
                                usage.cpu_requests_cores += quantity_to_cores(cpu);
                            }
                            if let Some(memory) = requests.get("memory") {
                                usage.memory_requests_gb += quantity_to_gb(memory);
                            }
                        }
                        if let Some(limits) = &resources.limits {
                            if let Some(cpu) = limits.get("cpu") {
                                usage.cpu_limits_cores += quantity_to_cores(cpu);
                            }
                            if let Some(memory) = limits.get("memory") {
                                usage.memory_limits_gb += quantity_to_gb(memory);
                            }
                        }
                    }
                }
            }
        }
        
        let mut namespace_usages: Vec<NamespaceUsage> = namespace_usage_map.into_values().collect();
        namespace_usages.sort_by(|a, b| b.cpu_requests_cores.partial_cmp(&a.cpu_requests_cores).unwrap());
        
        let total_namespaces = namespace_usages.len();
        
        let explanation = format!(
            "Cluster has {} namespaces. Resource usage shows CPU/memory requests and limits for each namespace, \
             sorted by CPU requests (descending).",
            total_namespaces
        );
        
        Ok(NamespaceUsageResponse {
            total_namespaces,
            namespaces: namespace_usages,
            explanation,
        })
    }
    
    /// Get pod resource stats
    async fn get_pod_resource_stats_internal() -> Result<PodResourceStatsResponse, String> {
        let client = Client::try_default().await
            .map_err(|e| format!("Failed to create Kubernetes client: {}", e))?;
        
        let pods_api: Api<Pod> = Api::all(client.clone());
        let pods = pods_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list pods: {}", e))?;
        
        let mut pod_infos = Vec::new();
        
        for pod in &pods.items {
            let name = pod.metadata.name.clone().unwrap_or_default();
            let namespace = pod.metadata.namespace.clone().unwrap_or_else(|| "default".to_string());
            let node = pod.spec.as_ref()
                .and_then(|s| s.node_name.clone())
                .unwrap_or_else(|| "unscheduled".to_string());
            
            let mut cpu_requests_millicores = 0i64;
            let mut memory_requests_mb = 0i64;
            let mut cpu_limits_millicores = 0i64;
            let mut memory_limits_mb = 0i64;
            
            if let Some(spec) = &pod.spec {
                for container in &spec.containers {
                    if let Some(resources) = &container.resources {
                        if let Some(requests) = &resources.requests {
                            if let Some(cpu) = requests.get("cpu") {
                                cpu_requests_millicores += quantity_to_millicores(cpu);
                            }
                            if let Some(memory) = requests.get("memory") {
                                memory_requests_mb += quantity_to_mb(memory);
                            }
                        }
                        if let Some(limits) = &resources.limits {
                            if let Some(cpu) = limits.get("cpu") {
                                cpu_limits_millicores += quantity_to_millicores(cpu);
                            }
                            if let Some(memory) = limits.get("memory") {
                                memory_limits_mb += quantity_to_mb(memory);
                            }
                        }
                    }
                }
            }
            
            pod_infos.push(PodResourceInfo {
                name,
                namespace,
                cpu_requests_millicores,
                memory_requests_mb,
                cpu_limits_millicores,
                memory_limits_mb,
                node,
            });
        }
        
        // Sort by CPU requests (descending)
        pod_infos.sort_by(|a, b| b.cpu_requests_millicores.cmp(&a.cpu_requests_millicores));
        
        let total_pods = pod_infos.len();
        
        // Take top 20 pods
        let top_pods: Vec<PodResourceInfo> = pod_infos.into_iter().take(20).collect();
        
        let explanation = format!(
            "Showing top 20 pods (out of {}) by CPU requests. Each pod shows CPU/memory requests and limits, \
             along with the node it's scheduled on.",
            total_pods
        );
        
        Ok(PodResourceStatsResponse {
            top_pods,
            total_pods,
            sorted_by: "CPU requests (descending)".to_string(),
            explanation,
        })
    }

    /// Check replica capacity
    async fn check_replica_capacity_internal(
        app_name: String,
        namespace: String,
        replica_count: i32,
    ) -> Result<CheckReplicaCapacityResponse, String> {
        if replica_count <= 0 {
            return Err("Replica count must be positive".to_string());
        }
        
        let client = Client::try_default().await
            .map_err(|e| format!("Failed to create Kubernetes client: {}", e))?;
        
        let pods_api: Api<Pod> = Api::namespaced(client.clone(), &namespace);
        let pods = pods_api.list(&Default::default()).await
            .map_err(|e| format!("Failed to list pods in namespace {}: {}", namespace, e))?;
        
        // Find pods matching the app name
        let matching_pods: Vec<&Pod> = pods.items.iter()
            .filter(|pod| {
                pod.metadata.name.as_ref()
                    .map(|name| name.contains(&app_name))
                    .unwrap_or(false)
            })
            .collect();
        
        if matching_pods.is_empty() {
            return Err(format!(
                "No pods found matching '{}' in namespace '{}'",
                app_name, namespace
            ));
        }
        
        // Use the first matching pod as reference
        let reference_pod = matching_pods[0];
        let reference_pod_name = reference_pod.metadata.name.clone().unwrap_or_default();
        
        // Calculate resource requirements from the reference pod
        let mut cpu_per_replica = 0.0;
        let mut memory_per_replica = 0.0;
        
        if let Some(spec) = &reference_pod.spec {
            for container in &spec.containers {
                if let Some(resources) = &container.resources {
                    if let Some(requests) = &resources.requests {
                        if let Some(cpu) = requests.get("cpu") {
                            cpu_per_replica += quantity_to_cores(cpu);
                        }
                        if let Some(memory) = requests.get("memory") {
                            memory_per_replica += quantity_to_gb(memory);
                        }
                    }
                }
            }
        }
        
        // Calculate total resources needed
        let total_cpu_required = cpu_per_replica * replica_count as f64;
        let total_memory_required = memory_per_replica * replica_count as f64;
        
        // Get cluster capacity
        let capacity = Self::get_cluster_capacity_internal().await?;
        
        // Check if resources fit
        let fits = capacity.available_cpu_cores >= total_cpu_required 
                   && capacity.available_memory_gb >= total_memory_required;
        
        // Calculate projected utilization
        let projected_cpu_utilization = if capacity.total_cpu_cores > 0.0 {
            (capacity.allocated_cpu_cores + total_cpu_required) / capacity.total_cpu_cores * 100.0
        } else {
            0.0
        };
        
        let projected_memory_utilization = if capacity.total_memory_gb > 0.0 {
            (capacity.allocated_memory_gb + total_memory_required) / capacity.total_memory_gb * 100.0
        } else {
            0.0
        };
        
        // Build explanation
        let explanation = if fits {
            format!(
                "✓ Capacity CHECK PASSED: You can add {} more replicas of '{}' in namespace '{}'.\n\
                 \n\
                 Reference pod: {}\n\
                 - CPU per replica: {:.3} cores\n\
                 - Memory per replica: {:.3} GB\n\
                 \n\
                 Total required for {} replicas:\n\
                 - CPU: {:.3} cores\n\
                 - Memory: {:.3} GB\n\
                 \n\
                 Cluster availability:\n\
                 - Available CPU: {:.3} cores (enough for {:.0} replicas)\n\
                 - Available Memory: {:.3} GB (enough for {:.0} replicas)\n\
                 \n\
                 Projected utilization after adding replicas:\n\
                 - CPU: {:.1}% (current: {:.1}%)\n\
                 - Memory: {:.1}% (current: {:.1}%)\n\
                 \n\
                 Current pods matching '{}': {}",
                replica_count, app_name, namespace,
                reference_pod_name,
                cpu_per_replica,
                memory_per_replica,
                replica_count,
                total_cpu_required,
                total_memory_required,
                capacity.available_cpu_cores,
                if cpu_per_replica > 0.0 { capacity.available_cpu_cores / cpu_per_replica } else { 0.0 },
                capacity.available_memory_gb,
                if memory_per_replica > 0.0 { capacity.available_memory_gb / memory_per_replica } else { 0.0 },
                projected_cpu_utilization,
                capacity.allocated_cpu_cores / capacity.total_cpu_cores * 100.0,
                projected_memory_utilization,
                capacity.allocated_memory_gb / capacity.total_memory_gb * 100.0,
                app_name,
                matching_pods.len()
            )
        } else {
            let mut issues = vec![];
            
            if capacity.available_cpu_cores < total_cpu_required {
                let shortfall = total_cpu_required - capacity.available_cpu_cores;
                let max_replicas = (capacity.available_cpu_cores / cpu_per_replica).floor() as i32;
                issues.push(format!(
                    "CPU shortage: Need {:.3} cores but only {:.3} available (shortfall: {:.3} cores). \
                     Maximum possible replicas based on CPU: {}",
                    total_cpu_required, capacity.available_cpu_cores, shortfall, max_replicas
                ));
            }
            
            if capacity.available_memory_gb < total_memory_required {
                let shortfall = total_memory_required - capacity.available_memory_gb;
                let max_replicas = (capacity.available_memory_gb / memory_per_replica).floor() as i32;
                issues.push(format!(
                    "Memory shortage: Need {:.3} GB but only {:.3} GB available (shortfall: {:.3} GB). \
                     Maximum possible replicas based on memory: {}",
                    total_memory_required, capacity.available_memory_gb, shortfall, max_replicas
                ));
            }
            
            format!(
                "✗ Capacity CHECK FAILED: Cannot add {} replicas of '{}' in namespace '{}'.\n\
                 \n\
                 Reference pod: {}\n\
                 - CPU per replica: {:.3} cores\n\
                 - Memory per replica: {:.3} GB\n\
                 \n\
                 Total required for {} replicas:\n\
                 - CPU: {:.3} cores\n\
                 - Memory: {:.3} GB\n\
                 \n\
                 Issues:\n{}\n\
                 \n\
                 Current pods matching '{}': {}",
                replica_count, app_name, namespace,
                reference_pod_name,
                cpu_per_replica,
                memory_per_replica,
                replica_count,
                total_cpu_required,
                total_memory_required,
                issues.join("\n"),
                app_name,
                matching_pods.len()
            )
        };
        
        Ok(CheckReplicaCapacityResponse {
            fits,
            reference_pod: reference_pod_name,
            cpu_per_replica_cores: cpu_per_replica,
            memory_per_replica_gb: memory_per_replica,
            total_cpu_required_cores: total_cpu_required,
            total_memory_required_gb: total_memory_required,
            available_cpu_cores: capacity.available_cpu_cores,
            available_memory_gb: capacity.available_memory_gb,
            current_pod_count: matching_pods.len(),
            projected_cpu_utilization_percent: projected_cpu_utilization,
            projected_memory_utilization_percent: projected_memory_utilization,
            explanation,
        })
    }
}

#[tool_router]
impl ClusterInsights {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
        }
    }

    /// Get cluster capacity
    #[tool(description = "Get total cluster capacity, allocated resources (requests), and available resources. \
                          Returns detailed information about CPU cores and memory in GB across all nodes. \
                          Example: Returns total 24 CPU cores, 96 GB memory, with 12 cores and 48 GB allocated.")]
    pub async fn get_cluster_capacity(&self) -> Result<CallToolResult, McpError> {
        let _timer = RequestTimer::new();
        increment_requests();

        match Self::get_cluster_capacity_internal().await {
            Ok(result) => {
                match serde_json::to_string_pretty(&result) {
                    Ok(json_str) => Ok(CallToolResult::success(vec![Content::text(json_str)])),
                    Err(e) => {
                        increment_errors();
                        Ok(CallToolResult::error(vec![Content::text(format!(
                            "Error serializing response: {}", e
                        ))]))
                    }
                }
            }
            Err(e) => {
                increment_errors();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to get cluster capacity: {}", e
                ))]))
            }
        }
    }

    /// Check if resources fit in cluster
    #[tool(description = "Check if specified CPU and memory resources can fit in the cluster. \
                          Parameters: cpu_cores (float), memory_gb (float). \
                          Returns whether resources fit, available resources, and utilization percentages. \
                          Example: cpu_cores=4, memory_gb=16 → checks if 4 cores and 16GB available.")]
    pub async fn check_resource_fit(
        &self,
        params: Parameters<CheckResourceFitParams>
    ) -> Result<CallToolResult, McpError> {
        let _timer = RequestTimer::new();
        increment_requests();

        if params.0.cpu_cores < 0.0 {
            increment_errors();
            return Ok(CallToolResult::error(vec![Content::text(
                "CPU cores must be non-negative".to_string()
            )]));
        }

        if params.0.memory_gb < 0.0 {
            increment_errors();
            return Ok(CallToolResult::error(vec![Content::text(
                "Memory GB must be non-negative".to_string()
            )]));
        }

        match Self::check_resource_fit_internal(params.0.cpu_cores, params.0.memory_gb).await {
            Ok(result) => {
                match serde_json::to_string_pretty(&result) {
                    Ok(json_str) => Ok(CallToolResult::success(vec![Content::text(json_str)])),
                    Err(e) => {
                        increment_errors();
                        Ok(CallToolResult::error(vec![Content::text(format!(
                            "Error serializing response: {}", e
                        ))]))
                    }
                }
            }
            Err(e) => {
                increment_errors();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to check resource fit: {}", e
                ))]))
            }
        }
    }

    /// Get node breakdown
    #[tool(description = "Get detailed breakdown of each node in the cluster. \
                          Lists each node with its total capacity, allocated resources (requests), \
                          available resources, and pod count. \
                          Example: Returns list of nodes with their CPU/memory capacity and usage.")]
    pub async fn get_node_breakdown(&self) -> Result<CallToolResult, McpError> {
        let _timer = RequestTimer::new();
        increment_requests();

        match Self::get_node_breakdown_internal().await {
            Ok(result) => {
                match serde_json::to_string_pretty(&result) {
                    Ok(json_str) => Ok(CallToolResult::success(vec![Content::text(json_str)])),
                    Err(e) => {
                        increment_errors();
                        Ok(CallToolResult::error(vec![Content::text(format!(
                            "Error serializing response: {}", e
                        ))]))
                    }
                }
            }
            Err(e) => {
                increment_errors();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to get node breakdown: {}", e
                ))]))
            }
        }
    }

    /// Get namespace resource usage
    #[tool(description = "Get resource usage per namespace. \
                          Returns CPU/memory requests and limits for each namespace, along with pod count. \
                          Results are sorted by CPU requests (descending). \
                          Example: Returns namespaces with their total CPU/memory consumption.")]
    pub async fn get_namespace_usage(&self) -> Result<CallToolResult, McpError> {
        let _timer = RequestTimer::new();
        increment_requests();

        match Self::get_namespace_usage_internal().await {
            Ok(result) => {
                match serde_json::to_string_pretty(&result) {
                    Ok(json_str) => Ok(CallToolResult::success(vec![Content::text(json_str)])),
                    Err(e) => {
                        increment_errors();
                        Ok(CallToolResult::error(vec![Content::text(format!(
                            "Error serializing response: {}", e
                        ))]))
                    }
                }
            }
            Err(e) => {
                increment_errors();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to get namespace usage: {}", e
                ))]))
            }
        }
    }

    /// Get pod resource statistics
    #[tool(description = "Get top pods by resource consumption. \
                          Returns the top 20 pods sorted by CPU requests, showing CPU/memory requests and limits. \
                          Includes namespace, node assignment, and resource metrics in millicores and MB. \
                          Example: Returns top resource-consuming pods across the cluster.")]
    pub async fn get_pod_resource_stats(&self) -> Result<CallToolResult, McpError> {
        let _timer = RequestTimer::new();
        increment_requests();

        match Self::get_pod_resource_stats_internal().await {
            Ok(result) => {
                match serde_json::to_string_pretty(&result) {
                    Ok(json_str) => Ok(CallToolResult::success(vec![Content::text(json_str)])),
                    Err(e) => {
                        increment_errors();
                        Ok(CallToolResult::error(vec![Content::text(format!(
                            "Error serializing response: {}", e
                        ))]))
                    }
                }
            }
            Err(e) => {
                increment_errors();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to get pod resource stats: {}", e
                ))]))
            }
        }
    }

    /// Check replica capacity
    #[tool(description = "Check if cluster has capacity to add more replicas of an application. \
                          Finds an existing pod matching the app name in the specified namespace, \
                          calculates its resource requirements, and checks if the cluster can accommodate \
                          the requested number of additional replicas. \
                          Parameters: app_name (string) - name or pattern to match pods, \
                          namespace (string) - Kubernetes namespace, \
                          replica_count (int) - number of additional replicas needed. \
                          Returns detailed capacity analysis including per-replica requirements, total needs, \
                          cluster availability, and projected utilization. \
                          Example: app_name='my-application', namespace='default', replica_count=10")]
    pub async fn check_replica_capacity(
        &self,
        params: Parameters<CheckReplicaCapacityParams>
    ) -> Result<CallToolResult, McpError> {
        let _timer = RequestTimer::new();
        increment_requests();

        if params.0.replica_count <= 0 {
            increment_errors();
            return Ok(CallToolResult::error(vec![Content::text(
                "Replica count must be positive".to_string()
            )]));
        }

        if params.0.app_name.is_empty() {
            increment_errors();
            return Ok(CallToolResult::error(vec![Content::text(
                "Application name cannot be empty".to_string()
            )]));
        }

        if params.0.namespace.is_empty() {
            increment_errors();
            return Ok(CallToolResult::error(vec![Content::text(
                "Namespace cannot be empty".to_string()
            )]));
        }

        match Self::check_replica_capacity_internal(
            params.0.app_name,
            params.0.namespace,
            params.0.replica_count,
        ).await {
            Ok(result) => {
                match serde_json::to_string_pretty(&result) {
                    Ok(json_str) => Ok(CallToolResult::success(vec![Content::text(json_str)])),
                    Err(e) => {
                        increment_errors();
                        Ok(CallToolResult::error(vec![Content::text(format!(
                            "Error serializing response: {}", e
                        ))]))
                    }
                }
            }
            Err(e) => {
                increment_errors();
                Ok(CallToolResult::error(vec![Content::text(format!(
                    "Failed to check replica capacity: {}", e
                ))]))
            }
        }
    }
}

#[tool_handler]
impl ServerHandler for ClusterInsights {
    fn get_info(&self) -> ServerInfo {
        // Read basic information from .env file (replaced by sync script during release)
        let name = "cluster-insights-mcp-rs".to_string();
        let version = "1.3.2".to_string();
        let title = "Cluster Insights Engine MCP Server".to_string();
        let website_url = "https://github.com/alpha-hack-program/cluster-insights-mcp-rs.git".to_string();

        ServerInfo {
            instructions: Some(
                "Kubernetes Cluster Insights providing resource analysis functions:\
                 \n\n1. get_cluster_capacity - Get total cluster capacity, allocated resources, and availability\
                 \n2. check_resource_fit - Check if specified resources can fit in the cluster\
                 \n3. get_node_breakdown - Get detailed breakdown of each node's resources\
                 \n4. get_namespace_usage - Get resource usage per namespace\
                 \n5. get_pod_resource_stats - Get top pods by resource consumption\
                 \n6. check_replica_capacity - Check if cluster can accommodate additional application replicas\
                 \n\nAll functions query live Kubernetes cluster data via kubeconfig.".into()
            ),
            capabilities: ServerCapabilities::builder().enable_tools().build(),
            server_info: rmcp::model::Implementation {
                name: name,
                version: version, 
                title: Some(title), 
                icons: None, 
                website_url: Some(website_url) 
            },
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quantity_to_cores() {
        assert_eq!(quantity_to_cores(&Quantity("2".to_string())), 2.0);
        assert_eq!(quantity_to_cores(&Quantity("500m".to_string())), 0.5);
        assert_eq!(quantity_to_cores(&Quantity("100m".to_string())), 0.1);
    }

    #[test]
    fn test_quantity_to_gb() {
        assert_eq!(quantity_to_gb(&Quantity("1Gi".to_string())), 1.0);
        assert_eq!(quantity_to_gb(&Quantity("512Mi".to_string())), 0.5);
    }

    // Test the engine to get the cluster capacity
    #[tokio::test]
    async fn test_get_cluster_capacity() {
        let cluster_insights = ClusterInsights::new();
        let result = cluster_insights.get_cluster_capacity().await;
        match result {
            Ok(call_result) => {
                println!("Cluster capacity: {:?}", call_result);
            },
            Err(e) => panic!("Error inesperado: {}", e),
        }
    }

    // Test the engine to check if resources fit
    #[tokio::test]
    async fn test_check_resource_fit() {
        let cluster_insights = ClusterInsights::new();
        let result = cluster_insights.check_resource_fit(Parameters(CheckResourceFitParams { cpu_cores: 1.0, memory_gb: 1.0 })).await;
        match result {
            Ok(call_result) => {
                println!("Check resource fit: {:?}", call_result);
            },
            Err(e) => panic!("Error inesperado: {}", e),
        }
    }

    // Test the engine to get the node breakdown
    #[tokio::test]
    async fn test_get_node_breakdown() {
        let cluster_insights = ClusterInsights::new();
        let result = cluster_insights.get_node_breakdown().await;
        match result {
            Ok(call_result) => {
                println!("Node breakdown: {:?}", call_result);
            },
            Err(e) => panic!("Error inesperado: {}", e),
        }
    }

    // Test the engine to check replica capacity
    #[tokio::test]
    async fn test_check_replica_capacity() {
        let cluster_insights = ClusterInsights::new();
        let result = cluster_insights.check_replica_capacity(Parameters(CheckReplicaCapacityParams {
            app_name: "test".to_string(),
            namespace: "default".to_string(),
            replica_count: 10,
        })).await;
        match result {
            Ok(call_result) => {
                println!("Check replica capacity: {:?}", call_result);
            },
            Err(e) => panic!("Error inesperado: {}", e),
        }
    }
}
