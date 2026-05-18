use std::collections::{BTreeMap, BTreeSet};

use petgraph::algo::toposort;
use petgraph::graphmap::DiGraphMap;
use thiserror::Error;

use crate::config::ProjectConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskPlan {
    pub start_order: Vec<String>,
    pub stop_order: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TaskGraph {
    tasks: BTreeSet<String>,
    dependencies: BTreeMap<String, Vec<String>>,
}

impl TaskGraph {
    pub fn from_config(config: &ProjectConfig) -> Result<Self, DagError> {
        let tasks: BTreeSet<String> = config.tasks.keys().cloned().collect();
        let mut dependencies = BTreeMap::new();

        for (task_name, task) in &config.tasks {
            for dependency in &task.depends_on {
                if !tasks.contains(dependency) {
                    return Err(DagError::new(
                        DagErrorKind::UnknownDependency,
                        format!("task [{task_name}] depends on unknown task [{dependency}]"),
                    ));
                }
            }
            dependencies.insert(task_name.clone(), task.depends_on.clone());
        }

        Ok(Self {
            tasks,
            dependencies,
        })
    }

    pub fn plan_all(&self) -> Result<TaskPlan, DagError> {
        let mut graph = DiGraphMap::<&str, ()>::new();

        for task in &self.tasks {
            graph.add_node(task.as_str());
        }

        for (task, dependencies) in &self.dependencies {
            for dependency in dependencies {
                graph.add_edge(dependency.as_str(), task.as_str(), ());
            }
        }

        let start_order = toposort(&graph, None)
            .map_err(|cycle| {
                DagError::new(
                    DagErrorKind::Cycle,
                    format!("dependency cycle detected at task [{}]", cycle.node_id()),
                )
            })?
            .into_iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        let stop_order = start_order.iter().rev().cloned().collect();

        Ok(TaskPlan {
            start_order,
            stop_order,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DagErrorKind {
    UnknownDependency,
    Cycle,
}

#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct DagError {
    kind: DagErrorKind,
    message: String,
}

impl DagError {
    pub fn new(kind: DagErrorKind, message: String) -> Self {
        Self { kind, message }
    }

    pub fn kind(&self) -> DagErrorKind {
        self.kind
    }
}
