use rspm_core::config::ProjectConfig;
use rspm_core::dag::{DagErrorKind, TaskGraph};

fn config_with_deps(deps: &str) -> ProjectConfig {
    ProjectConfig::from_toml_str(&format!(
        r#"
        [project]
        name = "dag-test"

        [tasks.master]
        cmd = "true"

        [tasks.ctp_md]
        cmd = "true"
        depends_on = ["master"]

        [tasks.strategy]
        cmd = "true"
        {deps}
        "#
    ))
    .expect("valid config")
}

#[test]
fn plans_start_order_and_reverse_stop_order() {
    let config = config_with_deps(r#"depends_on = ["ctp_md"]"#);
    let graph = TaskGraph::from_config(&config).expect("valid graph");
    let plan = graph.plan_all().expect("plan");

    assert_eq!(plan.start_order, vec!["master", "ctp_md", "strategy"]);
    assert_eq!(plan.stop_order, vec!["strategy", "ctp_md", "master"]);
}

#[test]
fn rejects_unknown_dependency() {
    let config = config_with_deps(r#"depends_on = ["missing"]"#);
    let error = TaskGraph::from_config(&config).expect_err("unknown dependency");

    assert_eq!(error.kind(), DagErrorKind::UnknownDependency);
    assert!(error.to_string().contains("strategy"));
    assert!(error.to_string().contains("missing"));
}

#[test]
fn rejects_dependency_cycles() {
    let config = ProjectConfig::from_toml_str(
        r#"
        [project]
        name = "cycle-test"

        [tasks.a]
        cmd = "true"
        depends_on = ["b"]

        [tasks.b]
        cmd = "true"
        depends_on = ["a"]
        "#,
    )
    .expect("valid config");

    let graph = TaskGraph::from_config(&config).expect("graph construction allows cycle check");
    let error = graph.plan_all().expect_err("cycle detected");

    assert_eq!(error.kind(), DagErrorKind::Cycle);
    assert!(error.to_string().contains("cycle"));
}
