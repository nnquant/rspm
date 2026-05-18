use std::path::{Path, PathBuf};

pub fn task_log_path(log_dir: &Path, task_name: &str) -> PathBuf {
    log_dir.join(format!("{task_name}.log"))
}
