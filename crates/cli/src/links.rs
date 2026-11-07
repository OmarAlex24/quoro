//! Linked folders: which project and app a directory works on.
//!
//! `links.toml`, next to `config.toml`, maps an absolute directory to a
//! context and the ids of a project and, optionally, an app:
//!
//! ```toml
//! [links."/home/me/src/web"]
//! context = "lab"
//! project_id = "prj_..."
//! app_id = "app_..."
//! ```
//!
//! Nothing is written inside the directory, so a repository never carries a
//! link. A command looks the link up from the current directory upwards: the
//! nearest linked ancestor wins. The file holds no secret (mode `0644`) and is
//! replaced in one rename.
//!
//! A link names its context. When the command does not choose one
//! (`--context`, `QUORO_CONTEXT`, `QUORO_URL`), the link's context is used
//! before `current`. The link's project and app apply only on that context:
//! ids from one cluster are never sent to another (`scope.rs`).

use crate::Environment;
use crate::context::{self, write_atomic};
use crate::error::Failure;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LinksFile {
    #[serde(default)]
    pub links: BTreeMap<String, Link>,
}

/// One linked directory.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Link {
    pub context: String,
    pub project_id: String,
    pub app_id: Option<String>,
}

/// `links.toml` and its path.
pub struct Links {
    pub path: PathBuf,
    pub file: LinksFile,
}

impl Links {
    pub fn read(environment: &Environment) -> Result<Self, Failure> {
        let path = context::config_dir(environment)?.join("links.toml");
        let file = match std::fs::read_to_string(&path) {
            Ok(text) => toml::from_str(&text).map_err(|e| {
                Failure::other(format!("cannot read {}: {}", path.display(), e.message()))
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => LinksFile::default(),
            Err(e) => {
                return Err(Failure::other(format!(
                    "cannot read {}: {e}",
                    path.display()
                )));
            }
        };
        Ok(Self { path, file })
    }

    /// The nearest linked directory at or above `directory`, and its link.
    pub fn nearest(&self, directory: &Path) -> Option<(PathBuf, &Link)> {
        directory.ancestors().find_map(|dir| {
            let key = dir.to_str()?;
            self.file
                .links
                .get(key)
                .map(|link| (dir.to_path_buf(), link))
        })
    }

    pub fn set(&mut self, directory: &Path, link: Link) -> Result<(), Failure> {
        let key = directory
            .to_str()
            .ok_or_else(|| Failure::usage("the directory's path must be UTF-8 to be linked"))?;
        self.file.links.insert(key.to_string(), link);
        Ok(())
    }

    pub fn remove(&mut self, directory: &Path) -> Option<Link> {
        self.file.links.remove(directory.to_str()?)
    }

    /// Drops every link of `context` to the project `project_id`; returns
    /// how many there were.
    pub fn forget_project(&mut self, context: &str, project_id: &str) -> usize {
        let before = self.file.links.len();
        self.file
            .links
            .retain(|_, link| !(link.context == context && link.project_id == project_id));
        before - self.file.links.len()
    }

    /// Clears the app of every link of `context` to the app `app_id`.
    pub fn forget_app(&mut self, context: &str, app_id: &str) -> usize {
        let mut cleared = 0;
        for link in self.file.links.values_mut() {
            if link.context == context && link.app_id.as_deref() == Some(app_id) {
                link.app_id = None;
                cleared += 1;
            }
        }
        cleared
    }

    pub fn write(&self) -> Result<(), Failure> {
        let mut text = String::new();
        for (directory, link) in &self.file.links {
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&format!("[links.{}]\n", context::toml_string(directory)));
            text.push_str(&format!(
                "context = {}\n",
                context::toml_string(&link.context)
            ));
            text.push_str(&format!(
                "project_id = {}\n",
                context::toml_string(&link.project_id)
            ));
            if let Some(app_id) = &link.app_id {
                text.push_str(&format!("app_id = {}\n", context::toml_string(app_id)));
            }
        }
        write_atomic(&self.path, text.as_bytes(), 0o644)
    }
}

/// The directory a command runs in.
pub fn current_dir(environment: &Environment) -> Result<PathBuf, Failure> {
    environment
        .cwd
        .clone()
        .filter(|path| path.is_absolute())
        .ok_or_else(|| Failure::other("cannot read the current directory"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(project: &str, app: Option<&str>) -> Link {
        Link {
            context: "lab".to_string(),
            project_id: project.to_string(),
            app_id: app.map(str::to_string),
        }
    }

    #[test]
    fn the_nearest_linked_ancestor_wins_and_the_file_reads_back() {
        let base = std::env::temp_dir().join(format!(
            "quoro-cli-links-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        let environment =
            Environment::new([("XDG_CONFIG_HOME", base.display().to_string())], false);
        let mut links = Links::read(&environment).unwrap();
        assert!(links.nearest(Path::new("/src/web")).is_none());
        links
            .set(Path::new("/src"), link("prj_outer", None))
            .unwrap();
        links
            .set(Path::new("/src/a \"b\""), link("prj_inner", Some("app_1")))
            .unwrap();
        links.write().unwrap();

        let links = Links::read(&environment).unwrap();
        let (dir, found) = links.nearest(Path::new("/src/a \"b\"/deep/er")).unwrap();
        assert_eq!(dir, Path::new("/src/a \"b\""));
        assert_eq!(found, &link("prj_inner", Some("app_1")));
        let (dir, found) = links.nearest(Path::new("/src/other")).unwrap();
        assert_eq!(
            (dir.as_path(), found.project_id.as_str()),
            (Path::new("/src"), "prj_outer")
        );
        assert!(links.nearest(Path::new("/elsewhere")).is_none());
    }
}
