// SPDX-License-Identifier: AGPL-3.0-only
//! Starting the app: finding the program, preparing its directory, and
//! spawning it as a child of PID 1.

use std::os::unix::fs::DirBuilderExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};

use quoro_init::GuestConfig;
use rustix::io::Errno;

use crate::guest::Fault;
use crate::guest::user::Identity;

/// `PATH` for the app when the config sets none; the same list runc uses.
pub(crate) const DEFAULT_PATH: &str =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// `PATH` from the config environment, or [`DEFAULT_PATH`].
pub(crate) fn path_env(env: &[(String, String)]) -> String {
    env.iter()
        .find(|(key, _)| key == "PATH")
        .map(|(_, value)| value.clone())
        .unwrap_or_else(|| DEFAULT_PATH.to_string())
}

/// The app's environment: the config's, plus `HOME` and `PATH` when it does
/// not set them.
fn app_env(env: &[(String, String)], home: &str) -> Vec<(String, String)> {
    let mut out = env.to_vec();
    if !out.iter().any(|(key, _)| key == "HOME") {
        out.push(("HOME".to_string(), home.to_string()));
    }
    if !out.iter().any(|(key, _)| key == "PATH") {
        out.push(("PATH".to_string(), DEFAULT_PATH.to_string()));
    }
    out
}

/// A regular file the app may execute.
fn is_executable(path: &str) -> bool {
    match std::fs::metadata(path) {
        Ok(metadata) => metadata.is_file() && metadata.permissions().mode() & 0o111 != 0,
        Err(_) => false,
    }
}

/// Resolve `args[0]`: a name with no `/` is searched in `PATH`, an empty `PATH`
/// entry meaning the current directory. Anything else is used as given. In both
/// cases the result must be a regular file with an execute bit.
pub(crate) fn resolve_program(entrypoint: &str, path: &str) -> Option<String> {
    if entrypoint.contains('/') {
        return is_executable(entrypoint).then_some(entrypoint.to_string());
    }
    for directory in path.split(':') {
        let directory = if directory.is_empty() { "." } else { directory };
        let candidate = format!("{directory}/{entrypoint}");
        if is_executable(&candidate) {
            return Some(candidate);
        }
    }
    None
}

/// Create the working directory (0755) if the image does not have it.
pub(crate) fn prepare_workdir(workdir: Option<&str>) -> Result<(), Fault> {
    let Some(workdir) = workdir else {
        return Ok(());
    };
    if std::path::Path::new(workdir).is_dir() {
        return Ok(());
    }
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true).mode(0o755);
    builder
        .create(workdir)
        .map_err(|err| Fault::new(format!("create workdir {workdir}: {err}")))
}

/// Spawn the app with its own process group, pipes on stdout/stderr and
/// `/dev/null` on stdin.
pub(crate) fn spawn(
    config: &GuestConfig,
    identity: &Identity,
    program: &str,
) -> Result<Child, Fault> {
    let entrypoint = config.args.first().cloned().unwrap_or_default();

    let mut command = Command::new(program);
    command.arg0(&entrypoint);
    for argument in config.args.iter().skip(1) {
        command.arg(argument);
    }

    command.env_clear();
    for (key, value) in app_env(&config.env, &identity.home) {
        command.env(key, value);
    }

    command.current_dir(config.workdir.as_deref().unwrap_or("/"));
    // With a uid set, `std` calls `setgroups(0)` before `setuid`, so the app
    // runs with no supplementary groups; groups from `/etc/group` are not
    // added (a gap documented in the README).
    command.uid(identity.uid);
    command.gid(identity.gid);
    // Its own process group, so the init can signal the whole tree and the app
    // cannot reach our console.
    command.process_group(0);
    command.stdin(Stdio::null());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    command
        .spawn()
        .map_err(|err| spawn_fault(&entrypoint, &err))
}

/// A spawn failure: missing or non-executable entrypoint is 127, anything else
/// is 1.
fn spawn_fault(entrypoint: &str, err: &std::io::Error) -> Fault {
    let message = format!("spawn {entrypoint:?}: {err}");
    match err.raw_os_error() {
        Some(code) => {
            let errno = Errno::from_raw_os_error(code);
            if errno == Errno::NOENT || errno == Errno::ACCESS {
                Fault::not_found(message)
            } else {
                Fault::new(message)
            }
        }
        None => Fault::new(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("quoro-init-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap_or_else(|err| panic!("mkdir: {err}"));
        dir
    }

    #[test]
    fn path_comes_from_the_environment_first() {
        let env = vec![("PATH".to_string(), "/opt/bin".to_string())];
        assert_eq!(path_env(&env), "/opt/bin");
        assert_eq!(path_env(&[]), DEFAULT_PATH);
    }

    #[test]
    fn home_and_path_defaults_are_appended_not_overridden() {
        let env = vec![
            ("HOME".to_string(), "/from/config".to_string()),
            ("PATH".to_string(), "/from/config".to_string()),
        ];
        assert_eq!(app_env(&env, "/from/passwd"), env);

        let out = app_env(&[], "/home/app");
        assert_eq!(
            out,
            vec![
                ("HOME".to_string(), "/home/app".to_string()),
                ("PATH".to_string(), DEFAULT_PATH.to_string()),
            ]
        );
    }

    #[test]
    fn a_bare_name_is_search_found() {
        let dir = scratch("resolve");
        let program = dir.join("tool");
        std::fs::write(&program, b"#!/bin/sh\n").unwrap_or_else(|err| panic!("{err}"));
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755))
            .unwrap_or_else(|err| panic!("{err}"));

        let path = dir.to_string_lossy().into_owned();
        assert_eq!(
            resolve_program("tool", &path),
            Some(program.to_string_lossy().into_owned())
        );
        assert_eq!(resolve_program("missing", &path), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_file_without_an_execute_bit_is_refused() {
        let dir = scratch("noexec");
        let program = dir.join("tool");
        std::fs::write(&program, b"#!/bin/sh\n").unwrap_or_else(|err| panic!("{err}"));
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o644))
            .unwrap_or_else(|err| panic!("{err}"));

        let path = dir.to_string_lossy().into_owned();
        assert_eq!(resolve_program("tool", &path), None);
        assert_eq!(resolve_program(&path, DEFAULT_PATH), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_explicit_path_is_used_as_given() {
        let dir = scratch("explicit");
        let program = dir.join("tool");
        std::fs::write(&program, b"#!/bin/sh\n").unwrap_or_else(|err| panic!("{err}"));
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700))
            .unwrap_or_else(|err| panic!("{err}"));

        let explicit = program.to_string_lossy().into_owned();
        assert_eq!(resolve_program(&explicit, ""), Some(explicit));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
