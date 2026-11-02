// SPDX-License-Identifier: AGPL-3.0-only
//! Turning `user=` into a uid, a gid and a home directory.
//!
//! The rules mirror runc's: a name comes from `/etc/passwd`, a bare number is
//! a uid (with the passwd entry's gid and home when there is one, and gid 0
//! when there is not), and `user:group` resolves each side as a name or a
//! number. A name that does not exist is a boot failure, never a silent 0.

use crate::guest::Fault;

/// Who the app runs as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
    /// `HOME` for the app, from `/etc/passwd` or `/`.
    pub(crate) home: String,
}

/// One parsed `/etc/passwd` line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PasswdEntry {
    name: String,
    uid: u32,
    gid: u32,
    home: String,
}

/// One parsed `/etc/group` line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupEntry {
    name: String,
    gid: u32,
}

/// Split a colon-separated database and keep the entries that have enough
/// fields. Comments and blank lines do not match the field count and are
/// skipped.
fn passwd_entries(text: &str) -> Vec<PasswdEntry> {
    let mut entries = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() < 7 {
            continue;
        }
        let (Ok(uid), Ok(gid)) = (fields[2].parse::<u32>(), fields[3].parse::<u32>()) else {
            continue;
        };
        entries.push(PasswdEntry {
            name: fields[0].to_string(),
            uid,
            gid,
            home: fields[5].to_string(),
        });
    }
    entries
}

fn group_entries(text: &str) -> Vec<GroupEntry> {
    let mut entries = Vec::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() < 3 {
            continue;
        }
        let Ok(gid) = fields[2].parse::<u32>() else {
            continue;
        };
        entries.push(GroupEntry {
            name: fields[0].to_string(),
            gid,
        });
    }
    entries
}

fn passwd_by_name<'a>(entries: &'a [PasswdEntry], name: &str) -> Option<&'a PasswdEntry> {
    entries.iter().find(|entry| entry.name == name)
}

fn passwd_by_uid(entries: &[PasswdEntry], uid: u32) -> Option<&PasswdEntry> {
    entries.iter().find(|entry| entry.uid == uid)
}

/// Resolve a user side: a name, or a number with its passwd entry when there
/// is one.
fn resolve_user_part(spec: &str, passwd: &[PasswdEntry]) -> Option<(u32, String)> {
    if let Ok(uid) = spec.parse::<u32>() {
        return match passwd_by_uid(passwd, uid) {
            Some(entry) => Some((uid, entry.home.clone())),
            None => Some((uid, "/".to_string())),
        };
    }
    passwd_by_name(passwd, spec).map(|entry| (entry.uid, entry.home.clone()))
}

/// Resolve a group side: a name or a number.
fn resolve_group_part(spec: &str, group: &[GroupEntry]) -> Option<u32> {
    if let Ok(gid) = spec.parse::<u32>() {
        return Some(gid);
    }
    group
        .iter()
        .find(|entry| entry.name == spec)
        .map(|entry| entry.gid)
}

/// The pure resolution, testable against sample database text.
fn resolve_in(spec: Option<&str>, passwd_text: &str, group_text: &str) -> Option<Identity> {
    let passwd = passwd_entries(passwd_text);
    let group = group_entries(group_text);

    let Some(spec) = spec else {
        // No `user=`: root, and root's home is not looked up.
        return Some(Identity {
            uid: 0,
            gid: 0,
            home: "/".to_string(),
        });
    };

    match spec.split_once(':') {
        None => {
            let (uid, home) = resolve_user_part(spec, &passwd)?;
            // A named user keeps its passwd gid; a bare number that is not in
            // passwd gets 0, exactly like runc.
            let gid = match spec.parse::<u32>() {
                Ok(uid) => passwd_by_uid(&passwd, uid).map_or(0, |entry| entry.gid),
                Err(_) => passwd_by_name(&passwd, spec)?.gid,
            };
            Some(Identity { uid, gid, home })
        }
        Some((user, group_spec)) => {
            let (uid, home) = resolve_user_part(user, &passwd)?;
            let gid = resolve_group_part(group_spec, &group)?;
            Some(Identity { uid, gid, home })
        }
    }
}

/// Read a file from the new root, or the empty string when it is missing.
fn read_or_empty(path: &str) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Resolve `user=` against the new root's databases.
pub(crate) fn resolve(spec: Option<&str>) -> Result<Identity, Fault> {
    let passwd = read_or_empty("/etc/passwd");
    let group = read_or_empty("/etc/group");
    resolve_in(spec, &passwd, &group).ok_or_else(|| {
        Fault::new(format!(
            "user {:?} not found in /etc/passwd or /etc/group",
            spec.unwrap_or("(none)")
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASSWD: &str = "root:x:0:0:root:/root:/bin/sh\n\
         nobody:x:65534:65534:nobody:/:/sbin/nologin\n\
         app:x:1000:1000:App:/home/app:/bin/sh\n\
         # a comment\n\
         malformed\n";

    const GROUP: &str = "root:x:0:root\nnobody:x:65534:\napp:x:1000:\nstaff:x:50:app\n";

    #[test]
    fn no_user_is_root() {
        assert_eq!(
            resolve_in(None, PASSWD, GROUP),
            Some(Identity {
                uid: 0,
                gid: 0,
                home: "/".to_string()
            })
        );
    }

    #[test]
    fn a_name_gets_its_passwd_uid_gid_and_home() {
        assert_eq!(
            resolve_in(Some("app"), PASSWD, GROUP),
            Some(Identity {
                uid: 1000,
                gid: 1000,
                home: "/home/app".to_string()
            })
        );
    }

    #[test]
    fn a_name_without_an_entry_fails() {
        assert_eq!(resolve_in(Some("ghost"), PASSWD, GROUP), None);
    }

    #[test]
    fn a_numeric_uid_uses_its_passwd_entry() {
        assert_eq!(
            resolve_in(Some("65534"), PASSWD, GROUP),
            Some(Identity {
                uid: 65534,
                gid: 65534,
                home: "/".to_string()
            })
        );
    }

    #[test]
    fn an_unknown_numeric_uid_gets_gid_zero() {
        assert_eq!(
            resolve_in(Some("4242"), PASSWD, GROUP),
            Some(Identity {
                uid: 4242,
                gid: 0,
                home: "/".to_string()
            })
        );
    }

    #[test]
    fn user_and_group_names() {
        assert_eq!(
            resolve_in(Some("app:staff"), PASSWD, GROUP),
            Some(Identity {
                uid: 1000,
                gid: 50,
                home: "/home/app".to_string()
            })
        );
    }

    #[test]
    fn user_and_group_numbers() {
        assert_eq!(
            resolve_in(Some("1000:50"), PASSWD, GROUP),
            Some(Identity {
                uid: 1000,
                gid: 50,
                home: "/home/app".to_string()
            })
        );
    }

    #[test]
    fn mixed_name_and_number() {
        assert_eq!(
            resolve_in(Some("app:50"), PASSWD, GROUP),
            Some(Identity {
                uid: 1000,
                gid: 50,
                home: "/home/app".to_string()
            })
        );
        assert_eq!(
            resolve_in(Some("1000:staff"), PASSWD, GROUP),
            Some(Identity {
                uid: 1000,
                gid: 50,
                home: "/home/app".to_string()
            })
        );
    }

    #[test]
    fn unknown_group_name_fails() {
        assert_eq!(resolve_in(Some("app:ghost"), PASSWD, GROUP), None);
        assert_eq!(resolve_in(Some("ghost_staff:app"), PASSWD, GROUP), None);
    }

    #[test]
    fn missing_databases_still_resolve_numbers() {
        assert_eq!(
            resolve_in(Some("1000:1000"), "", ""),
            Some(Identity {
                uid: 1000,
                gid: 1000,
                home: "/".to_string()
            })
        );
        assert_eq!(resolve_in(Some("app"), "", ""), None);
    }
}
