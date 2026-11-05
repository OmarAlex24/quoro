// SPDX-License-Identifier: AGPL-3.0-only
//! Fetching from OSV and CISA.
//!
//! The client is built like the webhook dispatcher's: native roots, the ring
//! provider, no redirect chain longer than five hops. A zip streams straight
//! to `<data_dir>/vulns-download/<bucket>.zip.part` and is renamed when it is
//! complete, so the memory the primary uses does not follow npm's 220 MB. The
//! small bodies (a change list, one record, the KEV catalog) are read into
//! memory with their cap enforced while reading, never by trusting
//! `Content-Length`.
//!
//! The primary has 2 GB of RAM, so the caps are not politeness: they are what
//! keeps one hostile or broken response from taking the host down.

use super::ecosystem;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Where the data comes from. Tests point this at a local mock server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Endpoints {
    /// The OSV bucket base, without a trailing slash.
    pub osv_base: String,
    /// The CISA KEV catalog.
    pub kev_url: String,
}

impl Default for Endpoints {
    fn default() -> Self {
        Self {
            osv_base: "https://osv-vulnerabilities.storage.googleapis.com".to_string(),
            kev_url: "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"
                .to_string(),
        }
    }
}

/// The size and count limits one pass works under.
///
/// The defaults are the contract's; tests lower them so a tiny body exercises
/// the same path a 2 GB archive would.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest bucket archive, in bytes.
    pub max_zip_bytes: u64,
    /// The largest change list, in bytes.
    pub max_csv_bytes: u64,
    /// The largest single record body, in bytes.
    pub max_record_bytes: u64,
    /// The largest KEV catalog, in bytes.
    pub max_kev_bytes: u64,
    /// Above this many ids, a delta becomes a full download.
    pub delta_max_ids: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_zip_bytes: 2 * 1024 * 1024 * 1024,
            max_csv_bytes: 64 * 1024 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            max_kev_bytes: 32 * 1024 * 1024,
            delta_max_ids: 5_000,
        }
    }
}

/// The directory downloads land in, next to the database.
const DOWNLOAD_DIR: &str = "vulns-download";

/// How long a zip download may take.
const ZIP_TIMEOUT: Duration = Duration::from_secs(30 * 60);
/// How long every other request may take.
const SMALL_TIMEOUT: Duration = Duration::from_secs(2 * 60);
/// How long the connect may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The HTTP client the mirror and the fetcher share.
///
/// # Errors
///
/// The TLS backend could not be initialized.
pub fn client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent(format!("quoro/{}", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|error| format!("cannot build the vulnerability data client: {error}"))
}

/// One pass's fetcher: the shared client, where the data comes from, where a
/// download lands and the limits.
pub struct Fetch<'a> {
    client: &'a reqwest::Client,
    endpoints: &'a Endpoints,
    data_dir: &'a Path,
    limits: Limits,
}

impl<'a> Fetch<'a> {
    #[must_use]
    pub fn new(
        client: &'a reqwest::Client,
        endpoints: &'a Endpoints,
        data_dir: &'a Path,
        limits: Limits,
    ) -> Self {
        Self {
            client,
            endpoints,
            data_dir,
            limits,
        }
    }

    /// Downloads one bucket's archive and returns the path of the complete
    /// file. A part file is left behind only while the download runs.
    ///
    /// # Errors
    ///
    /// A refused bucket name, a non-2xx status, a transport error, a body
    /// larger than the cap, or a file that could not be written.
    pub async fn download_zip(&self, bucket: &str) -> Result<PathBuf, String> {
        let url = ecosystem::all_zip_url(&self.endpoints.osv_base, bucket)
            .ok_or_else(|| format!("{bucket:?} is not an OSV bucket"))?;
        let dir = self.data_dir.join(DOWNLOAD_DIR);
        std::fs::create_dir_all(&dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
        let part = dir.join(format!("{bucket}.zip.part"));
        let complete = dir.join(format!("{bucket}.zip"));
        let result = self.stream_zip(&url, &part).await;
        match result {
            Ok(()) => {
                std::fs::rename(&part, &complete).map_err(|error| {
                    let _ = std::fs::remove_file(&part);
                    format!("cannot rename {}: {error}", part.display())
                })?;
                Ok(complete)
            }
            Err(error) => {
                let _ = std::fs::remove_file(&part);
                Err(error)
            }
        }
    }

    async fn stream_zip(&self, url: &str, path: &Path) -> Result<(), String> {
        use std::io::Write as _;
        let mut response = self.send(url, ZIP_TIMEOUT).await?;
        let mut file = std::fs::File::create(path)
            .map_err(|error| format!("cannot create {}: {error}", path.display()))?;
        let mut total = 0u64;
        loop {
            let chunk = response
                .chunk()
                .await
                .map_err(|error| format!("cannot read {url}: {error}"))?;
            let Some(chunk) = chunk else {
                break;
            };
            total += chunk.len() as u64;
            if total > self.limits.max_zip_bytes {
                return Err(format!(
                    "{url} is larger than the {} byte cap",
                    self.limits.max_zip_bytes
                ));
            }
            file.write_all(&chunk)
                .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        }
        file.flush()
            .map_err(|error| format!("cannot write {}: {error}", path.display()))?;
        Ok(())
    }

    /// Reads a small body into memory, refusing one larger than `cap` while
    /// reading it.
    ///
    /// # Errors
    ///
    /// A non-2xx status, a transport error, or a body over the cap.
    pub async fn get_text(&self, url: &str, cap: u64) -> Result<String, String> {
        let bytes = self.get_bytes(url, cap).await?;
        String::from_utf8(bytes).map_err(|error| format!("{url} is not UTF-8: {error}"))
    }

    /// Reads a small body into memory, refusing one larger than `cap` while
    /// reading it.
    ///
    /// # Errors
    ///
    /// A non-2xx status, a transport error, or a body over the cap.
    pub async fn get_bytes(&self, url: &str, cap: u64) -> Result<Vec<u8>, String> {
        let mut response = self.send(url, SMALL_TIMEOUT).await?;
        let mut body = Vec::new();
        loop {
            let chunk = response
                .chunk()
                .await
                .map_err(|error| format!("cannot read {url}: {error}"))?;
            let Some(chunk) = chunk else {
                break;
            };
            if body.len() as u64 + chunk.len() as u64 > cap {
                return Err(format!("{url} is larger than the {cap} byte cap"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }

    /// Like [`Fetch::get_bytes`], but a 404 is `None`: the record was removed.
    ///
    /// # Errors
    ///
    /// Any other non-2xx status, a transport error, or a body over the cap.
    pub async fn get_optional(&self, url: &str, cap: u64) -> Result<Option<Vec<u8>>, String> {
        let mut response = self
            .client
            .get(url)
            .timeout(SMALL_TIMEOUT)
            .send()
            .await
            .map_err(|error| format!("cannot fetch {url}: {error}"))?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(format!("{url} answered {}", response.status()));
        }
        let mut body = Vec::new();
        loop {
            let chunk = response
                .chunk()
                .await
                .map_err(|error| format!("cannot read {url}: {error}"))?;
            let Some(chunk) = chunk else {
                break;
            };
            if body.len() as u64 + chunk.len() as u64 > cap {
                return Err(format!("{url} is larger than the {cap} byte cap"));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Some(body))
    }

    async fn send(&self, url: &str, timeout: Duration) -> Result<reqwest::Response, String> {
        let response = self
            .client
            .get(url)
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| format!("cannot fetch {url}: {error}"))?;
        if !response.status().is_success() {
            return Err(format!("{url} answered {}", response.status()));
        }
        Ok(response)
    }
}

/// Deletes the part files a previous run left in `data_dir`.
///
/// A crash mid-download leaves one there; it is worthless on the next start.
pub fn clear_leftovers(data_dir: &Path) {
    let dir = data_dir.join(DOWNLOAD_DIR);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .extension()
            .is_some_and(|extension| extension == "part")
        {
            let _ = std::fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_endpoints_are_the_real_ones() {
        let endpoints = Endpoints::default();
        assert_eq!(
            endpoints.osv_base,
            "https://osv-vulnerabilities.storage.googleapis.com"
        );
        assert!(
            endpoints
                .kev_url
                .ends_with("known_exploited_vulnerabilities.json")
        );
    }

    #[test]
    fn the_default_limits_match_the_contract() {
        let limits = Limits::default();
        assert_eq!(limits.max_zip_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(limits.max_csv_bytes, 64 * 1024 * 1024);
        assert_eq!(limits.max_record_bytes, 16 * 1024 * 1024);
        assert_eq!(limits.max_kev_bytes, 32 * 1024 * 1024);
        assert_eq!(limits.delta_max_ids, 5_000);
    }

    #[test]
    fn leftovers_are_part_files_only() {
        let dir =
            std::env::temp_dir().join(format!("quoro-vulns-{}", crate::store::random_secret()));
        let download = dir.join(DOWNLOAD_DIR);
        std::fs::create_dir_all(&download).unwrap();
        std::fs::write(download.join("Debian.zip.part"), b"half").unwrap();
        std::fs::write(download.join("Debian.zip"), b"whole").unwrap();
        clear_leftovers(&dir);
        assert!(!download.join("Debian.zip.part").exists());
        assert!(download.join("Debian.zip").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
