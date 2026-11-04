// SPDX-License-Identifier: AGPL-3.0-only
//! An S3-compatible client for the `object_store` provider entries: backups
//! (`docs/contracts/backup-format.md` sections 5, 9 and 10) and the off-site
//! audit copy (`docs/contracts/audit-log-format.md` section 8).
//!
//! Requests use path-style addressing (`<endpoint>/<bucket>/<key>`) and AWS
//! Signature Version 4 with header signing, service `s3`, and the SHA-256 of
//! the body in `x-amz-content-sha256` (a signed payload). Bodies are at most a
//! few MiB, so multipart upload is not implemented (backup-format.md section
//! 11). The secret access key never leaves [`S3Config`]/[`S3Client`] memory and
//! is never printed: debug output names the endpoint, region and bucket only.

use std::fmt;
use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use base64::Engine as _;
use quick_xml::events::Event;
use reqwest::{Client, Method, Response, Url};
use zeroize::Zeroizing;

use crate::object_store::{
    ComplianceLock, ObjectHead, ObjectInfo, ObjectStore, ObjectStoreError, ObjectVersion,
};

/// One request may take this long before it counts as a transport failure.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// A `GET` body larger than this is refused, so a hostile or broken server
/// cannot exhaust the agent's memory.
const MAX_BODY: usize = 16 * 1024 * 1024;
/// The checksum header S3 wants on a write (`x-amz-checksum-sha256`).
const CHECKSUM_HEADER: &str = "x-amz-checksum-sha256";

/// Credentials and addressing for one bucket (backup-format.md section 10).
#[derive(Clone)]
pub struct S3Config {
    /// `https://` origin. `http://` is accepted only for a loopback address,
    /// which the integration tests use.
    pub endpoint: String,
    /// `auto` is fine for providers that do not care.
    pub region: String,
    pub bucket: String,
    pub access_key_id: String,
    pub secret_access_key: Zeroizing<String>,
}

impl fmt::Debug for S3Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .field("bucket", &self.bucket)
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[redacted]")
            .finish()
    }
}

/// A client against one bucket. Cloning shares the connection pool.
#[derive(Clone)]
pub struct S3Client {
    http: Client,
    /// `<endpoint>/<bucket>`, no trailing slash.
    base: String,
    region: String,
    identity: Identity,
    settings: SigningSettings,
}

impl fmt::Debug for S3Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Client")
            .field("base", &self.base)
            .field("region", &self.region)
            .finish()
    }
}

impl S3Client {
    pub fn new(config: S3Config) -> Result<Self, ObjectStoreError> {
        let base = base_url(&config.endpoint, &config.bucket)?;
        // reqwest is built without a default crypto provider; install ring.
        // An error means another caller already installed one.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            // A redirect would carry the signature, and with it the signed
            // path, somewhere the operator did not name.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;

        let credentials = Credentials::new(
            config.access_key_id,
            config.secret_access_key.to_string(),
            None,
            None,
            "quoro-s3",
        );
        let mut settings = SigningSettings::default();
        settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256;
        settings.percent_encoding_mode = PercentEncodingMode::Single;
        settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
        settings.signature_location = SignatureLocation::Headers;

        Ok(Self {
            http,
            base,
            region: config.region,
            identity: credentials.into(),
            settings,
        })
    }

    /// Signs one request and sends it. `path` starts with `/` and is already
    /// percent-encoded; `query` values are encoded here.
    async fn execute(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, String)],
        extra: &[(&str, String)],
        body: &[u8],
    ) -> Result<Response, ObjectStoreError> {
        let mut uri = format!("{}{path}", self.base);
        if !query.is_empty() {
            let encoded: Vec<String> = query
                .iter()
                .map(|(name, value)| format!("{}={}", encode(name, false), encode(value, false)))
                .collect();
            uri.push('?');
            uri.push_str(&encoded.join("&"));
        }

        let signed = self.sign_headers(method.as_str(), &uri, extra, body, SystemTime::now())?;

        let mut request = self.http.request(method, &uri);
        for (name, value) in &signed {
            request = request.header(name, value);
        }
        for (name, value) in extra {
            request = request.header(*name, value.as_str());
        }
        let response = request
            .body(body.to_vec())
            .send()
            .await
            .map_err(|err| ObjectStoreError::Transport(err.to_string()))?;

        let status = response.status().as_u16();
        if response.status().is_success() {
            return Ok(response);
        }
        let mut response = response;
        let body = read_limited(&mut response).await?;
        Err(status_error(status, &body))
    }

    /// Path style: `/<bucket>/<key>` is already in `base`, so only the key.
    fn key_path(key: &str) -> String {
        format!("/{}", encode(key, true))
    }

    /// The headers SigV4 adds to a request (`host`, `x-amz-date`,
    /// `x-amz-content-sha256`, `authorization`). Separated from sending so the
    /// tests can pin the documented example.
    fn sign_headers(
        &self,
        method: &str,
        uri: &str,
        extra: &[(&str, String)],
        body: &[u8],
        time: SystemTime,
    ) -> Result<Vec<(String, String)>, ObjectStoreError> {
        let headers = extra.iter().map(|(name, value)| (*name, value.as_str()));
        let signable = SignableRequest::new(method, uri, headers, SignableBody::Bytes(body))
            .map_err(|err| {
                ObjectStoreError::Protocol(format!("could not build a request: {err}"))
            })?;
        let params = v4::SigningParams::builder()
            .identity(&self.identity)
            .region(&self.region)
            .name("s3")
            .time(time)
            .settings(self.settings.clone())
            .build()
            .map_err(|err| ObjectStoreError::Protocol(format!("could not sign a request: {err}")))?
            .into();
        let (instructions, _signature) = sign(signable, &params)
            .map_err(|err| ObjectStoreError::Protocol(format!("could not sign a request: {err}")))?
            .into_parts();
        Ok(instructions
            .headers()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect())
    }

    async fn put(
        &self,
        key: &str,
        body: &[u8],
        extra: Vec<(&str, String)>,
    ) -> Result<Option<String>, ObjectStoreError> {
        let response = self
            .execute(Method::PUT, &Self::key_path(key), &[], &extra, body)
            .await?;
        Ok(header(&response, "x-amz-version-id"))
    }

    async fn get_bytes(
        &self,
        key: &str,
        query: &[(&str, String)],
    ) -> Result<Vec<u8>, ObjectStoreError> {
        let mut response = self
            .execute(Method::GET, &Self::key_path(key), query, &[], &[])
            .await?;
        read_limited(&mut response).await
    }
}

impl ObjectStore for S3Client {
    async fn put_new(
        &self,
        key: &str,
        body: Vec<u8>,
        lock: Option<&ComplianceLock>,
    ) -> Result<Option<String>, ObjectStoreError> {
        // backup-format.md section 5: `If-None-Match: *`, and a 412 means the
        // object is already there.
        let mut extra = vec![
            ("if-none-match", "*".to_string()),
            (CHECKSUM_HEADER, sha256_base64(&body)),
        ];
        if let Some(lock) = lock {
            // audit-log-format.md section 8.1: compliance mode per object.
            extra.push(("x-amz-object-lock-mode", "COMPLIANCE".to_string()));
            extra.push((
                "x-amz-object-lock-retain-until-date",
                lock.retain_until.clone(),
            ));
        }
        self.put(key, &body, extra).await
    }

    async fn put_overwrite(
        &self,
        key: &str,
        body: Vec<u8>,
    ) -> Result<Option<String>, ObjectStoreError> {
        let extra = vec![(CHECKSUM_HEADER, sha256_base64(&body))];
        self.put(key, &body, extra).await
    }

    async fn get(&self, key: &str) -> Result<Vec<u8>, ObjectStoreError> {
        self.get_bytes(key, &[]).await
    }

    async fn get_version(&self, key: &str, version_id: &str) -> Result<Vec<u8>, ObjectStoreError> {
        self.get_bytes(key, &[("versionId", version_id.to_string())])
            .await
    }

    async fn head(&self, key: &str) -> Result<ObjectHead, ObjectStoreError> {
        let response = self
            .execute(Method::HEAD, &Self::key_path(key), &[], &[], &[])
            .await?;
        let size = response
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        Ok(ObjectHead {
            size,
            version_id: header(&response, "x-amz-version-id"),
            lock_mode: header(&response, "x-amz-object-lock-mode"),
            retain_until: header(&response, "x-amz-object-lock-retain-until-date"),
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>, ObjectStoreError> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![
                ("list-type", "2".to_string()),
                ("prefix", prefix.to_string()),
            ];
            if let Some(token) = &token {
                query.push(("continuation-token", token.clone()));
            }
            let mut response = self.execute(Method::GET, "", &query, &[], &[]).await?;
            let body = read_limited(&mut response).await?;
            let page = parse_list_page(&body)?;
            out.extend(page.objects);
            match page.next_token {
                Some(next) if Some(&next) == token.as_ref() => {
                    return Err(ObjectStoreError::Protocol(
                        "the list response repeated its continuation token".to_string(),
                    ));
                }
                Some(next) => token = Some(next),
                None => return Ok(out),
            }
        }
    }

    async fn list_versions(&self, prefix: &str) -> Result<Vec<ObjectVersion>, ObjectStoreError> {
        let mut out = Vec::new();
        let mut key_marker: Option<String> = None;
        let mut version_marker: Option<String> = None;
        loop {
            let mut query = vec![("versions", String::new()), ("prefix", prefix.to_string())];
            if let Some(marker) = &key_marker {
                query.push(("key-marker", marker.clone()));
            }
            if let Some(marker) = &version_marker {
                query.push(("version-id-marker", marker.clone()));
            }
            let mut response = self.execute(Method::GET, "", &query, &[], &[]).await?;
            let body = read_limited(&mut response).await?;
            let page = parse_versions_page(&body)?;
            out.extend(page.versions);
            match page.next_key_marker {
                Some(next) if Some(&next) == key_marker.as_ref() => {
                    return Err(ObjectStoreError::Protocol(
                        "the versions response repeated its key marker".to_string(),
                    ));
                }
                Some(next) => {
                    key_marker = Some(next);
                    version_marker = page.next_version_id_marker;
                }
                None => return Ok(out),
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), ObjectStoreError> {
        // Deleting what is already gone is not an error.
        match self
            .execute(Method::DELETE, &Self::key_path(key), &[], &[], &[])
            .await
        {
            Ok(_) | Err(ObjectStoreError::NotFound) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn delete_version(&self, key: &str, version_id: &str) -> Result<(), ObjectStoreError> {
        let query = [("versionId", version_id.to_string())];
        self.execute(Method::DELETE, &Self::key_path(key), &query, &[], &[])
            .await
            .map(|_| ())
    }
}

/// Validates the endpoint and builds `<origin>/<bucket>`. `http://` is only
/// for a loopback address: a plaintext credential and a signed request to
/// anywhere else is not a configuration Quoro accepts.
fn base_url(endpoint: &str, bucket: &str) -> Result<String, ObjectStoreError> {
    let url = Url::parse(endpoint)
        .map_err(|err| ObjectStoreError::Protocol(format!("invalid endpoint: {err}")))?;
    match url.scheme() {
        "https" => {}
        "http" => {
            let host = url.host_str().unwrap_or_default();
            if host != "127.0.0.1" && host != "localhost" && host != "::1" {
                return Err(ObjectStoreError::Protocol(
                    "http is only allowed for a loopback endpoint".to_string(),
                ));
            }
        }
        scheme => {
            return Err(ObjectStoreError::Protocol(format!(
                "unsupported endpoint scheme: {scheme}"
            )));
        }
    }
    if bucket.is_empty() {
        return Err(ObjectStoreError::Protocol(
            "the bucket is empty".to_string(),
        ));
    }
    Ok(format!(
        "{}/{}",
        endpoint.trim_end_matches('/'),
        encode(bucket, false)
    ))
}

fn header(response: &Response, name: &str) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn sha256_base64(body: &[u8]) -> String {
    use sha2::Digest as _;
    let mut hasher = sha2::Sha256::new();
    hasher.update(body);
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

/// Percent-encodes one path segment or query value the way SigV4 does:
/// unreserved characters stay, everything else becomes `%XX` (uppercase). A
/// path key keeps `/` (S3 concatenates the segments itself).
fn encode(value: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(char::from(byte));
            }
            b'/' if keep_slash => out.push('/'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

async fn read_limited(response: &mut Response) -> Result<Vec<u8>, ObjectStoreError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_BODY as u64)
    {
        return Err(ObjectStoreError::Protocol(format!(
            "the response body exceeds {MAX_BODY} bytes"
        )));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|err| ObjectStoreError::Transport(err.to_string()))?
    {
        if body.len() + chunk.len() > MAX_BODY {
            return Err(ObjectStoreError::Protocol(format!(
                "the response body exceeds {MAX_BODY} bytes"
            )));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Maps a non-success status onto [`ObjectStoreError`]. The error body, when
/// it is one of S3's, names the code and the message; a body that is not XML
/// (a proxy's HTML page, say) leaves them empty.
fn status_error(status: u16, body: &[u8]) -> ObjectStoreError {
    let (code, message) = parse_error_body(body);
    let message = if message.is_empty() {
        format!("HTTP {status}")
    } else {
        message
    };
    match status {
        401 => ObjectStoreError::Denied(message),
        403 if code.to_ascii_lowercase().contains("lock")
            || message.to_ascii_lowercase().contains("lock") =>
        {
            ObjectStoreError::Locked(message)
        }
        403 => ObjectStoreError::Denied(message),
        404 => ObjectStoreError::NotFound,
        409 if code.to_ascii_lowercase().contains("lock") => ObjectStoreError::Locked(message),
        412 => ObjectStoreError::AlreadyExists,
        _ => ObjectStoreError::Status {
            status,
            code,
            message,
        },
    }
}

fn parse_error_body(body: &[u8]) -> (String, String) {
    #[derive(Clone, Copy)]
    enum Field {
        Code,
        Message,
    }

    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut field = None;
    let mut code = String::new();
    let mut message = String::new();
    while let Ok(event) = reader.read_event() {
        match event {
            Event::Start(element) => match element.local_name().as_ref() {
                b"Code" => field = Some(Field::Code),
                b"Message" => field = Some(Field::Message),
                _ => {}
            },
            Event::Text(text) => {
                if let Ok(text) = text.xml_content(quick_xml::XmlVersion::Explicit1_0) {
                    match field {
                        Some(Field::Code) => code = text.into_owned(),
                        Some(Field::Message) => message = text.into_owned(),
                        None => {}
                    }
                }
            }
            Event::End(_) => field = None,
            Event::Eof => break,
            _ => {}
        }
    }
    (code, message)
}

struct ListPage {
    objects: Vec<ObjectInfo>,
    next_token: Option<String>,
}

fn parse_list_page(body: &[u8]) -> Result<ListPage, ObjectStoreError> {
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut objects = Vec::new();
    let mut current: Option<ObjectInfo> = None;
    let mut next_token = None;

    loop {
        match next_event(&mut reader)? {
            Event::Start(element) => {
                let name = element.local_name().as_ref().to_vec();
                if name == b"Contents" {
                    current = Some(ObjectInfo {
                        key: String::new(),
                        size: 0,
                        last_modified: String::new(),
                    });
                }
                stack.push(name);
            }
            Event::Text(text) => {
                let text = text
                    .xml_content(quick_xml::XmlVersion::Explicit1_0)
                    .map_err(|err| xml_error(&err.to_string()))?
                    .into_owned();
                match stack.last().map(Vec::as_slice) {
                    Some(name) if name == b"Key" => {
                        if let Some(object) = &mut current {
                            object.key = text;
                        }
                    }
                    Some(name) if name == b"Size" => {
                        if let Some(object) = &mut current {
                            object.size = parse_u64(&text)?;
                        }
                    }
                    Some(name) if name == b"LastModified" => {
                        if let Some(object) = &mut current {
                            object.last_modified = text;
                        }
                    }
                    Some(name) if name == b"NextContinuationToken" => next_token = Some(text),
                    _ => {}
                }
            }
            Event::End(element) => {
                let name = element.local_name().as_ref().to_vec();
                stack.pop();
                if name == b"Contents"
                    && let Some(object) = current.take()
                {
                    objects.push(object);
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(ListPage {
        objects,
        next_token: next_token.filter(|token| !token.is_empty()),
    })
}

struct VersionsPage {
    versions: Vec<ObjectVersion>,
    next_key_marker: Option<String>,
    next_version_id_marker: Option<String>,
}

fn parse_versions_page(body: &[u8]) -> Result<VersionsPage, ObjectStoreError> {
    let mut reader = quick_xml::Reader::from_reader(body);
    reader.config_mut().trim_text(true);
    let mut stack: Vec<Vec<u8>> = Vec::new();
    let mut versions = Vec::new();
    let mut current: Option<ObjectVersion> = None;
    let mut next_key_marker = None;
    let mut next_version_id_marker = None;

    loop {
        match next_event(&mut reader)? {
            Event::Start(element) => {
                let name = element.local_name().as_ref().to_vec();
                if name == b"Version" || name == b"DeleteMarker" {
                    current = Some(ObjectVersion {
                        key: String::new(),
                        version_id: String::new(),
                        is_latest: false,
                        is_delete_marker: name == b"DeleteMarker",
                        size: 0,
                        last_modified: String::new(),
                    });
                }
                stack.push(name);
            }
            Event::Text(text) => {
                let text = text
                    .xml_content(quick_xml::XmlVersion::Explicit1_0)
                    .map_err(|err| xml_error(&err.to_string()))?
                    .into_owned();
                match stack.last().map(Vec::as_slice) {
                    Some(name) if name == b"Key" => {
                        if let Some(version) = &mut current {
                            version.key = text;
                        }
                    }
                    Some(name) if name == b"VersionId" => {
                        if let Some(version) = &mut current {
                            version.version_id = text;
                        }
                    }
                    Some(name) if name == b"IsLatest" => {
                        if let Some(version) = &mut current {
                            version.is_latest = text == "true";
                        }
                    }
                    Some(name) if name == b"Size" => {
                        if let Some(version) = &mut current {
                            version.size = parse_u64(&text)?;
                        }
                    }
                    Some(name) if name == b"LastModified" => {
                        if let Some(version) = &mut current {
                            version.last_modified = text;
                        }
                    }
                    Some(name) if name == b"NextKeyMarker" => next_key_marker = Some(text),
                    Some(name) if name == b"NextVersionIdMarker" => {
                        next_version_id_marker = Some(text)
                    }
                    _ => {}
                }
            }
            Event::End(element) => {
                let name = element.local_name().as_ref().to_vec();
                stack.pop();
                if (name == b"Version" || name == b"DeleteMarker")
                    && let Some(version) = current.take()
                {
                    versions.push(version);
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(VersionsPage {
        versions,
        next_key_marker: next_key_marker.filter(|marker| !marker.is_empty()),
        next_version_id_marker: next_version_id_marker.filter(|marker| !marker.is_empty()),
    })
}

fn next_event<'a>(reader: &mut quick_xml::Reader<&'a [u8]>) -> Result<Event<'a>, ObjectStoreError> {
    reader
        .read_event()
        .map_err(|err| xml_error(&err.to_string()))
}

fn xml_error(reason: &str) -> ObjectStoreError {
    ObjectStoreError::Protocol(format!("could not parse the xml response: {reason}"))
}

fn parse_u64(value: &str) -> Result<u64, ObjectStoreError> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| ObjectStoreError::Protocol(format!("not a size: {value}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn example_client() -> S3Client {
        S3Client::new(S3Config {
            endpoint: "https://examplebucket.s3.amazonaws.com".to_string(),
            region: "us-east-1".to_string(),
            bucket: "examplebucket".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: Zeroizing::new(
                "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            ),
        })
        .unwrap()
    }

    fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
        headers
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn sigv4_matches_the_documented_get_object_example() {
        // The worked example of the S3 SigV4 documentation: GET /test.txt with
        // `range: bytes=0-9`, empty body, 2013-05-24T00:00:00Z.
        let time = UNIX_EPOCH + Duration::from_secs(1_369_353_600);
        let headers = example_client()
            .sign_headers(
                "GET",
                "https://examplebucket.s3.amazonaws.com/test.txt",
                &[("range", "bytes=0-9".to_string())],
                b"",
                time,
            )
            .unwrap();

        assert_eq!(
            header_value(&headers, "x-amz-date"),
            Some("20130524T000000Z")
        );
        assert_eq!(
            header_value(&headers, "x-amz-content-sha256"),
            Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
        assert_eq!(
            header_value(&headers, "authorization"),
            Some(
                "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
            )
        );
    }

    #[test]
    fn content_sha256_is_the_hash_of_the_body() {
        let client = example_client();
        let headers = client
            .sign_headers(
                "PUT",
                "https://examplebucket.s3.amazonaws.com/test.txt",
                &[],
                b"hello",
                UNIX_EPOCH,
            )
            .unwrap();
        let expected = {
            use sha2::Digest as _;
            let digest = sha2::Sha256::digest(b"hello");
            digest
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        };
        assert_eq!(
            header_value(&headers, "x-amz-content-sha256"),
            Some(expected.as_str())
        );
    }

    #[test]
    fn keys_are_percent_encoded_but_keep_slashes() {
        assert_eq!(encode("chunks/9c/9c4e", true), "chunks/9c/9c4e");
        assert_eq!(encode("chunks/9c/9c4e", false), "chunks%2F9c%2F9c4e");
        assert_eq!(encode("a b?c#d", true), "a%20b%3Fc%23d");
        assert_eq!(encode("é", true), "%C3%A9");
        assert_eq!(encode("a+b&c=d", false), "a%2Bb%26c%3Dd");
    }

    #[test]
    fn a_list_page_is_parsed_with_its_continuation_token() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>examplebucket</Name>
  <Prefix>chunks/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>2</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=</NextContinuationToken>
  <Contents>
    <Key>chunks/00/00a1</Key>
    <LastModified>2026-11-05T02:00:00.000Z</LastModified>
    <ETag>&quot;abc&quot;</ETag>
    <Size>1024</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>chunks/00/00b2</Key>
    <LastModified>2026-11-05T02:00:01.000Z</LastModified>
    <Size>0</Size>
  </Contents>
</ListBucketResult>"#;
        let page = parse_list_page(xml).unwrap();
        assert_eq!(page.objects.len(), 2);
        assert_eq!(page.objects[0].key, "chunks/00/00a1");
        assert_eq!(page.objects[0].size, 1024);
        assert_eq!(page.objects[0].last_modified, "2026-11-05T02:00:00.000Z");
        assert_eq!(page.objects[1].key, "chunks/00/00b2");
        assert_eq!(page.objects[1].size, 0);
        assert_eq!(
            page.next_token.as_deref(),
            Some("1ueGcxLPRx1Tr/XYExHnhbYLgveDs2J/wm36Hy4vbOwM=")
        );
    }

    #[test]
    fn a_versions_page_keeps_delete_markers_and_latest_flags() {
        let xml = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IsTruncated>false</IsTruncated>
  <Version>
    <Key>audit/0001.jsonl</Key>
    <VersionId>3HL4kqtJvjVBH40Nrjfkd</VersionId>
    <IsLatest>false</IsLatest>
    <LastModified>2026-11-05T02:00:00.000Z</LastModified>
    <Size>42</Size>
  </Version>
  <DeleteMarker>
    <Key>audit/0001.jsonl</Key>
    <VersionId>3HL4kqtJvjVBH40Nrjfke</VersionId>
    <IsLatest>true</IsLatest>
    <LastModified>2026-11-05T03:00:00.000Z</LastModified>
  </DeleteMarker>
</ListVersionsResult>"#;
        let page = parse_versions_page(xml).unwrap();
        assert_eq!(page.versions.len(), 2);
        assert_eq!(page.versions[0].version_id, "3HL4kqtJvjVBH40Nrjfkd");
        assert!(!page.versions[0].is_latest);
        assert!(!page.versions[0].is_delete_marker);
        assert_eq!(page.versions[0].size, 42);
        assert!(page.versions[1].is_latest);
        assert!(page.versions[1].is_delete_marker);
        assert!(page.next_key_marker.is_none());
    }

    #[test]
    fn error_bodies_name_the_code_and_the_message() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>ObjectLockedByBucketPolicy</Code><Message>Object is locked</Message><RequestId>1</RequestId></Error>"#;
        assert_eq!(
            parse_error_body(body),
            (
                "ObjectLockedByBucketPolicy".to_string(),
                "Object is locked".to_string()
            )
        );
        assert!(matches!(
            status_error(409, body),
            ObjectStoreError::Locked(_)
        ));
        assert_eq!(
            status_error(412, b"<Error><Code>PreconditionFailed</Code></Error>"),
            ObjectStoreError::AlreadyExists
        );
        assert_eq!(status_error(404, b""), ObjectStoreError::NotFound);
        assert_eq!(
            status_error(401, b""),
            ObjectStoreError::Denied("HTTP 401".to_string())
        );
        assert!(matches!(
            status_error(500, b"not xml"),
            ObjectStoreError::Status {
                status: 500,
                ref code,
                ref message,
            } if code.is_empty() && message == "HTTP 500"
        ));
    }

    #[test]
    fn http_is_only_accepted_for_loopback_endpoints() {
        assert!(base_url("http://127.0.0.1:19000", "bucket").is_ok());
        assert!(base_url("http://localhost:19000", "bucket").is_ok());
        assert!(base_url("http://s3.example.com", "bucket").is_err());
        assert!(base_url("ftp://example.com", "bucket").is_err());
        assert!(base_url("https://s3.example.com", "").is_err());
        assert_eq!(
            base_url("https://s3.example.com/", "b").unwrap(),
            "https://s3.example.com/b"
        );
    }

    #[test]
    fn debug_output_never_contains_the_secret() {
        let secret = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        let config = S3Config {
            endpoint: "https://s3.example.com".to_string(),
            region: "auto".to_string(),
            bucket: "examplebucket".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: Zeroizing::new(secret.to_string()),
        };
        let client = S3Client::new(config.clone()).unwrap();
        assert!(!format!("{config:?}").contains(secret));
        assert!(!format!("{client:?}").contains(secret));
        assert!(format!("{config:?}").contains("[redacted]"));
    }
}
