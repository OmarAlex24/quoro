//! The web panel (D-0056), embedded in the binary and served by the same
//! listener as the API (D-0054).
//!
//! `panel/dist` is compiled in by `rust-embed` in release builds. A build
//! without it (a Rust-only checkout, CI's Rust job) still compiles: the panel
//! then answers with a short page that says how to build it. Every path that
//! is not a file and not under `/api/` gets `index.html`, so the panel's
//! client-side routes survive a reload.

use axum::body::Body;
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use quoro_control_plane::error::ApiError;
use rust_embed::Embed;

#[derive(Embed)]
// Relative to this crate's manifest directory.
#[folder = "../../panel/dist"]
#[allow_missing = true]
struct Assets;

/// Same-origin only. `'unsafe-inline'` styles cover the `<style>` element the
/// dialog's scroll lock injects; scripts stay `'self'`.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
     img-src 'self' data:; font-src 'self'; connect-src 'self'; object-src 'none'; \
     base-uri 'none'; form-action 'self'; frame-ancestors 'none'";

const MISSING: &str = "<!doctype html><title>Quoro</title>\
     <p>This binary was built without the panel. Run <code>npm ci &amp;&amp; npm run build</code> \
     in <code>panel/</code>, then rebuild. The API is served under <code>/api/v1/</code>.</p>";

/// Fallback handler of the router: static files, then `index.html`.
pub async fn serve(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if path == "/api" || path.starts_with("/api/") {
        return ApiError::not_found("route").into_response();
    }
    if method != Method::GET && method != Method::HEAD {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }

    let relative = path.trim_start_matches('/');
    let (name, file) = match Assets::get(relative).filter(|_| !relative.is_empty()) {
        Some(file) => (relative, file),
        None => match Assets::get("index.html") {
            Some(file) => ("index.html", file),
            None => {
                return with_headers(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "text/html; charset=utf-8",
                    false,
                    Body::from(MISSING),
                );
            }
        },
    };
    // Vite fingerprints everything under `assets/`, so it can be cached for
    // good; `index.html` must be revalidated so a new build is picked up.
    let immutable = name.starts_with("assets/");
    with_headers(
        StatusCode::OK,
        content_type(name),
        immutable,
        Body::from(file.data.into_owned()),
    )
}

fn with_headers(status: StatusCode, kind: &'static str, immutable: bool, body: Body) -> Response {
    let mut response = (status, body).into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(kind));
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if immutable {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        }),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(CSP),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    headers.insert(header::X_FRAME_OPTIONS, HeaderValue::from_static("DENY"));
    response
}

/// The handful of types a Vite build emits; anything else is opaque bytes.
fn content_type(name: &str) -> &'static str {
    match name.rsplit_once('.').map(|(_, ext)| ext) {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "map") => "application/json",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_cover_the_vite_output() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type("assets/index-abc.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(content_type("assets/archivo.woff2"), "font/woff2");
        assert_eq!(content_type("LICENSE"), "application/octet-stream");
    }

    #[tokio::test]
    async fn api_paths_never_fall_back_to_the_panel() {
        let response = serve(Method::GET, Uri::from_static("/api/v1/nope")).await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            response.headers()[header::CONTENT_TYPE],
            "application/problem+json"
        );
    }

    #[tokio::test]
    async fn client_routes_get_the_shell_with_security_headers() {
        let response = serve(Method::GET, Uri::from_static("/vms")).await;
        // With or without a built panel, a client route is HTML with the CSP.
        assert!(
            response.headers()[header::CONTENT_TYPE]
                .to_str()
                .unwrap()
                .starts_with("text/html")
        );
        assert!(
            response
                .headers()
                .contains_key(header::CONTENT_SECURITY_POLICY)
        );
        assert_eq!(response.headers()[header::X_FRAME_OPTIONS], "DENY");
    }
}
