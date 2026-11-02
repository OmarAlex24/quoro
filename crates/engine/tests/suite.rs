// SPDX-License-Identifier: AGPL-3.0-only
//! Runs the engine conformance suite against the in-memory fake.

mod common;

use quoro_engine::FakeEngine;

#[tokio::test]
async fn fake_engine_passes_the_lifecycle_suite() {
    let engine = FakeEngine::new();
    common::lifecycle_suite(&engine, common::image_ref()).await;
}
