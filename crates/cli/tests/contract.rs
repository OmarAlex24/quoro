//! Every operation the client calls by name exists in the frozen contract
//! (`docs/contracts/openapi.yaml`, ADR 0048) with the same method, path
//! template and `operationId`.

use quoro_cli::operations::OPERATIONS;
use serde_json::Value;
use std::path::Path;

fn openapi() -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/contracts/openapi.yaml");
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_norway::from_str(&text).unwrap_or_else(|e| panic!("cannot parse {}: {e}", path.display()))
}

#[test]
fn every_registered_operation_is_in_the_contract() {
    assert!(
        !OPERATIONS.is_empty(),
        "the registry lists the client's calls"
    );
    let paths = &openapi()["paths"];
    for operation in OPERATIONS {
        let defined = &paths[operation.path][operation.method.to_ascii_lowercase()];
        assert!(
            defined.is_object(),
            "{} {} ({}) is not in openapi.yaml",
            operation.method,
            operation.path,
            operation.id
        );
        assert_eq!(
            defined["operationId"], operation.id,
            "{} {}",
            operation.method, operation.path
        );
    }
}

#[test]
fn operations_are_registered_once() {
    for (index, operation) in OPERATIONS.iter().enumerate() {
        assert!(
            OPERATIONS[..index]
                .iter()
                .all(|other| other.id != operation.id
                    && (other.method, other.path) != (operation.method, operation.path)),
            "{} is registered twice",
            operation.id
        );
    }
}
