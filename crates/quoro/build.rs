//! rust-embed reads `panel/dist` when the crate compiles, but cargo does not
//! know about that directory. Watching it rebuilds the binary after a new
//! panel build (Vite gives every build new file names).

fn main() {
    println!("cargo::rerun-if-changed=../../panel/dist");
}
