// Recompile when the injected packaging version (see main.rs) changes —
// option_env! alone doesn't make cargo track the env var, so without this a
// stale binary could keep an old version string.
fn main() {
    println!("cargo:rerun-if-env-changed=SNINJA_VERSION");
}
