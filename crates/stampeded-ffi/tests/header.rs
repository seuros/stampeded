//! `include/stampeded.h` and its `go/` copy must match what cbindgen generates.
//! STAMPEDED_BLESS=1 rewrites them.

#[test]
fn the_committed_header_matches_the_exports() {
    let crate_dir = env!("CARGO_MANIFEST_DIR");
    let config =
        cbindgen::Config::from_file(format!("{crate_dir}/cbindgen.toml")).expect("cbindgen.toml");
    let mut generated = Vec::new();
    cbindgen::Builder::new()
        .with_crate(crate_dir)
        .with_config(config)
        .generate()
        .expect("cbindgen")
        .write(&mut generated);

    // The Go module carries a copy: a module fetched from the proxy holds only its own directory.
    for path in [
        format!("{crate_dir}/include/stampeded.h"),
        format!("{crate_dir}/../../go/stampeded.h"),
    ] {
        if std::env::var_os("STAMPEDED_BLESS").is_some() {
            std::fs::write(&path, &generated).expect("write header");
            continue;
        }
        let committed = std::fs::read(&path).unwrap_or_default();
        assert!(
            committed == generated,
            "{path} is out of date: STAMPEDED_BLESS=1 cargo test -p stampeded-ffi --test header"
        );
    }
}
