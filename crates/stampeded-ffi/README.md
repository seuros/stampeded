# stampeded-ffi

The C ABI for [stampeded](https://crates.io/crates/stampeded): connect,
enqueue, and pull jobs from any language with a C FFI. The header is
`include/stampeded.h`; the Ruby and Go bindings in the
[repository](https://github.com/seuros/stampeded) load this library.

A binding that builds its own native library depends on this crate from a
`cdylib` and re-exports it:

```toml
[lib]
name = "mybinding_native"   # not stampeded_ffi: that file name is this crate's
crate-type = ["cdylib"]

[dependencies]
ffi = { package = "stampeded-ffi", version = "0.4" }
```

```rust
pub use ffi::*;
```

TLS to Postgres is `tls-aws-lc` (default) or `tls-ring`; enable one.

Licensed under BSD-3-Clause.
