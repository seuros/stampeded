// `sqlx::migrate!` embeds schema/*.sql: rebuild when a migration is added.
fn main() {
    println!("cargo:rerun-if-changed=schema");
}
