//! Postgres over TLS. The session test needs a server with a certificate and
//! skips without `STAMPEDED_TEST_TLS_DATABASE_URL`, CI included:
//!
//!   STAMPEDED_TEST_TLS_DATABASE_URL='postgres://u:p@localhost/db?sslmode=verify-full&sslrootcert=/path/to/ca.crt' \
//!     cargo test -p stampeded --test tls

use sqlx::{Connection, PgConnection};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Length 8, then the SSLRequest code 80877103.
const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 0x04, 0xd2, 0x16, 0x2f];

#[tokio::test]
async fn sslmode_require_starts_a_tls_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();

    // MOCKED: not Postgres; answers 'S', then reads a TLS record header and message type.
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await?;
        let mut request = [0u8; 8];
        socket.read_exact(&mut request).await?;
        socket.write_all(b"S").await?;
        let mut record = [0u8; 6];
        socket.read_exact(&mut record).await?;
        Ok::<_, std::io::Error>((request, record))
    });

    let url = format!("postgres://stampeded@127.0.0.1:{port}/stampeded?sslmode=require");
    let client = tokio::time::timeout(Duration::from_secs(10), PgConnection::connect(&url))
        .await
        .expect("the server hangs up after the first record");
    let error = client.expect_err("the server is not Postgres");

    let (request, record) = server
        .await
        .expect("server task")
        .unwrap_or_else(|e| panic!("no TLS handshake ({e}); the client said: {error}"));
    assert_eq!(request, SSL_REQUEST);
    // A handshake record (0x16) whose first message is a ClientHello (0x01).
    assert_eq!(
        (record[0], record[5]),
        (0x16, 0x01),
        "the client said: {error}"
    );
}

#[tokio::test]
async fn the_tls_database_url_gets_an_encrypted_session() {
    let Ok(url) = std::env::var("STAMPEDED_TEST_TLS_DATABASE_URL") else {
        eprintln!("skipping: STAMPEDED_TEST_TLS_DATABASE_URL not set");
        return;
    };

    let mut conn = PgConnection::connect(&url).await.expect("connect");
    let ssl: bool = sqlx::query_scalar("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        .fetch_one(&mut conn)
        .await
        .expect("pg_stat_ssl");

    assert!(ssl, "connected, but in plaintext");
}
