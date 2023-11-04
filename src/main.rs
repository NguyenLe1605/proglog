use std::net::SocketAddr;

use proglog::server;

#[tokio::main]
async fn main() {
    let router = server::create_router();
    let addr = SocketAddr::from(([127, 0, 0, 1], 8080));
    axum::Server::bind(&addr)
        .serve(router.into_make_service())
        .await
        .expect("can not start the server");
}
