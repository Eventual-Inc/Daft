use std::{path::Path, pin::pin};

use tokio::{net::TcpListener, sync::mpsc};

use crate::DashboardState;

pub async fn launch() {
    let listener = TcpListener::bind((super::SERVER_ADDR, super::SERVER_PORT))
        .await
        .unwrap_or_else(|error| panic!(
            "Failed to bind to `{}:{}`, another process is already bound to it; consider running `kill -9 $(lsof -t -i :3238)` in order to kill it; {error}",
            super::SERVER_ADDR,
            super::SERVER_PORT,
        ));

    let (send, mut recv) = mpsc::channel::<()>(1);
    let mut api_signal = pin!(async { recv.recv().await.unwrap() });

    let state = DashboardState::new(send);

    loop {
        tokio::select! {
            (stream, _) = async {
                listener
                    .accept()
                    .await
                    .unwrap_or_else(|error| panic!("Unable to accept incoming connection: {error}"))
            } => super::handle_stream(stream, state.clone()),
            () = &mut api_signal => break,
        }
    }
}
