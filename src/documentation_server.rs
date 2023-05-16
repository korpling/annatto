use std::net::SocketAddr;

use anyhow::Ok;
use axum::{
    body::{self, Empty, Full},
    extract::Path,
    http::{header, HeaderValue, Response, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};
use include_dir::{include_dir, Dir};
use log::{error, info};

static DOC_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/docs/book/");

pub fn start_server() -> anyhow::Result<()> {
    let addr = SocketAddr::from((
        [127, 0, 0, 1],
        portpicker::pick_unused_port().unwrap_or(3000),
    ));

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let router = app()?;
            let server = axum::Server::bind(&addr).serve(router.into_make_service());

            info!("Opening http://{addr} in your browser.");
            open::that(format!("http://{addr}"))?;

            if let Err(e) = server.await {
                error!("{}", e);
            }
            Ok(())
        })?;
    Ok(())
}

fn app() -> anyhow::Result<Router> {
    let result = Router::new()
        .route("/", get(index))
        .route("/*path", get(static_file));
    Ok(result)
}

async fn index() -> impl IntoResponse {
    let response = match DOC_DIR.get_file("index.html") {
        None => {
            error!("index.html not found");
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(body::boxed(Empty::new()))
                .unwrap()
        }
        Some(file) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_str("text/html").unwrap(),
            )
            .body(body::boxed(Full::from(file.contents())))
            .unwrap(),
    };
    response
}

async fn static_file(Path(path): Path<String>) -> impl IntoResponse {
    let path = path.trim_start_matches('/');
    let mime_type = mime_guess::from_path(path).first_or_text_plain();

    let response = match DOC_DIR.get_file(path) {
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(body::boxed(Empty::new()))
            .unwrap(),
        Some(file) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                HeaderValue::from_str(mime_type.as_ref()).unwrap(),
            )
            .body(body::boxed(Full::from(file.contents())))
            .unwrap(),
    };
    response
}
