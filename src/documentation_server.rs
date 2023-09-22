use std::{borrow::Cow, net::SocketAddr};

use anyhow::{anyhow, Context, Result};

use include_dir::{include_dir, Dir};
use log::info;
use tiny_http::{Header, Response, ResponseBox};
use url::Url;
static DOC_DIR: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/docs/book/");

pub fn start_server(port: u16, open_browser: bool) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let server = tiny_http::Server::http(addr)
        .map_err(|e| anyhow!("{e}"))
        .context("Could not start documentation server.")?;

    let base_url = Url::parse(&format!("http://{addr}"))?;

    if open_browser {
        info!("Opening {base_url} in your browser. Press CTRL-C to stop the server.");
        open::that(base_url.to_string())?;
    }
    for request in server.incoming_requests() {
        if let Ok(request_url) = base_url.join(request.url()) {
            let path = get_path_with_index(request_url.path());
            let response = get_response_from_file(path.as_ref())?;
            request.respond(response)?
        }
    }

    Ok(())
}

/// For paths that end in "/", append the default index.html
fn get_path_with_index(path: &str) -> Cow<str> {
    if path.ends_with('/') {
        format!("{path}index.html").into()
    } else if path.is_empty() {
        "/index.html".into()
    } else {
        path.into()
    }
}

fn get_response_from_file(path: &str) -> Result<ResponseBox> {
    let mime_type = mime_guess::from_path(path).first_or_text_plain();

    let response = match DOC_DIR.get_file(path.trim_start_matches('/')) {
        None => Response::from_string(format!("{path} not found"))
            .with_status_code(404)
            .boxed(),
        Some(file) => {
            let mut response = Response::from_data(file.contents()).with_status_code(200);
            let content_header =
                Header::from_bytes(&b"Content-Type"[..], mime_type.to_string().as_bytes())
                    .map_err(|_| anyhow!("Content Type header was not created"))?;
            response.add_header(content_header);
            response.boxed()
        }
    };
    Ok(response)
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn compile_instructions_found() {
        let port = portpicker::pick_unused_port().unwrap_or(3000);
        std::thread::spawn(move || start_server(port, false).unwrap());

        let response = ureq::get(&format!("http://localhost:{port}/compile.txt"))
            .call()
            .unwrap();
        assert_eq!(response.status(), 200);
    }

    #[test]
    fn missing_static_resource() {
        let port = portpicker::pick_unused_port().unwrap_or(3000);
        std::thread::spawn(move || start_server(port, false).unwrap());

        let response = ureq::get(&format!(
            "http://localhost:{port}/THIS_FILE_DOES_NOT_EXIST.html"
        ))
        .call();
        let response = response
            .expect_err("Call should return 404 error")
            .into_response()
            .unwrap();

        assert_eq!(response.status(), 404);
    }

    #[test]
    fn test_get_path_with_index() {
        assert_eq!(get_path_with_index(""), "/index.html");
        assert_eq!(get_path_with_index("/"), "/index.html");
        assert_eq!(get_path_with_index("/mydir/"), "/mydir/index.html");
        assert_eq!(get_path_with_index("/mydir/file.txt"), "/mydir/file.txt");
    }
}
