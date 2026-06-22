use std::io::{Read, Write};

use tiny_http::{HTTPVersion, Header, Request, Response, StatusCode};

const STREAMING_CHUNK_READ_BUF_BYTES: usize = 8 * 1024;

fn should_skip_streaming_manual_header(header: &Header) -> bool {
    header.field.equiv("connection")
        || header.field.equiv("content-length")
        || header.field.equiv("trailer")
        || header.field.equiv("transfer-encoding")
        || header.field.equiv("upgrade")
}

fn header_name_exists(headers: &[Header], name: &'static str) -> bool {
    headers.iter().any(|header| header.field.equiv(name))
}

pub(super) fn write_streaming_chunked_response<W, R>(
    writer: &mut W,
    http_version: &HTTPVersion,
    status: StatusCode,
    headers: &[Header],
    mut body: R,
    do_not_send_body: bool,
) -> std::io::Result<()>
where
    W: Write,
    R: Read,
{
    write!(
        writer,
        "HTTP/{}.{} {} {}\r\n",
        http_version.0,
        http_version.1,
        status.0,
        status.default_reason_phrase()
    )?;
    for header in headers {
        if should_skip_streaming_manual_header(header) {
            continue;
        }
        writer.write_all(header.field.as_str().as_str().as_bytes())?;
        writer.write_all(b": ")?;
        writer.write_all(header.value.as_str().as_bytes())?;
        writer.write_all(b"\r\n")?;
    }
    if !header_name_exists(headers, "x-accel-buffering") {
        writer.write_all(b"X-Accel-Buffering: no\r\n")?;
    }
    writer.write_all(b"Transfer-Encoding: chunked\r\n\r\n")?;
    writer.flush()?;

    if !do_not_send_body {
        let mut buffer = vec![0_u8; STREAMING_CHUNK_READ_BUF_BYTES];
        loop {
            let read = body.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            write!(writer, "{read:x}\r\n")?;
            writer.write_all(&buffer[..read])?;
            writer.write_all(b"\r\n")?;
            writer.flush()?;
        }
    }

    writer.write_all(b"0\r\n\r\n")?;
    writer.flush()
}

pub(super) fn respond_streaming_chunked<R>(
    request: Request,
    status: StatusCode,
    headers: Vec<Header>,
    body: R,
) -> std::io::Result<()>
where
    R: Read + Send + 'static,
{
    if *request.http_version() <= (1, 0) {
        return request.respond(Response::new(status, headers, body, None, None));
    }

    let http_version = request.http_version().clone();
    let do_not_send_body = request.method().as_str().eq_ignore_ascii_case("HEAD");
    let mut writer = request.into_writer();
    write_streaming_chunked_response(
        &mut writer,
        &http_version,
        status,
        &headers,
        body,
        do_not_send_body,
    )
}
