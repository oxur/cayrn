//! Small sequential RPC client. Separate clients attach to the same durable session.
use cayrn_protocol::{Call, ErrorCode, Fault, Reply, Request, Response, Result, VERSION, wire};
use std::path::Path;
use tokio::net::UnixStream;
use uuid::Uuid;

/// Connection ownership does not imply session/runtime ownership.
#[derive(Debug)]
pub struct Client {
    stream: UnixStream,
}
impl Client {
    /// Connect to a local host socket.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            stream: UnixStream::connect(path).await?,
        })
    }
    /// Execute one RPC. On ambiguous I/O, inspect/resubmit the same operation ID.
    /// This method never automatically retries evaluated code.
    pub async fn request(&mut self, request: Request) -> Result<Response> {
        if let Request::Submit { submission } = &request {
            submission.validate()?;
        }
        let call = Call {
            version: VERSION,
            id: Uuid::new_v4().to_string(),
            request,
        };
        wire::write(&mut self.stream, &call).await?;
        let reply: Reply = wire::read(&mut self.stream).await?;
        if reply.version != VERSION || reply.id != call.id {
            return Err(Fault::new(
                ErrorCode::Protocol,
                "response correlation/version mismatch",
            ));
        }
        match reply.response {
            Response::Error(error) => Err(error),
            response => Ok(response),
        }
    }
}
