use clap::{Parser, Subcommand};
use std::{
    io::{self, Write},
    path::PathBuf,
    process::ExitCode,
};

#[derive(Parser)]
#[command(
    version,
    about = "Cayrn local session demonstrator (fixture backend only)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Demonstrate two independent clients sharing one fixture session.
    Demo {
        #[arg(long)]
        state_dir: PathBuf,
    },
    /// Start a standalone host with a separate fixture process per session.
    ServeFixture {
        #[arg(long)]
        state_dir: PathBuf,
        #[arg(long)]
        worker: PathBuf,
    },
    /// Send one prototype JSON request; disconnecting never closes its session.
    Request {
        #[arg(long)]
        state_dir: PathBuf,
        json: String,
    },
}
#[tokio::main]
async fn main() -> ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|e| e.kind() == io::ErrorKind::BrokenPipe)
            {
                return ExitCode::SUCCESS;
            }
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Command::Demo { state_dir } => demo(state_dir).await?,
        Command::ServeFixture { state_dir, worker } => {
            let host = cayrn_host::Host::bind(state_dir, worker).await?;
            writeln!(
                io::stdout(),
                "{}",
                serde_json::json!({"socket":host.socket_path(),"backend":"fixture-only"})
            )?;
            host.serve(async {
                if let Err(e) = tokio::signal::ctrl_c().await {
                    eprintln!("signal handler: {e}");
                }
            })
            .await?;
        }
        Command::Request { state_dir, json } => {
            let request = serde_json::from_str::<cayrn_protocol::Request>(&json)?;
            let mut client = cayrn_client::Client::connect(state_dir.join("cayrn.sock")).await?;
            let response = client.request(request).await?;
            serde_json::to_writer(io::stdout().lock(), &response)?;
            writeln!(io::stdout())?;
        }
    }
    Ok(())
}

async fn demo(directory: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    use cayrn_protocol::{Cursor, Phase, Request, Response, Submission};
    let mut terminal = cayrn_client::Client::connect(directory.join("cayrn.sock")).await?;
    let mut editor = cayrn_client::Client::connect(directory.join("cayrn.sock")).await?;
    let Response::Session(session) = terminal.request(Request::Create).await? else {
        return Err("expected session".into());
    };
    for (client, id, source) in [
        (&mut terminal, "terminal", "add 41"),
        (&mut editor, "editor", "add 1"),
    ] {
        client
            .request(Request::Submit {
                submission: Submission {
                    session: session.id.clone(),
                    generation: session.generation.clone(),
                    operation: id.into(),
                    client: id.into(),
                    source: source.into(),
                },
            })
            .await?;
        let op = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                let Response::Operation(op) = client
                    .request(Request::InspectOperation {
                        session: session.id.clone(),
                        operation: id.into(),
                    })
                    .await?
                else {
                    return Err(cayrn_protocol::Fault::new(
                        cayrn_protocol::ErrorCode::Protocol,
                        "expected operation",
                    ));
                };
                if !matches!(op.phase, Phase::Queued | Phase::Dispatched) {
                    break Ok(op);
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await??;
        writeln!(
            io::stdout(),
            "{}",
            serde_json::json!({"client":id,"session":session.id,"phase":op.phase,"value":op.value})
        )?;
    }
    let request = Request::Read {
        session: session.id,
        after: Cursor(0),
        limit: 64,
    };
    let first = terminal.request(request.clone()).await?;
    let second = editor.request(request).await?;
    if first != second {
        return Err("clients observed different transcript pages".into());
    }
    writeln!(
        io::stdout(),
        "{}",
        serde_json::json!({"both_clients_observe_same_transcript":true,"transcript":first})
    )?;
    Ok(())
}
