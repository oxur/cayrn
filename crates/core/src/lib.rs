//! Runtime-independent execution policy. Construction and blocking execution belong
//! on the adapter's owning thread, not the network executor.
use cayrn_protocol::{Fault, Operation, Phase, Result, Submission};

/// Transactional admission result; an existing record must never run again.
#[derive(Debug)]
pub enum Admission {
    New(Operation),
    Existing(Operation),
}
/// Required journal atomicity/durability boundary, implemented by the host.
pub trait Journal: Clone + Send + Sync + 'static {
    /// Atomically record admission and its event, or return the matching prior record.
    fn admit(&self, submission: &Submission) -> Result<Admission>;
    /// Commit dispatch intent before any execution.
    fn dispatch(&self, submission: &Submission) -> Result<()>;
    /// Commit output before clients can replay it.
    fn output(&self, submission: &Submission, text: &str) -> Result<()>;
    /// Atomically commit the outcome and operation registry update.
    fn finish(&self, submission: &Submission, phase: Phase, value: &str) -> Result<()>;
    /// Mark runtime loss/uncertain execution; future admission must be rejected.
    fn lose_runtime(&self, session: &str, reason: &str) -> Result<()>;
}
/// A foreground outcome. A user-code error need not imply runtime loss.
#[derive(Debug)]
pub enum Outcome {
    Value(String),
    Error(String),
    /// Native execution was confirmed stopped; the environment may be lost.
    Cancelled(String),
}
/// Native execution seam. A live VM need not implement Send or run on Tokio.
pub trait Runtime {
    /// Evaluate on the owning worker; output callbacks are durable capture boundaries.
    fn evaluate(
        &mut self,
        source: &str,
        emit: &mut dyn FnMut(&str) -> Result<()>,
    ) -> Result<Outcome>;
}
/// Run an already admitted operation exactly once, after durable dispatch intent.
///
/// Errors leave the outcome uncertain; the owner must quarantine/stop this worker.
pub fn execute(
    journal: &impl Journal,
    runtime: &mut impl Runtime,
    submission: &Submission,
) -> Result<()> {
    let attempt = (|| {
        journal.dispatch(submission)?;
        let outcome = runtime.evaluate(&submission.source, &mut |text| {
            journal.output(submission, text)
        })?;
        match outcome {
            Outcome::Value(value) => journal.finish(submission, Phase::Succeeded, &value),
            Outcome::Error(error) => journal.finish(submission, Phase::Failed, &error),
            Outcome::Cancelled(reason) => journal.finish(submission, Phase::Cancelled, &reason),
        }
    })();
    if let Err(error) = attempt {
        return match journal.lose_runtime(&submission.session, &error.to_string()) {
            Ok(()) => Err(error),
            Err(record_error) => Err(Fault::new(
                error.code,
                format!("{error}; could not durably record runtime loss: {record_error}"),
            )),
        };
    }
    Ok(())
}
