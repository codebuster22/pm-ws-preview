//! The pm-ws upstream daemon: one process running the native-event ingestion rail.
//!
//! Arguments are handed to [`pm_ws::upstream::run_cli`] unchanged. A leading `upstream`
//! token is accepted and stripped so the invocation the bench harnesses use — `pmwsd
//! upstream <args>` — keeps working alongside the plain `pmwsd <args>` form.
//!
//! Exits 0 when the rail completes its run and 2 with the failure on stderr otherwise,
//! which is the exit contract [`pm_ws::upstream::run_cli`] answers with.

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut arguments = std::env::args().skip(1).peekable();
    if arguments.peek().map(String::as_str) == Some("upstream") {
        let _upstream = arguments.next();
    }
    if let Err(error) = pm_ws::upstream::run_cli(arguments).await {
        eprintln!("pmwsd upstream: {error}");
        std::process::exit(2);
    }
}
