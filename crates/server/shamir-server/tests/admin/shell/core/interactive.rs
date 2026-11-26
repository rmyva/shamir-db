//! Live scripted interactive child shared by the shell acceptance probes
//! (round 3): commands are sent one at a time over the child's stdin and the
//! driver waits for the EXACT human result heading and a named field row
//! before issuing the next line. Lives in `core/` beside the harness so no
//! existing module's visibility changes; `secure_transports::capture` keeps
//! its pre-round-2 surface.

use std::path::PathBuf;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;

use crate::shell_tests::harness::{ShellFixture, ShellRun};
use crate::shell_tests::secure_fixture::{scripted_child_command, secret_suffix};

/// Same literals as the child entry in `secure_transports::capture`
/// (`shell_scripted_repl_child_entry`): the spawned process is the SAME test
/// binary running ONLY that entry, so both spellings must stay in sync.
const CHILD_GATE: &str = "SHAMIR_SHELL_SCRIPTED_CHILD";
const CHILD_ENTRY: &str =
    "shell_tests::secure_transports::capture::shell_scripted_repl_child_entry";

const INTERACTIVE_CAPTURE_LIMIT: usize = 1024 * 1024;
const INTERACTIVE_LINE_LIMIT: usize = 4096;
const INTERACTIVE_CHANNEL_CAPACITY: usize = 16;

struct CapturedLine {
    text: String,
    too_long: bool,
}

/// A live scripted interactive child. The stderr pipe is continuously drained
/// into a bounded channel; finish drains concurrently with child exit, then
/// awaits both readers. Capture limits cap retained memory while excess bytes
/// are still drained to avoid pipe backpressure deadlocks.
pub struct InteractiveChild {
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    output_lines: tokio::sync::mpsc::Receiver<CapturedLine>,
    /// Sequence number of the trailing no-op sent after each command.
    noop_seq: u64,
    stdout_task: Option<tokio::task::JoinHandle<(Vec<u8>, bool)>>,
    stderr_task: Option<tokio::task::JoinHandle<std::io::Result<()>>>,
    stderr: String,
    overflow: bool,
    secret: Zeroizing<String>,
    fixture_home: PathBuf,
}

impl InteractiveChild {
    /// cancel-safe: NO — the child waits only terminate after stdin closes;
    /// command awaits use 180-second bounds.
    pub async fn spawn(
        mut command: tokio::process::Command,
        secret: Zeroizing<String>,
        fixture_home: PathBuf,
    ) -> Self {
        let mut child = command.spawn().expect("spawn interactive scripted child");
        let mut stdin = child.stdin.take().expect("interactive child stdin");
        let stdout = child.stdout.take().expect("interactive child stdout");
        let stderr = child.stderr.take().expect("interactive child stderr");
        let mut secret_line = Zeroizing::new(Vec::with_capacity(secret.len() + 1));
        secret_line.extend_from_slice(secret.as_bytes());
        secret_line.push(b'\n');
        stdin
            .write_all(secret_line.as_slice())
            .await
            .expect("write interactive child secret");
        stdin.flush().await.expect("flush interactive child secret");

        let (line_tx, output_lines) = tokio::sync::mpsc::channel(INTERACTIVE_CHANNEL_CAPACITY);
        let stderr_task = tokio::spawn(async move {
            let mut reader = stderr;
            let mut bytes = [0u8; 4096];
            let mut line = Vec::with_capacity(128);
            let mut too_long = false;
            loop {
                let count = reader.read(&mut bytes).await?;
                if count == 0 {
                    break;
                }
                for byte in &bytes[..count] {
                    if *byte == b'\n' {
                        let text = String::from_utf8_lossy(&line)
                            .trim_end_matches('\r')
                            .to_owned();
                        if line_tx.send(CapturedLine { text, too_long }).await.is_err() {
                            return Ok(());
                        }
                        line.clear();
                        too_long = false;
                    } else if !too_long {
                        if line.len() == INTERACTIVE_LINE_LIMIT {
                            line.clear();
                            too_long = true;
                        } else {
                            line.push(*byte);
                        }
                    }
                }
            }
            if !line.is_empty() || too_long {
                let text = String::from_utf8_lossy(&line).into_owned();
                let _ = line_tx.send(CapturedLine { text, too_long }).await;
            }
            Ok(())
        });
        let stdout_task = tokio::spawn(async move {
            let mut reader = stdout;
            let mut bytes = Vec::with_capacity(4096);
            let mut chunk = [0u8; 4096];
            let mut overflow = false;
            loop {
                let count = reader
                    .read(&mut chunk)
                    .await
                    .expect("read interactive child stdout");
                if count == 0 {
                    break;
                }
                let remaining = INTERACTIVE_CAPTURE_LIMIT.saturating_sub(bytes.len());
                let kept = count.min(remaining);
                bytes.extend_from_slice(&chunk[..kept]);
                overflow |= kept != count;
            }
            (bytes, overflow)
        });
        Self {
            child,
            stdin,
            output_lines,
            noop_seq: 0,
            stdout_task: Some(stdout_task),
            stderr_task: Some(stderr_task),
            stderr: String::new(),
            overflow: false,
            secret,
            fixture_home,
        }
    }

    /// In-process child entry ready-made for the REPL subprocess harness.
    pub fn child_command(fixture: &ShellFixture) -> tokio::process::Command {
        scripted_child_command(fixture, CHILD_GATE, CHILD_ENTRY)
    }

    /// Send one command, require its exact result/error heading, and collect
    /// rows through the specified terminal field. Error headings are `error`;
    /// their code and message rows must be checked by the caller.
    ///
    /// FRAMING: the human renderer prints the heading followed by ALL field
    /// rows with NO end-of-response marker (`shell/output.rs`
    /// write_human_result has no trailing blank line, and `-q` suppresses the
    /// prompt), so "until the requested field" alone cannot prove the frame
    /// ended. After the requested field this driver therefore sends an
    /// unparsable no-op (`zzz-noop-<seq>`): the interactive loop answers it
    /// with the usage ERROR frame, whose heading is exactly `error` (the
    /// error renderer ignores the command label) followed by the fixed `code`
    /// and `message` rows. A field row is always `name  value` (two spaces),
    /// so a line equal to `error` can only be a heading. Every line read
    /// before that heading belongs to the REAL response and is appended to
    /// it; reading through the no-op's `message` row leaves nothing unread,
    /// so the next command's heading read sees the new command's heading.
    /// The whole exchange has ONE 180-second deadline. cancel-safe: NO — a
    /// command is sent before waiting; dropping this future leaves its
    /// result unknown.
    pub async fn command_until(
        &mut self,
        command: &str,
        heading: &str,
        terminal_field: &str,
    ) -> String {
        self.noop_seq += 1;
        let noop = format!("zzz-noop-{}", self.noop_seq);
        self.send_line(command).await;
        tokio::time::timeout(Duration::from_secs(180), async {
            let mut response = String::new();
            let mut first = self.next_output_line().await;
            // The secret prompt (`password: `, then a newline once the secret
            // line is read) precedes the first result of every child.
            while first.trim() == "password:" {
                first = self.next_output_line().await;
            }
            assert_eq!(first, heading, "unexpected interactive result heading");
            append_bounded(&mut response, &first);
            loop {
                let line = self.next_output_line().await;
                append_bounded(&mut response, &line);
                if human_field(&line).is_some_and(|(field, _)| field == terminal_field) {
                    break;
                }
            }
            self.send_line(&noop).await;
            loop {
                let line = self.next_output_line().await;
                if line == "error" {
                    let code = self.next_output_line().await;
                    assert!(
                        human_field(&code).is_some_and(|(field, _)| field == "code"),
                        "no-op error frame must carry the code row"
                    );
                    let message = self.next_output_line().await;
                    assert!(
                        human_field(&message).is_some_and(|(field, _)| field == "message"),
                        "no-op error frame must carry the message row"
                    );
                    return response;
                }
                append_bounded(&mut response, &line);
            }
        })
        .await
        .expect("interactive child command output deadline")
    }

    async fn send_line(&mut self, line: &str) {
        self.stdin
            .write_all(line.as_bytes())
            .await
            .expect("write interactive line");
        self.stdin
            .write_all(b"\n")
            .await
            .expect("terminate interactive line");
        self.stdin.flush().await.expect("flush interactive line");
    }

    async fn next_output_line(&mut self) -> String {
        let line = self
            .output_lines
            .recv()
            .await
            .expect("interactive child stderr closed before result field");
        assert!(
            !line.too_long,
            "interactive output line exceeds capture bound"
        );
        let additional = line.text.len().saturating_add(1);
        if self.stderr.len().saturating_add(additional) <= INTERACTIVE_CAPTURE_LIMIT {
            self.stderr.push_str(&line.text);
            self.stderr.push('\n');
        } else {
            self.overflow = true;
        }
        line.text
    }

    /// cancel-safe: NO — sending `quit` is terminal; child wait has a fixed
    /// 180-second bound.
    pub async fn finish(mut self) -> ShellRun {
        self.stdin
            .write_all(b"quit\n")
            .await
            .expect("write interactive quit");
        self.stdin
            .shutdown()
            .await
            .expect("close interactive stdin");
        let status = tokio::time::timeout(Duration::from_secs(180), async {
            let mut status = None;
            while status.is_none() {
                tokio::select! {
                    exited = self.child.wait() => {
                        status = Some(exited.expect("wait for interactive child"));
                    }
                    line = self.output_lines.recv() => {
                        if let Some(line) = line {
                            self.collect_line(line);
                        }
                    }
                }
            }
            status.expect("interactive child status")
        })
        .await
        .expect("interactive child exit deadline");
        while let Some(line) = self.output_lines.recv().await {
            self.collect_line(line);
        }
        let stderr_result = self
            .stderr_task
            .take()
            .expect("retained interactive stderr task")
            .await
            .expect("interactive stderr task joins");
        stderr_result.expect("read interactive child stderr");
        let (stdout, stdout_overflow) = self
            .stdout_task
            .take()
            .expect("retained interactive stdout task")
            .await
            .expect("interactive stdout task joins");
        assert!(!self.overflow, "interactive stderr capture limit exceeded");
        assert!(
            !stdout_overflow,
            "interactive stdout capture limit exceeded"
        );
        let run = ShellRun {
            exit_code: status.code().unwrap_or(-1),
            stdout,
            stderr: std::mem::take(&mut self.stderr).into_bytes(),
        };
        assert_clean(&run, self.secret.as_str());
        assert!(
            !run.contains_canary(secret_suffix(self.secret.as_str())),
            "secret suffix leaked onto interactive child stream"
        );
        scan_operator_home_path(
            &self.fixture_home,
            &[self.secret.as_str(), secret_suffix(self.secret.as_str())],
        );
        assert_eq!(run.exit_code, 0, "interactive child exited nonzero");
        run
    }

    fn collect_line(&mut self, line: CapturedLine) {
        if line.too_long {
            self.overflow = true;
        }
        let additional = line.text.len().saturating_add(1);
        if self.stderr.len().saturating_add(additional) <= INTERACTIVE_CAPTURE_LIMIT {
            self.stderr.push_str(&line.text);
            self.stderr.push('\n');
        } else {
            self.overflow = true;
        }
    }
}

impl Drop for InteractiveChild {
    fn drop(&mut self) {
        // On assertion/timeout unwinding, do not silently detach pipe readers.
        // The child is configured kill-on-drop; abort and explicitly discard
        // each still-owned reader task handle.
        if let Some(task) = self.stdout_task.take() {
            task.abort();
        }
        if let Some(task) = self.stderr_task.take() {
            task.abort();
        }
    }
}

fn append_bounded(response: &mut String, line: &str) {
    if response.len().saturating_add(line.len()).saturating_add(1) > INTERACTIVE_CAPTURE_LIMIT {
        panic!("interactive command response exceeds capture limit");
    }
    response.push_str(line);
    response.push('\n');
}

/// One `field  value` row of the human table. The renderer pads the KEY
/// column to a fixed width (`shell/output.rs` write_human_result /
/// write_human_error), so BOTH sides are trimmed: the label carries trailing
/// spaces and the value may carry leading spaces (error rows pad `code` /
/// `message` to width nine).
fn human_field(line: &str) -> Option<(&str, &str)> {
    let (label, value) = line.split_once("  ")?;
    Some((label.trim(), value.trim()))
}

pub fn human_field_value<'a>(response: &'a str, field: &str) -> &'a str {
    response
        .lines()
        .skip(1)
        .filter_map(human_field)
        .find_map(|(name, value)| (name == field).then_some(value))
        .unwrap_or_else(|| panic!("interactive result omitted field {field}"))
}

pub fn assert_human_field(response: &str, field: &str, value: &str) {
    assert_eq!(
        human_field_value(response, field),
        value,
        "unexpected human field {field}"
    );
}

/// The child is a libtest SUBPROCESS: its stdout legitimately carries libtest
/// framing, so nothing here asserts stdout emptiness — the callers scan every
/// captured byte for secrets, paths and separators instead.
fn assert_clean(run: &ShellRun, secret: &str) {
    assert!(
        !run.contains_canary(secret),
        "secret leaked onto a child stream"
    );
    for stream in [&run.stdout, &run.stderr] {
        assert!(
            stream
                .iter()
                .all(|&byte| !matches!(byte, b'\t' | 0x07 | 0x1b)),
            "unexpected raw TAB, BEL or ESC on a child stream"
        );
    }
}

fn scan_operator_home_path(root: &std::path::Path, needles: &[&str]) {
    let mut dirs = vec![root.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).expect("scan interactive operator home") {
            let entry = entry.expect("interactive operator-home entry");
            let kind = entry
                .file_type()
                .expect("interactive operator-home entry type");
            assert!(
                !entry
                    .file_name()
                    .to_string_lossy()
                    .to_ascii_lowercase()
                    .contains("history"),
                "unexpected interactive shell history"
            );
            if kind.is_dir() {
                dirs.push(entry.path());
            } else {
                assert!(
                    kind.is_file(),
                    "unexpected non-file in interactive operator home"
                );
                let bytes = std::fs::read(entry.path()).expect("read interactive operator file");
                for needle in needles {
                    assert!(
                        !bytes
                            .windows(needle.len())
                            .any(|window| window == needle.as_bytes()),
                        "secret leaked in interactive operator home"
                    );
                }
            }
        }
    }
}
