//! The interactive read-eval-print loop.
//!
//! A statement spans as many input lines as needed and is submitted when a line
//! leaves the accumulated buffer ending in `;`. Two meta-commands, recognized
//! only at the start of a statement, control the session: `\q` quits and
//! `\timing` toggles a per-statement elapsed-time line. `EXPLAIN` needs no
//! special handling - it flows through `Runtime::execute` like any statement and
//! renders as its single `plan` column. Ctrl-C abandons the statement in
//! progress and returns to a fresh prompt; Ctrl-D (EOF) on an empty prompt ends
//! the session. A statement that fails prints its error to stderr and the loop
//! continues, so one bad query never ends the session.

use std::path::PathBuf;
use std::time::Instant;

use fq_runtime::Runtime;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::render::{render, row_count, OutputFormat};

/// Run the REPL against `runtime`, rendering every result in `format`, until the
/// user quits (`\q`) or sends EOF. Returns an error only when the line editor
/// itself fails; a failing statement is reported and the loop keeps going.
pub fn run(runtime: &Runtime, format: OutputFormat) -> Result<(), ReadlineError> {
    let mut editor = DefaultEditor::new()?;
    let history = history_path();
    if let Some(path) = &history {
        // A first run has no history file; anything else (unreadable, corrupt)
        // is reported rather than swallowed, because a history that silently
        // fails to load looks identical to one that was never written.
        match editor.load_history(path) {
            Ok(()) => {}
            Err(ReadlineError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => eprintln!("could not read history {}: {error}", path.display()),
        }
    }
    println!("fedq interactive shell. End statements with ';'. Type \\q to quit.");
    let mut buffer = String::new();
    let mut timing = false;
    loop {
        let prompt = if buffer.is_empty() { "fedq> " } else { "...> " };
        match editor.readline(prompt) {
            Ok(line) => {
                if handle_line(
                    runtime,
                    format,
                    &line,
                    &mut buffer,
                    &mut timing,
                    &mut editor,
                ) {
                    break;
                }
            }
            // Ctrl-C abandons the partial statement and returns to a clean prompt.
            Err(ReadlineError::Interrupted) => buffer.clear(),
            // Ctrl-D ends the session.
            Err(ReadlineError::Eof) => break,
            Err(error) => return Err(error),
        }
    }
    save_history(&mut editor, history.as_deref());
    Ok(())
}

/// The history file: `$FEDQ_HISTORY` when set, else `~/.fedq_history`. `None`
/// when neither is available, in which case the session keeps history in memory
/// only, exactly as before.
fn history_path() -> Option<PathBuf> {
    if let Some(explicit) = std::env::var_os("FEDQ_HISTORY") {
        return Some(PathBuf::from(explicit));
    }
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".fedq_history"))
}

/// Persist the session's history. A write failure is REPORTED, not swallowed:
/// losing a session's history silently is how someone discovers on the next run
/// that it was never being saved.
fn save_history(editor: &mut DefaultEditor, path: Option<&std::path::Path>) {
    let Some(path) = path else {
        return;
    };
    if let Err(error) = editor.save_history(&path) {
        eprintln!("could not write history {}: {error}", path.display());
    }
}

/// Process one input line. Returns `true` when the session should end (a `\q`
/// meta-command). A meta-command is honored only at the start of a statement;
/// otherwise the line is appended to the buffer and, once the buffer ends in
/// `;`, the statement runs.
fn handle_line(
    runtime: &Runtime,
    format: OutputFormat,
    line: &str,
    buffer: &mut String,
    timing: &mut bool,
    editor: &mut DefaultEditor,
) -> bool {
    if buffer.is_empty() {
        match meta_command(line) {
            Some(Meta::Quit) => return true,
            Some(Meta::ToggleTiming) => {
                *timing = !*timing;
                println!("timing is {}", if *timing { "on" } else { "off" });
                return false;
            }
            Some(Meta::Unknown) => {
                eprintln!(
                    "unknown command '{}'. Known commands: \\q (quit), \\timing.",
                    line.trim()
                );
                return false;
            }
            None => {}
        }
    }
    if !line.trim().is_empty() {
        let _ = editor.add_history_entry(line);
    }
    if !buffer.is_empty() {
        buffer.push('\n');
    }
    buffer.push_str(line);
    if buffer.trim_end().ends_with(';') {
        let statement = std::mem::take(buffer);
        run_statement(
            runtime,
            format,
            statement.trim().trim_end_matches(';').trim(),
            *timing,
        );
    }
    false
}

/// A recognized start-of-statement meta-command.
enum Meta {
    /// End the session.
    Quit,
    /// Flip the per-statement timing line on or off.
    ToggleTiming,
    /// A backslash line that names no known command.
    Unknown,
}

/// Classify a line as a meta-command, or `None` for ordinary SQL input.
///
/// A line that STARTS with a backslash is always a meta-command attempt, even
/// when the name is not one this shell knows. Returning `None` for it would
/// append `\dt` to the SQL buffer, where it becomes part of the next statement
/// and surfaces as a parse error about the query rather than about the typo.
fn meta_command(line: &str) -> Option<Meta> {
    match line.trim() {
        "\\q" => Some(Meta::Quit),
        "\\timing" => Some(Meta::ToggleTiming),
        other if other.starts_with('\\') => Some(Meta::Unknown),
        _ => None,
    }
}

/// Execute one complete statement and print its rendered result, a row-count
/// summary, and (when enabled) the elapsed time. An empty statement is skipped.
/// An execution or render failure prints to stderr and returns; the caller's
/// loop continues.
fn run_statement(runtime: &Runtime, format: OutputFormat, sql: &str, timing: bool) {
    if sql.is_empty() {
        return;
    }
    let started = Instant::now();
    let result = runtime.execute(sql);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    match result {
        Ok((schema, batches)) => match render(format, &schema, &batches) {
            Ok(text) => {
                println!("{text}");
                let rows = row_count(&batches);
                println!("({rows} row{})", if rows == 1 { "" } else { "s" });
                if timing {
                    println!("Time: {elapsed_ms:.2} ms");
                }
            }
            Err(error) => eprintln!("error: {error}"),
        },
        Err(error) => eprintln!("error: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{history_path, meta_command, Meta};

    #[test]
    fn known_meta_commands_are_recognized() {
        assert!(matches!(meta_command("\\q"), Some(Meta::Quit)));
        assert!(matches!(
            meta_command("  \\timing  "),
            Some(Meta::ToggleTiming)
        ));
    }

    #[test]
    fn an_unknown_backslash_line_is_a_meta_command_not_sql() {
        // The point of the Unknown arm: a mistyped or unsupported command must
        // be reported as a command, not appended to the SQL buffer where it
        // surfaces later as a parse error blaming the query.
        for line in ["\\dt", "\\tming", "\\d orders", "\\"] {
            assert!(
                matches!(meta_command(line), Some(Meta::Unknown)),
                "{line} must classify as an unknown command"
            );
        }
    }

    #[test]
    fn ordinary_sql_is_not_a_meta_command() {
        for line in ["SELECT 1;", "  FROM t", "", "-- a comment"] {
            assert!(
                meta_command(line).is_none(),
                "{line} must stay ordinary input"
            );
        }
    }

    #[test]
    fn history_path_prefers_the_explicit_override() {
        // Serialized against the other env-reading test by running in one test.
        let previous = std::env::var_os("FEDQ_HISTORY");
        // SAFETY: single-threaded within this test; restored before returning.
        unsafe { std::env::set_var("FEDQ_HISTORY", "/tmp/explicit_fedq_history") };
        assert_eq!(
            history_path(),
            Some(std::path::PathBuf::from("/tmp/explicit_fedq_history"))
        );
        match previous {
            Some(value) => unsafe { std::env::set_var("FEDQ_HISTORY", value) },
            None => unsafe { std::env::remove_var("FEDQ_HISTORY") },
        }
    }
}
