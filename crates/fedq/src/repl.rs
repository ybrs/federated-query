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
    Ok(())
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
}

/// Classify a line as a meta-command, or `None` for ordinary SQL input.
fn meta_command(line: &str) -> Option<Meta> {
    match line.trim() {
        "\\q" => Some(Meta::Quit),
        "\\timing" => Some(Meta::ToggleTiming),
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
