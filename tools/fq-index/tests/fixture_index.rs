//! The indexer is checked against `tests/fixture`, a crate small enough that
//! every answer is known by reading it.
//!
//! What is pinned here is exactly what a syntax-only index would get wrong: a
//! method call resolved through the receiver's inferred type, a call generated
//! by a macro body, a call that reaches a standard-library sink, dispatch
//! through a trait object, and the calls that are genuinely unresolvable.

use std::process::{Command, Output};

/// Build an index of the fixture crate in a temporary directory and return the
/// path of the database, panicking with the tool's own output if it fails.
fn build_fixture_index(temp: &tempfile::TempDir) -> String {
    let db_path = temp.path().join("fixture.sqlite");
    let db_path = db_path.to_str().expect("temp path is utf8").to_string();
    let output = run_build(&db_path, &[]);
    assert!(
        output.status.success(),
        "index build failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    db_path
}

/// Run one build over the fixture crate with the given extra arguments.
fn run_build(db_path: &str, extra: &[&str]) -> Output {
    let root = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixture");
    Command::new(env!("CARGO_BIN_EXE_fq-index"))
        .args(["--db", db_path])
        .args(["build", "--root", root, "--workspace"])
        .args(extra)
        .output()
        .expect("the indexer binary runs")
}

/// The log a build wrote, which is where its progress goes.
fn build_log(db_path: &str) -> String {
    std::fs::read_to_string(format!("{db_path}.log")).expect("the build wrote its log")
}

/// One counted answer from the index, for comparing two builds.
fn count(db_path: &str, table: &str) -> usize {
    let output = query(db_path, &["coverage"]);
    for line in output.lines() {
        let trimmed = line.trim();
        if let Some(number) = trimmed.strip_prefix(&format!("{table}: ")) {
            return number.parse().expect("a coverage count is a number");
        }
    }
    panic!("coverage does not report {table}:\n{output}");
}

/// Run one query against a built index and return its stdout.
fn query(db_path: &str, args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_fq-index"))
        .args(["--db", db_path])
        .args(args)
        .output()
        .expect("the indexer binary runs");
    assert!(
        output.status.success(),
        "query {args:?} failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("output is utf8")
}

/// A definition lookup finds the fixture's own functions with their file and line.
#[test]
fn finds_definitions_in_the_fixture() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(&db_path, &["defs", "fixture::wipe"]);
    assert!(output.contains("fixture::wipe [fn]"), "{output}");
    assert!(output.contains("lib.rs:"), "{output}");
}

/// The standard library is part of the index, so a `std` function is a real item
/// with a body, not merely a name seen at a call site.
#[test]
fn indexes_the_standard_library() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(&db_path, &["defs", "std::fs::remove_file"]);
    assert!(output.contains("std::fs::remove_file"), "{output}");
}

/// A call into the standard library produces an edge, so `wipe` reaches
/// `remove_file` through resolved calls alone.
#[test]
fn reaches_a_standard_library_sink() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(
        &db_path,
        &[
            "reach",
            "--from",
            "fixture::wipe",
            "--to",
            "std::fs::remove_file",
        ],
    );
    assert!(
        output.contains("REACHABLE std::fs::remove_file"),
        "{output}"
    );
    assert!(output.contains("resolved calls only"), "{output}");
}

/// A function that removes nothing does not reach the removal sink. This is the
/// negative answer the acceptance question depends on being trustworthy.
#[test]
fn does_not_invent_a_path_that_is_not_there() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(
        &db_path,
        &[
            "reach",
            "--from",
            "fixture::touch",
            "--to",
            "std::fs::remove_file",
        ],
    );
    assert!(
        output.contains("NOT REACHABLE std::fs::remove_file"),
        "{output}"
    );
}

/// A call on a `dyn Trait` receiver resolves to the trait method, and the walk
/// expands it to the implementations, marking the step as dispatch rather than
/// as a resolved call.
#[test]
fn expands_dispatch_through_a_trait_object() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(
        &db_path,
        &[
            "reach",
            "--from",
            "fixture::Engine::run",
            "--to",
            "fixture::DiskStore::purge",
        ],
    );
    assert!(
        output.contains("REACHABLE fixture::DiskStore::purge"),
        "{output}"
    );
    assert!(output.contains("only via trait dispatch"), "{output}");
    assert!(output.contains("~>"), "{output}");
}

/// A call written inside a macro body is indexed, because the macro is expanded
/// rather than skipped.
#[test]
fn sees_calls_inside_macro_bodies() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(
        &db_path,
        &[
            "reach",
            "--from",
            "fixture::Engine::describe",
            "--to",
            "fixture::Engine::count",
        ],
    );
    assert!(
        output.contains("REACHABLE fixture::Engine::count"),
        "{output}"
    );
}

/// A call through a function pointer is recorded as unresolvable, so a query
/// over that function reports an edge the index does not have instead of an
/// empty, confident answer.
#[test]
fn reports_a_function_pointer_call_as_unresolved() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(&db_path, &["calls", "fixture::indirect"]);
    assert!(output.contains("fn_pointer"), "{output}");
    assert!(
        output.contains("not resolved to a single function"),
        "{output}"
    );
}

/// Every query names its own blind spots, and the build records them.
#[test]
fn coverage_reports_the_blind_spots() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = query(&db_path, &["coverage"]);
    for key in [
        "blind.closure_calls",
        "blind.fn_pointer_calls",
        "blind.unresolved_calls",
        "blind.trait_method_calls",
        "blind.unexpanded_macros",
    ] {
        assert!(output.contains(key), "{key} missing from:\n{output}");
    }
}

/// A build reports itself to its log file and prints nothing at all, so a long
/// run is watched by reading the log and stdout stays free for query results.
#[test]
fn writes_progress_to_the_log_and_nothing_to_the_terminal() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = temp.path().join("fixture.sqlite");
    let db_path = db_path.to_str().expect("temp path is utf8").to_string();
    let output = run_build(&db_path, &[]);
    assert!(output.status.success());
    assert_eq!(output.stdout, Vec::<u8>::new());
    assert_eq!(output.stderr, Vec::<u8>::new());
    let log = build_log(&db_path);
    assert!(log.contains("[   1/1] fixture items="), "{log}");
    assert!(log.contains("edges="), "{log}");
    assert!(log.contains("build finished in"), "{log}");
    assert!(
        log.contains("Z ["),
        "a log line carries a timestamp:\n{log}"
    );
}

/// Each crate is committed as it is finished, so a second run finds it already
/// there and does not read it again. This is what makes a killed run continue
/// rather than start over.
#[test]
fn skips_crates_an_earlier_run_finished() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let before = (count(&db_path, "item"), count(&db_path, "edge"));
    let output = run_build(&db_path, &[]);
    assert!(output.status.success());
    let log = build_log(&db_path);
    assert!(log.contains("already indexed, skipped"), "{log}");
    let after = (count(&db_path, "item"), count(&db_path, "edge"));
    assert_eq!(
        before, after,
        "a resumed run must not write a second copy of a crate"
    );
}

/// `--rebuild` throws the index away and reads everything again, and reading the
/// same sources again produces the same index.
#[test]
fn rebuilding_reads_everything_again() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let before = (count(&db_path, "item"), count(&db_path, "edge"));
    let output = run_build(&db_path, &["--rebuild"]);
    assert!(output.status.success());
    let log = build_log(&db_path);
    let last_crate_line = log
        .lines()
        .rfind(|line| line.contains("[   1/1] fixture"))
        .expect("the rebuild logged its crate");
    assert!(
        !last_crate_line.contains("skipped"),
        "a rebuild reads the crate again: {last_crate_line}"
    );
    assert_eq!(before, (count(&db_path, "item"), count(&db_path, "edge")));
}

/// A build that names no scope is refused. Silently defaulting to one would
/// leave the caller thinking the dependency graph was read when it was not.
#[test]
fn a_build_without_a_scope_fails() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = temp.path().join("fixture.sqlite");
    let output = Command::new(env!("CARGO_BIN_EXE_fq-index"))
        .args(["--db", db_path.to_str().expect("temp path is utf8")])
        .args([
            "build",
            "--root",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixture"),
        ])
        .output()
        .expect("the indexer binary runs");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("name what to read"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// A workspace-only build names the dependency functions its code calls, and
/// says plainly that their bodies were never read - which is what bounds a
/// "not reachable" answer.
#[test]
fn names_the_crates_it_did_not_read() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let coverage = query(&db_path, &["coverage"]);
    assert!(
        coverage.contains("crates in the index had their bodies read"),
        "{coverage}"
    );
    let reach = query(
        &db_path,
        &[
            "reach",
            "--from",
            "fixture::wipe",
            "--to",
            "std::fs::remove_file",
        ],
    );
    assert!(
        reach.contains("crates were reached but NEVER read"),
        "{reach}"
    );
}

/// A pattern that matches nothing is an error, never an empty result that reads
/// like "there are no callers".
#[test]
fn an_unmatched_pattern_fails_loudly() {
    let temp = tempfile::tempdir().expect("temp dir");
    let db_path = build_fixture_index(&temp);
    let output = Command::new(env!("CARGO_BIN_EXE_fq-index"))
        .args([
            "--db",
            db_path.as_str(),
            "callers",
            "no_such_function_anywhere",
        ])
        .output()
        .expect("the indexer binary runs");
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("no definition matches"),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
