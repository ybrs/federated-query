//! The engine construction linter. Ports `lint/flake8_fedq.py` to Rust.
//!
//! Two rules, checked over the engine crates (never test code):
//!
//! - FQ-CONSTRUCT: reconstructing a plan / expression node by re-listing every
//!   field (`Node { a, b, c }` with no `..base`) is forbidden. On any transform
//!   this silently RESETS a field to whatever you retyped instead of preserving
//!   the original - the exact way a wrong value (a dropped column list, a reset
//!   estimate) sneaks in. Use the shallow-copy form `Node { field: new, ..base }`
//!   (Rust's `model_copy(update=...)`: it copies every OTHER field, so none can be
//!   silently reset) or clone-and-mutate. A genuinely fresh node (nothing to copy
//!   from - the parser building from SQL) is allowed ONLY when justified by >= 2
//!   comment lines directly above, confirming the field list is complete.
//! - FQ-NAMEMATCH: deciding which relation a column belongs to by matching its
//!   NAME text (`.starts_with` / `.ends_with` on a `.column` or `.table`) is
//!   forbidden. Relation membership is resolved by qualifier / identity.
//!
//! A `syn` AST walk (not a regex) so a struct DEFINITION is not mistaken for a
//! construction and a `..base` copy is correctly exempt.

use std::collections::HashSet;
use std::path::Path;
use std::process::ExitCode;

use proc_macro2::LineColumn;
use syn::spanned::Spanned;
use syn::visit::Visit;
use walkdir::WalkDir;

/// The enum names whose struct-variant literals (`Expr::Literal { .. }`) count as
/// node construction even though the variant is not itself a named struct.
const NODE_ENUMS: &[&str] = &["Expr", "LogicalPlan", "PhysicalPlan"];

/// Field names whose value is a relational identifier; a name-test method called
/// on one is string-matching relation membership (FQ-NAMEMATCH).
const RELATION_FIELDS: &[&str] = &["column", "table"];

/// Node child-field names; a `.clone()` on one deep-copies a subtree (FQ-CLONE).
const NODE_CHILD_FIELDS: &[&str] = &[
    "input", "left", "right", "body", "child", "producer", "subquery", "operand", "value",
];

/// Bare bindings that conventionally hold a plan/expr node; a `.clone()` on one
/// deep-copies a subtree (FQ-CLONE). A heuristic - a genuinely needed clone (a
/// node reused twice, e.g. a shared CTE body) is justified with >= 2 comments.
const NODE_BINDINGS: &[&str] = &[
    "plan", "node", "subtree", "input", "left", "right", "body", "child",
];

/// One reported violation: file, 1-indexed line, rule code, and message.
struct Violation {
    file: String,
    line: usize,
    code: &'static str,
    message: String,
}

fn main() -> ExitCode {
    let root = std::env::args().nth(1).unwrap_or_else(|| ".".to_string());
    let root = Path::new(&root);
    let node_names = collect_node_struct_names(root);
    let mut violations = Vec::new();
    for entry in WalkDir::new(root.join("crates"))
        .into_iter()
        .filter_map(Result::ok)
    {
        let path = entry.path();
        if !is_lintable_file(path) {
            continue;
        }
        lint_file(path, &node_names, &mut violations);
    }
    report(&violations)
}

/// A `.rs` file under an engine crate's `src/`, excluding test code and the
/// imported (battle-tested, allowed) fedqrs engine modules in fq-exec.
fn is_lintable_file(path: &Path) -> bool {
    if path.extension().is_none_or(|ext| ext != "rs") {
        return false;
    }
    let text = path.to_string_lossy();
    if !text.contains("/src/") || text.contains("/tests/") {
        return false;
    }
    // A `tests.rs` file is unit-test code by convention (a crate splits its
    // `#[cfg(test)] mod tests;` into a sibling file). The visitor cannot see the
    // parent's cfg-test gate across files, so skip the whole file.
    if path.file_name().is_some_and(|name| name == "tests.rs") {
        return false;
    }
    // The imported engine (fq-exec engine/connectors/core) is not ours to
    // restrict; it constructs its own IR types and carries its own allow.
    !(text.contains("/fq-exec/src/engine.rs")
        || text.contains("/fq-exec/src/connectors.rs")
        || text.contains("/fq-exec/src/core/"))
}

/// Every `pub struct` name declared in fq-plan's node modules - the set FQ-CONSTRUCT
/// guards. Ports the Python StateModel static scan.
fn collect_node_struct_names(root: &Path) -> HashSet<String> {
    let mut names = HashSet::new();
    for module in ["logical.rs", "physical.rs", "expr.rs"] {
        let path = root.join("crates/fq-plan/src").join(module);
        let Ok(src) = std::fs::read_to_string(&path) else {
            continue;
        };
        let Ok(file) = syn::parse_file(&src) else {
            continue;
        };
        for item in file.items {
            if let syn::Item::Struct(item_struct) = item {
                names.insert(item_struct.ident.to_string());
            }
        }
    }
    names
}

/// Parse one file and collect its violations.
fn lint_file(path: &Path, node_names: &HashSet<String>, out: &mut Vec<Violation>) {
    let Ok(src) = std::fs::read_to_string(path) else {
        return;
    };
    let Ok(file) = syn::parse_file(&src) else {
        return;
    };
    let lines: Vec<&str> = src.lines().collect();
    let mut linter = Linter {
        file: path.to_string_lossy().into_owned(),
        lines,
        node_names,
        violations: out,
    };
    linter.visit_file(&file);
}

/// The AST visitor accumulating violations for one file.
struct Linter<'a> {
    file: String,
    lines: Vec<&'a str>,
    node_names: &'a HashSet<String>,
    violations: &'a mut Vec<Violation>,
}

impl Linter<'_> {
    /// Whether the path names a guarded node type: a bare node struct
    /// (`Scan { .. }`) or an enum struct-variant (`Expr::Literal { .. }`).
    fn is_node_path(&self, path: &syn::Path) -> bool {
        let n = path.segments.len();
        let Some(last) = path.segments.last() else {
            return false;
        };
        if n == 1 {
            // Bare construction: `Scan { .. }`.
            return self.node_names.contains(&last.ident.to_string());
        }
        // `X::Y { .. }` is a node only when X is a node enum, so a same-named
        // variant of an unrelated enum (`Fragment::Aggregate`) is not flagged.
        NODE_ENUMS.contains(&path.segments[n - 2].ident.to_string().as_str())
    }

    /// Count contiguous full-line `//` comments directly above 1-indexed `line`.
    /// A blank or code line breaks the run, so the comments must defend this
    /// statement. Ports `_comments_above`.
    fn comments_above(&self, line: usize) -> usize {
        // `line` is 1-indexed; the line directly above is index `line - 2`. Walk
        // upward (reversed) counting contiguous `//` lines until a non-comment.
        let Some(above) = line.checked_sub(1) else {
            return 0;
        };
        self.lines[..above]
            .iter()
            .rev()
            .take_while(|text| text.trim_start().starts_with("//"))
            .count()
    }

    /// Record a violation at `span`'s start line.
    fn record(&mut self, span: LineColumn, code: &'static str, message: String) {
        self.violations.push(Violation {
            file: self.file.clone(),
            line: span.line,
            code,
            message,
        });
    }
}

impl<'ast> Visit<'ast> for Linter<'_> {
    /// Skip a `#[cfg(test)]` module wholesale (test code is out of scope).
    fn visit_item_mod(&mut self, node: &'ast syn::ItemMod) {
        if node.attrs.iter().any(is_cfg_test) {
            return;
        }
        syn::visit::visit_item_mod(self, node);
    }

    /// FQ-CONSTRUCT: a node struct literal with no `..base` rest.
    fn visit_expr_struct(&mut self, node: &'ast syn::ExprStruct) {
        if node.rest.is_none() && self.is_node_path(&node.path) {
            let line = node.path.span().start().line;
            if self.comments_above(line) < 2 {
                let name = node
                    .path
                    .segments
                    .last()
                    .map(|s| s.ident.to_string())
                    .unwrap_or_default();
                self.record(
                    node.path.span().start(),
                    "FQ-CONSTRUCT",
                    format!(
                        "reconstructing {name} by re-listing fields is forbidden; use \
                         `{name} {{ field: new, ..base }}` (copies every other field, \
                         Rust's model_copy) or clone-and-mutate. A genuinely fresh node \
                         needs >=2 comment lines above stating why and confirming the \
                         field list is complete"
                    ),
                );
            }
        }
        syn::visit::visit_expr_struct(self, node);
    }

    /// FQ-NAMEMATCH: `.starts_with` / `.ends_with` on a `.column` / `.table`.
    fn visit_expr_method_call(&mut self, node: &'ast syn::ExprMethodCall) {
        let method = node.method.to_string();
        if (method == "starts_with" || method == "ends_with")
            && receiver_is_relation(&node.receiver)
        {
            self.record(
                node.method.span().start(),
                "FQ-NAMEMATCH",
                "resolving relation membership by matching a column/table NAME string \
                 (.starts_with/.ends_with on .column/.table) is forbidden; resolve by \
                 qualifier/identity"
                    .to_string(),
            );
        }
        if method == "clone"
            && node.args.is_empty()
            && receiver_is_node_subtree(&node.receiver)
            && self.comments_above(node.method.span().start().line) < 2
        {
            self.record(
                node.method.span().start(),
                "FQ-CLONE",
                "cloning a plan/expr subtree deep-copies every boxed descendant (the one \
                 operation that hits the allocator); own the node or take &mut instead. A \
                 genuinely-shared node (a CTE body reused twice) needs >=2 comment lines \
                 above justifying the clone"
                    .to_string(),
            );
        }
        syn::visit::visit_expr_method_call(self, node);
    }
}

/// Whether a `.clone()` receiver is (heuristically) a plan/expr subtree: a node
/// child field (`x.input`, `x.left`) or a conventional node binding (`plan`,
/// `node`). Deliberately conservative - a false positive is silenced by the same
/// >=2-comment justification, and a missed one is caught in review.
fn receiver_is_node_subtree(expr: &syn::Expr) -> bool {
    match expr {
        syn::Expr::Field(field) => match &field.member {
            syn::Member::Named(ident) => NODE_CHILD_FIELDS.contains(&ident.to_string().as_str()),
            syn::Member::Unnamed(_) => false,
        },
        syn::Expr::Path(path) => path
            .path
            .get_ident()
            .is_some_and(|ident| NODE_BINDINGS.contains(&ident.to_string().as_str())),
        _ => false,
    }
}

/// Whether an attribute is `#[cfg(test)]`.
fn is_cfg_test(attr: &syn::Attribute) -> bool {
    if !attr.path().is_ident("cfg") {
        return false;
    }
    let mut found = false;
    let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("test") {
            found = true;
        }
        Ok(())
    });
    found
}

/// Whether an expression is a field access ending in `.column` / `.table`.
fn receiver_is_relation(expr: &syn::Expr) -> bool {
    if let syn::Expr::Field(field) = expr {
        if let syn::Member::Named(ident) = &field.member {
            return RELATION_FIELDS.contains(&ident.to_string().as_str());
        }
    }
    false
}

/// Print the violations sorted by file+line and return the process exit code.
fn report(violations: &[Violation]) -> ExitCode {
    if violations.is_empty() {
        println!("fq-lint: clean");
        return ExitCode::SUCCESS;
    }
    let mut sorted: Vec<&Violation> = violations.iter().collect();
    sorted.sort_by(|a, b| a.file.cmp(&b.file).then(a.line.cmp(&b.line)));
    for v in &sorted {
        println!("{}:{}: {} {}", v.file, v.line, v.code, v.message);
    }
    let count = |code| violations.iter().filter(|v| v.code == code).count();
    println!(
        "\nfq-lint: {} violation(s) - {} FQ-CONSTRUCT, {} FQ-CLONE, {} FQ-NAMEMATCH",
        violations.len(),
        count("FQ-CONSTRUCT"),
        count("FQ-CLONE"),
        count("FQ-NAMEMATCH"),
    );
    ExitCode::FAILURE
}
