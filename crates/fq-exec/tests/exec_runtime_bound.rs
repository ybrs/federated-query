//! The process-wide execution runtime's worker bound is fixed by the first
//! configuration the process loads; a later, different bound must raise.

#[test]
fn a_conflicting_max_threads_bound_raises() {
    fq_exec::ensure_exec_runtime(0).expect("the per-core default always builds");
    let cores = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let error = fq_exec::ensure_exec_runtime(cores + 7)
        .expect_err("a different bound in the same process must raise");
    assert!(
        error.contains("already built"),
        "the error must say the runtime is fixed: {error}"
    );
    fq_exec::ensure_exec_runtime(0).expect("re-asking for the same bound is fine");
}
