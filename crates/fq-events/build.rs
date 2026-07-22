//! Compiles the vendored libsais C source (csrc/libsais.c, MIT-licensed
//! suffix-array construction) into this crate. The file is a single C99
//! translation unit that builds in seconds, always at -O3: suffix sorting is
//! the dominant cost of the paths index build, and a debug-profile sort would
//! multiply materialize wall time for no debugging value. OpenMP is enabled
//! (LIBSAIS_OPENMP) so one sort scales across cores.

fn main() {
    println!("cargo:rerun-if-changed=csrc/libsais.c");
    println!("cargo:rerun-if-changed=csrc/libsais.h");
    cc::Build::new()
        .file("csrc/libsais.c")
        .include("csrc")
        .opt_level(3)
        .flag("-fopenmp")
        .define("LIBSAIS_OPENMP", None)
        .compile("sais");
    println!("cargo:rustc-link-lib=gomp");
}
