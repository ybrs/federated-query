//! Safe wrappers over the vendored libsais C library: suffix-array and LCP
//! construction for the paths index. Two input widths are exposed - byte
//! text for datasets whose event dictionary fits 8-bit symbols, and i32 text
//! for any larger dictionary - each returning plain `Vec<i32>` arrays. Every
//! native return code is checked; a nonzero code raises, never degrades.

use crate::error::EventBuildError;

extern "C" {
    fn libsais_omp(t: *const u8, sa: *mut i32, n: i32, fs: i32, freq: *mut i32, threads: i32)
        -> i32;
    fn libsais_int_omp(t: *mut i32, sa: *mut i32, n: i32, k: i32, fs: i32, threads: i32) -> i32;
    fn libsais_plcp_omp(t: *const u8, sa: *const i32, plcp: *mut i32, n: i32, threads: i32)
        -> i32;
    fn libsais_plcp_int_omp(t: *const i32, sa: *const i32, plcp: *mut i32, n: i32, threads: i32)
        -> i32;
    fn libsais_lcp_omp(plcp: *const i32, sa: *const i32, lcp: *mut i32, n: i32, threads: i32)
        -> i32;
}

/// The i32 length of a text, raising when it exceeds the 32-bit suffix-array
/// bound instead of truncating.
fn checked_n(len: usize) -> Result<i32, EventBuildError> {
    i32::try_from(len).map_err(|_| EventBuildError::SuffixTextTooLong { positions: len })
}

/// Working-room entries appended to a suffix-array buffer: capped so the
/// surplus never exceeds the i32 index space next to n.
fn free_space(len: usize) -> i32 {
    let cap = i32::MAX as usize - len;
    i32::try_from((len / 2).min(cap)).expect("free space fits i32")
}

/// Map a libsais return code: zero is success, anything else raises.
fn checked(code: i32) -> Result<(), EventBuildError> {
    if code == 0 {
        return Ok(());
    }
    Err(EventBuildError::SuffixSort { code })
}

/// The suffix array of a byte text, sorted over `threads` cores.
pub(crate) fn suffix_array_u8(text: &[u8], threads: i32) -> Result<Vec<i32>, EventBuildError> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let n = checked_n(text.len())?;
    // Extra tail space measurably speeds the sort (libsais uses any surplus
    // as working room); the buffer truncates back to n entries afterwards.
    let fs = free_space(text.len());
    let mut sa = vec![0i32; text.len() + fs as usize];
    // The native call writes n entries and scratches the fs tail.
    checked(unsafe {
        libsais_omp(text.as_ptr(), sa.as_mut_ptr(), n, fs, std::ptr::null_mut(), threads)
    })?;
    sa.truncate(text.len());
    Ok(sa)
}

/// The suffix array of an i32 text over the alphabet `0..k`, sorted over
/// `threads` cores. The text is cloned because libsais_int transforms its
/// input buffer in place during construction.
pub(crate) fn suffix_array_i32(
    text: &[i32],
    k: i32,
    threads: i32,
) -> Result<Vec<i32>, EventBuildError> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let n = checked_n(text.len())?;
    let mut scratch = text.to_vec();
    // Extra tail space measurably speeds the sort, as in [`suffix_array_u8`].
    let fs = free_space(text.len());
    let mut sa = vec![0i32; text.len() + fs as usize];
    // The native call writes n entries and scratches the fs tail.
    checked(unsafe {
        libsais_int_omp(scratch.as_mut_ptr(), sa.as_mut_ptr(), n, k, fs, threads)
    })?;
    sa.truncate(text.len());
    Ok(sa)
}

/// The LCP array of a byte text and its suffix array: `lcp[i]` is the longest
/// common prefix of the suffixes at `sa[i - 1]` and `sa[i]` (`lcp[0]` is 0).
pub(crate) fn lcp_array_u8(
    text: &[u8],
    sa: &[i32],
    threads: i32,
) -> Result<Vec<i32>, EventBuildError> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let n = checked_n(text.len())?;
    let mut plcp = vec![0i32; text.len()];
    // Both native calls read and write exactly n entries of sized buffers.
    checked(unsafe { libsais_plcp_omp(text.as_ptr(), sa.as_ptr(), plcp.as_mut_ptr(), n, threads) })?;
    let mut lcp = vec![0i32; text.len()];
    checked(unsafe { libsais_lcp_omp(plcp.as_ptr(), sa.as_ptr(), lcp.as_mut_ptr(), n, threads) })?;
    Ok(lcp)
}

/// The LCP array of an i32 text and its suffix array, shaped like
/// [`lcp_array_u8`].
pub(crate) fn lcp_array_i32(
    text: &[i32],
    sa: &[i32],
    threads: i32,
) -> Result<Vec<i32>, EventBuildError> {
    if text.is_empty() {
        return Ok(Vec::new());
    }
    let n = checked_n(text.len())?;
    let mut plcp = vec![0i32; text.len()];
    // Both native calls read and write exactly n entries of sized buffers.
    checked(unsafe {
        libsais_plcp_int_omp(text.as_ptr(), sa.as_ptr(), plcp.as_mut_ptr(), n, threads)
    })?;
    let mut lcp = vec![0i32; text.len()];
    checked(unsafe { libsais_lcp_omp(plcp.as_ptr(), sa.as_ptr(), lcp.as_mut_ptr(), n, threads) })?;
    Ok(lcp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Brute-force suffix order and LCP of a tiny text, checked against the
    /// native construction on both input widths.
    #[test]
    fn suffix_and_lcp_match_brute_force() {
        let text = b"banana".to_vec();
        let sa = suffix_array_u8(&text, 1).expect("suffix array");
        assert_eq!(sa, vec![5, 3, 1, 0, 4, 2]);
        let lcp = lcp_array_u8(&text, &sa, 1).expect("lcp");
        assert_eq!(lcp, vec![0, 1, 3, 0, 0, 2]);
        let wide: Vec<i32> = text.iter().map(|byte| i32::from(*byte)).collect();
        let sa_wide = suffix_array_i32(&wide, 256, 1).expect("wide suffix array");
        assert_eq!(sa_wide, sa);
        let lcp_wide = lcp_array_i32(&wide, &sa_wide, 1).expect("wide lcp");
        assert_eq!(lcp_wide, lcp);
    }

    /// An empty text yields empty arrays without touching the native library.
    #[test]
    fn empty_text_is_empty_arrays() {
        let sa = suffix_array_u8(&[], 1).expect("empty suffix array");
        assert!(sa.is_empty());
    }
}
