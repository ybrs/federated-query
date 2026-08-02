//! The build log: a file a long run appends to, one line per crate.
//!
//! A build writes here and nowhere else. Nobody watches a terminal for the hours
//! a dependency pass takes, so the log is the record and it is read afterwards;
//! stdout stays empty so a query's output can be piped. Every line is flushed as
//! it is written, because the interesting log is the one belonging to the run
//! that was killed.

use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::Mutex;

use crate::index::{BuildLog, CrateReport};

/// A log file, the instant the run started, and the lock that keeps two threads
/// from interleaving a line. rust-analyzer's loader reports progress from its own
/// threads, so the lock is not decoration.
pub struct FileLog {
    file: Mutex<std::fs::File>,
    started: std::time::Instant,
}

impl FileLog {
    /// Open the log for appending, creating it if it is not there.
    pub fn appending_to(path: &std::path::Path) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        Ok(FileLog {
            file: Mutex::new(file),
            started: std::time::Instant::now(),
        })
    }

    /// Append one line, stamped with the wall clock and the elapsed time, and
    /// flush it: an unflushed line is exactly the one a killed run loses.
    pub fn line(&self, message: &str) {
        let stamp = utc_stamp(std::time::SystemTime::now());
        let elapsed = self.started.elapsed().as_secs_f64();
        let mut file = self.file.lock().expect("the log lock is not poisoned");
        writeln!(file, "{stamp} [{elapsed:>8.1}s] {message}").expect("the log file is writable");
        file.flush().expect("the log file is writable");
    }
}

impl BuildLog for FileLog {
    fn note(&self, message: &str) {
        self.line(message);
    }

    fn crate_done(&self, report: &CrateReport) {
        let mut line = String::new();
        write!(
            line,
            "[{:>4}/{}] {} ",
            report.position, report.total, report.name
        )
        .expect("writing to a string cannot fail");
        if report.skipped {
            line.push_str("already indexed, skipped");
        } else {
            write!(
                line,
                "items={} calls={} edges={} bodies_unread={} took={:.1}s",
                report.items, report.calls, report.edges, report.bodies_unread, report.seconds
            )
            .expect("writing to a string cannot fail");
        }
        self.line(&line);
    }
}

/// One instant as `YYYY-MM-DDTHH:MM:SSZ`.
///
/// A log line has to be readable next to a system log or a shell history, so it
/// carries a date and not only an offset from the start of the run.
fn utc_stamp(at: std::time::SystemTime) -> String {
    let seconds = at
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after 1970")
        .as_secs();
    let (year, month, day) = civil_date(i64::try_from(seconds / 86_400).expect("day fits in i64"));
    let time = seconds % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        time / 3600,
        (time % 3600) / 60,
        time % 60
    )
}

/// The civil date of a day number counted from 1970-01-01.
///
/// The shift to a year starting in March makes the leap day the last day of the
/// year, so the month lengths repeat on a fixed 153-day pattern and no month
/// table is needed.
fn civil_date(days_since_epoch: i64) -> (i64, i64, i64) {
    let shifted = days_since_epoch + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let march_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * march_month + 2) / 5 + 1;
    let month = if march_month < 10 {
        march_month + 3
    } else {
        march_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

#[cfg(test)]
mod tests {
    use super::civil_date;

    /// The date arithmetic is checked against days whose civil date is known,
    /// including a leap day and a century that is not a leap year.
    #[test]
    fn converts_known_days() {
        assert_eq!(civil_date(0), (1970, 1, 1));
        assert_eq!(civil_date(11_016), (2000, 2, 29));
        assert_eq!(civil_date(20_513), (2026, 3, 1));
    }
}
