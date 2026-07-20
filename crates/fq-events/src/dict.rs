//! Build-time dictionaries: append-only `value -> code` maps in first-seen
//! order, memory-accounted, with the raw-string promotion rule for a property
//! whose value map outgrows its budget. Codes are u64 logically; nothing here
//! or in the format caps the code space.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use crate::error::EventBuildError;

/// One column's build dictionary. `values` holds every value in code order;
/// codes for a refresh continue from the loaded prior generations.
pub struct DictBuilder {
    map: HashMap<String, u64>,
    values: Vec<String>,
    /// Codes below this were loaded from prior generations and are not part
    /// of this build's appended tail.
    first_new_code: u64,
    /// Accounted map bytes: value bytes plus a fixed per-entry overhead.
    bytes: u64,
}

/// The accounted fixed overhead per dictionary entry (hash-map slot, code,
/// string header), alongside the value bytes themselves.
const ENTRY_OVERHEAD: u64 = 64;

impl DictBuilder {
    /// A dictionary seeded with prior-generation values (empty for an initial
    /// build).
    pub fn with_values(values: Vec<String>) -> Self {
        let mut map = HashMap::with_capacity(values.len());
        let mut bytes = 0u64;
        for (code, value) in values.iter().enumerate() {
            bytes += value.len() as u64 + ENTRY_OVERHEAD;
            map.insert(value.clone(), code as u64);
        }
        let first_new_code = values.len() as u64;
        Self {
            map,
            values,
            first_new_code,
            bytes,
        }
    }

    /// The code of `value`, appending it when unseen.
    pub fn encode(&mut self, value: &str) -> u64 {
        if let Some(code) = self.map.get(value) {
            return *code;
        }
        let code = self.values.len() as u64;
        self.values.push(value.to_string());
        self.map.insert(value.to_string(), code);
        self.bytes += value.len() as u64 * 2 + ENTRY_OVERHEAD;
        code
    }

    /// Accounted map bytes.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Total codes (prior + appended).
    pub fn code_count(&self) -> u64 {
        self.values.len() as u64
    }

    /// The values appended by THIS build (the `.dict` generation payload).
    pub fn appended(&self) -> &[String] {
        &self.values[self.first_new_code as usize..]
    }

    /// The first code of this build's appended tail.
    pub fn first_new_code(&self) -> u64 {
        self.first_new_code
    }

    /// Every value in code order.
    pub fn values(&self) -> &[String] {
        &self.values
    }
}

/// The shared build dictionaries: the event-name dictionary plus one per
/// dict-encoded property, each behind its own lock (workers encode a whole
/// batch per acquisition, so contention stays coarse).
pub struct BuildDicts {
    pub event: Mutex<DictBuilder>,
    pub props: Vec<PropDict>,
    dict_max_bytes: u64,
}

/// One dict-encoded property's build state: the dictionary plus its
/// promotion flag. Once promoted, encoded codes are dead weight (the spill
/// always carries the raw string) and the segment encoder writes raw-string
/// blocks instead.
pub struct PropDict {
    pub dict: Mutex<DictBuilder>,
    pub promoted: AtomicBool,
}

impl BuildDicts {
    /// Fresh dictionaries for a build: the event dictionary plus one per
    /// dict property, each seeded with prior-generation values (empty vecs
    /// for an initial build).
    pub fn new(
        event_values: Vec<String>,
        prop_values: Vec<Vec<String>>,
        dict_max_bytes: u64,
    ) -> Self {
        let props = prop_values
            .into_iter()
            .map(|values| PropDict {
                dict: Mutex::new(DictBuilder::with_values(values)),
                promoted: AtomicBool::new(false),
            })
            .collect();
        Self {
            event: Mutex::new(DictBuilder::with_values(event_values)),
            props,
            dict_max_bytes,
        }
    }

    /// Encode one batch worth of event names into `codes`. The event column
    /// may NOT be promoted: the analyses' inner loops are defined over event
    /// codes, so exceeding the budget raises `EventNameCardinality`.
    pub fn encode_events<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
        codes: &mut Vec<u64>,
    ) -> Result<(), EventBuildError> {
        let mut dict = self.event.lock().expect("event dict lock poisoned");
        for name in names {
            codes.push(dict.encode(name));
        }
        if dict.bytes() > self.dict_max_bytes {
            return Err(EventBuildError::EventNameCardinality {
                budget: self.dict_max_bytes,
                count: dict.code_count(),
            });
        }
        Ok(())
    }

    /// Encode one batch worth of one property's values into `codes` (null
    /// positions push 0). Returns whether the property is (now) promoted;
    /// when the map crosses its budget the property PROMOTES to raw-string
    /// encoding: the partial dictionary stays loaded but unused, already
    /// spilled rows carry their raw value, and group-bys/filters compare
    /// strings - slower, never wrong, never capped.
    pub fn encode_prop<'a>(
        &self,
        index: usize,
        values: impl Iterator<Item = Option<&'a str>>,
        codes: &mut Vec<u64>,
    ) -> bool {
        let prop = &self.props[index];
        if prop.promoted.load(Ordering::Relaxed) {
            for _ in values {
                codes.push(0);
            }
            return true;
        }
        let mut dict = prop.dict.lock().expect("prop dict lock poisoned");
        for value in values {
            match value {
                Some(value) => codes.push(dict.encode(value)),
                None => codes.push(0),
            }
        }
        if dict.bytes() > self.dict_max_bytes {
            prop.promoted.store(true, Ordering::Relaxed);
            return true;
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_are_first_seen_order_and_unbounded() {
        let mut dict = DictBuilder::with_values(Vec::new());
        assert_eq!(dict.encode("a"), 0);
        assert_eq!(dict.encode("b"), 1);
        assert_eq!(dict.encode("a"), 0);
        // Far past any small width: 70k distinct values encode fine.
        for index in 0..70_000 {
            dict.encode(&format!("value-{index}"));
        }
        assert_eq!(dict.code_count(), 70_002);
        assert_eq!(dict.appended().len(), 70_002);
    }

    #[test]
    fn refresh_seeding_continues_codes() {
        let mut dict = DictBuilder::with_values(vec!["x".to_string(), "y".to_string()]);
        assert_eq!(dict.encode("y"), 1);
        assert_eq!(dict.encode("z"), 2);
        assert_eq!(dict.first_new_code(), 2);
        assert_eq!(dict.appended(), ["z".to_string()]);
    }

    #[test]
    fn event_dictionary_over_budget_raises() {
        let dicts = BuildDicts::new(Vec::new(), Vec::new(), 200);
        let mut codes = Vec::new();
        let names: Vec<String> = (0..100)
            .map(|index| format!("event-name-{index}"))
            .collect();
        let error = dicts
            .encode_events(names.iter().map(String::as_str), &mut codes)
            .unwrap_err();
        assert!(matches!(
            error,
            EventBuildError::EventNameCardinality { .. }
        ));
    }

    #[test]
    fn property_over_budget_promotes_instead_of_raising() {
        let dicts = BuildDicts::new(Vec::new(), vec![Vec::new()], 200);
        let mut codes = Vec::new();
        let values: Vec<String> = (0..100).map(|index| format!("value-{index}")).collect();
        assert!(dicts.encode_prop(0, values.iter().map(|v| Some(v.as_str())), &mut codes));
        assert!(dicts.props[0].promoted.load(Ordering::Relaxed));
    }
}
