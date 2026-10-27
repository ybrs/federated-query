//! Own native JSON documents and borrow their values for lookup and rendering.

use datafusion::common::{DataFusionError, Result};
use fq_common::json_path::{JsonPath, JsonStep};
use std::collections::VecDeque;
use std::ffi::CStr;
use std::ptr::NonNull;
use yyjson_sys as ffi;

// These are yyjson's public kind tags returned by yyjson_get_type.
const JSON_NULL: u8 = 2;
const JSON_STRING: u8 = 5;
const JSON_ARRAY: u8 = 6;
const JSON_OBJECT: u8 = 7;
const READ_FLAGS: u32 = ffi::YYJSON_READ_ALLOW_TRAILING_COMMAS
    | ffi::YYJSON_READ_ALLOW_INF_AND_NAN
    | ffi::YYJSON_READ_BIGNUM_AS_RAW;

pub(super) struct JsonDocument(NonNull<ffi::yyjson_doc>);

#[derive(Clone)]
struct JsonValue<'document> {
    pointer: NonNull<ffi::yyjson_val>,
    document: &'document JsonDocument,
}

impl Drop for JsonDocument {
    /// Release the document only after its borrowed values have gone out of scope.
    fn drop(&mut self) {
        // SAFETY: parsing returned this owned, non-null document exactly once.
        unsafe { ffi::yyjson_doc_free(self.0.as_ptr()) };
    }
}

impl JsonDocument {
    /// Parse with the same trailing-comma, nonfinite and big-number rules as DuckDB.
    pub(super) fn parse(input: &str) -> Result<Self> {
        let mut error = read_error_slot();
        // SAFETY: input stays live and INSITU is absent, so yyjson copies and never modifies it.
        let pointer = unsafe {
            ffi::yyjson_read_opts(
                input.as_ptr().cast_mut().cast(),
                input.len(),
                READ_FLAGS,
                std::ptr::null(),
                &raw mut error,
            )
        };
        NonNull::new(pointer)
            .map(Self)
            .ok_or_else(|| read_error(&error))
    }

    /// Traverse only the parsed document; missing matches remain absent.
    pub(super) fn extract(&self, path: &JsonPath) -> Result<Vec<Option<String>>> {
        // SAFETY: this live document owns a non-null root value.
        let root = unsafe { ffi::yyjson_doc_get_root(self.0.as_ptr()) };
        let mut values = vec![self.value(root).expect("JSON document has a root")];
        for step in &path.steps {
            values = lookup(values, step);
        }
        values.iter().map(JsonValue::text).collect()
    }

    /// Tie a borrowed native value's lifetime to its owning document.
    fn value(&self, pointer: *mut ffi::yyjson_val) -> Option<JsonValue<'_>> {
        NonNull::new(pointer).map(|pointer| JsonValue {
            pointer,
            document: self,
        })
    }
}

/// Initialize every native diagnostic field before handing it to the parser.
fn read_error_slot() -> ffi::yyjson_read_err {
    ffi::yyjson_read_err {
        code: 0,
        msg: std::ptr::null(),
        pos: 0,
    }
}

/// Preserve the parser's native position, code and diagnostic text.
fn read_error(error: &ffi::yyjson_read_err) -> DataFusionError {
    let message = if error.msg.is_null() {
        "JSON parser returned no diagnostic".into()
    } else {
        // SAFETY: yyjson returns a static, terminated error string on failure.
        unsafe { CStr::from_ptr(error.msg) }
            .to_string_lossy()
            .into_owned()
    };
    DataFusionError::Execution(format!(
        "Malformed JSON at byte {} (code {}): {message}",
        error.pos, error.code
    ))
}

/// Apply one path operation without introducing NULLs for absent wildcard members.
fn lookup<'document>(
    values: Vec<JsonValue<'document>>,
    step: &JsonStep,
) -> Vec<JsonValue<'document>> {
    let mut selected = Vec::new();
    for value in values {
        selected.extend(value.select(step));
    }
    selected
}

impl JsonValue<'_> {
    /// Read the native node's kind while the owning document remains live.
    fn kind(&self) -> u8 {
        // SAFETY: this value is borrowed from a live parsed document.
        unsafe { ffi::yyjson_get_type(self.pointer.as_ptr()) }
    }

    /// Select children using the key, index, pointer, wildcard or recursive rule.
    fn select(&self, step: &JsonStep) -> Vec<Self> {
        match step {
            JsonStep::Field(key) => self.field(key).into_iter().collect(),
            JsonStep::Index(index) => self.index(*index).into_iter().collect(),
            JsonStep::ArrayIndex(index) => usize::try_from(*index)
                .ok()
                .and_then(|index| self.array_value(index))
                .into_iter()
                .collect(),
            JsonStep::Descendants { include_self } => self.descendants(*include_self),
            JsonStep::Pointer(token) => self.pointer_token(token).into_iter().collect(),
            JsonStep::FromEnd(index) => self.index_from_end(*index).into_iter().collect(),
            JsonStep::Missing => Vec::new(),
            JsonStep::ObjectWildcard => self.object_values(),
            JsonStep::ArrayWildcard => self.array_values(),
            JsonStep::Recursive(inner) => self.recursive(inner),
        }
    }

    /// Resolve the first object entry with this exact, case-sensitive UTF-8 key.
    fn field(&self, key: &str) -> Option<Self> {
        // SAFETY: yyjson validates the node kind and reads exactly key.len() bytes.
        let value =
            unsafe { ffi::yyjson_obj_getn(self.pointer.as_ptr(), key.as_ptr().cast(), key.len()) };
        self.document.value(value)
    }

    /// Interpret pointer tokens as indexes for arrays and literal keys for objects.
    fn pointer_token(&self, token: &str) -> Option<Self> {
        if self.kind() == JSON_ARRAY {
            return pointer_index(token).and_then(|index| self.array_value(index));
        }
        self.field(token)
    }

    /// Negative zero denotes the first element, matching nonnegative zero.
    fn index_from_end(&self, distance: u64) -> Option<Self> {
        if self.kind() != JSON_ARRAY {
            return None;
        }
        if distance == 0 {
            return self.array_value(0);
        }
        let distance = usize::try_from(distance).ok()?;
        self.length()
            .checked_sub(distance)
            .and_then(|index| self.array_value(index))
    }

    /// Resolve a zero-based or negative array index without integer overflow.
    fn index(&self, index: i64) -> Option<Self> {
        if self.kind() != JSON_ARRAY {
            return None;
        }
        let index = if index < 0 {
            self.length() as i128 + i128::from(index)
        } else {
            i128::from(index)
        };
        usize::try_from(index)
            .ok()
            .and_then(|index| self.array_value(index))
    }

    /// Read a container's declared number of members.
    fn length(&self) -> usize {
        // SAFETY: the value belongs to a live parsed document.
        unsafe { ffi::yyjson_get_len(self.pointer.as_ptr()) }
    }

    /// Borrow an array element, preserving a JSON-null element as a real value.
    fn array_value(&self, index: usize) -> Option<Self> {
        // SAFETY: yyjson checks the node kind and index bounds.
        let value = unsafe { ffi::yyjson_arr_get(self.pointer.as_ptr(), index) };
        self.document.value(value)
    }

    /// Enumerate array members in source order, or nothing for a non-array.
    fn array_values(&self) -> Vec<Self> {
        if self.kind() != JSON_ARRAY {
            return Vec::new();
        }
        let mut values = Vec::with_capacity(self.length());
        for index in 0..self.length() {
            values.push(self.array_value(index).expect("JSON array member exists"));
        }
        values
    }

    /// Enumerate object values in source order, retaining duplicate keys.
    fn object_values(&self) -> Vec<Self> {
        if self.kind() != JSON_OBJECT {
            return Vec::new();
        }
        // SAFETY: this live value is an object, so its iterator covers every member.
        let mut iterator = unsafe { ffi::yyjson_obj_iter_with(self.pointer.as_ptr()) };
        let mut values = Vec::with_capacity(self.length());
        for _ in 0..self.length() {
            // SAFETY: the iterator has an unconsumed member; its key owns the following value.
            let value = unsafe {
                ffi::yyjson_obj_iter_get_val(ffi::yyjson_obj_iter_next(&raw mut iterator))
            };
            values.push(
                self.document
                    .value(value)
                    .expect("JSON object member exists"),
            );
        }
        values
    }

    /// Enumerate children for recursive lookup while leaving scalar nodes as leaves.
    fn children(&self) -> Vec<Self> {
        match self.kind() {
            JSON_ARRAY => self.array_values(),
            JSON_OBJECT => self.object_values(),
            _ => Vec::new(),
        }
    }

    /// Apply recursive lookups in breadth-first document order.
    fn recursive(&self, step: &JsonStep) -> Vec<Self> {
        let mut pending = VecDeque::from([self.clone()]);
        let mut selected = Vec::new();
        while let Some(value) = pending.pop_front() {
            selected.extend(value.select(step));
            pending.extend(value.children());
        }
        selected
    }

    /// Enumerate recursive wildcard matches breadth-first, optionally including the root.
    fn descendants(&self, include_self: bool) -> Vec<Self> {
        let mut pending = VecDeque::from([self.clone()]);
        let mut values = Vec::new();
        while let Some(value) = pending.pop_front() {
            pending.extend(value.children());
            values.push(value);
        }
        if !include_self {
            values.remove(0);
        }
        values
    }

    /// Return strings unquoted, JSON null as SQL NULL, and other values as compact JSON.
    fn text(&self) -> Result<Option<String>> {
        match self.kind() {
            JSON_NULL => Ok(None),
            JSON_STRING => self.string().map(Some),
            _ => self.serialized().map(Some),
        }
    }

    /// Preserve embedded NULs by reading the explicit string length.
    fn string(&self) -> Result<String> {
        // SAFETY: the value is a string whose buffer remains owned by its document.
        let bytes = unsafe {
            std::slice::from_raw_parts(
                ffi::yyjson_get_str(self.pointer.as_ptr()).cast::<u8>(),
                self.length(),
            )
        };
        String::from_utf8(bytes.to_vec())
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    /// Serialize with yyjson's minified representation and nonfinite-number spelling.
    fn serialized(&self) -> Result<String> {
        let buffer = self.serialization_buffer()?;
        String::from_utf8(buffer.bytes().to_vec())
            .map_err(|error| DataFusionError::Execution(error.to_string()))
    }

    /// Own yyjson's independently allocated output so every return path frees it.
    fn serialization_buffer(&self) -> Result<SerializedBuffer> {
        let mut length = 0;
        // SAFETY: the live value is immutable; the default allocator owns the result.
        let pointer = unsafe {
            ffi::yyjson_val_write(
                self.pointer.as_ptr(),
                ffi::YYJSON_WRITE_ALLOW_INF_AND_NAN,
                &raw mut length,
            )
        };
        let pointer = NonNull::new(pointer).ok_or_else(|| {
            DataFusionError::Execution("JSON serialization allocation failed".into())
        })?;
        Ok(SerializedBuffer { pointer, length })
    }
}

/// JSON Pointer array tokens use unsigned decimal indexes without leading zeros.
fn pointer_index(token: &str) -> Option<usize> {
    match token.as_bytes() {
        [b'0'] => Some(0),
        [b'1'..=b'9', ..] if token.bytes().all(|byte| byte.is_ascii_digit()) => token.parse().ok(),
        _ => None,
    }
}

struct SerializedBuffer {
    pointer: NonNull<std::ffi::c_char>,
    length: usize,
}

impl SerializedBuffer {
    /// Borrow exactly the initialized bytes owned by this native output allocation.
    fn bytes(&self) -> &[u8] {
        // SAFETY: yyjson allocated and initialized length bytes; this buffer is live.
        unsafe { std::slice::from_raw_parts(self.pointer.as_ptr().cast(), self.length) }
    }
}

impl Drop for SerializedBuffer {
    /// Release the sole owned output buffer through yyjson's default allocator.
    fn drop(&mut self) {
        // SAFETY: the default writer allocator uses malloc/free and this is its unique owner.
        unsafe { libc::free(self.pointer.as_ptr().cast()) };
    }
}
