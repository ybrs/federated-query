//! Resolve scalar, integer and list path arguments without changing their shape.

use super::{document::JsonDocument, parse_path};
use arrow::array::{Array, ArrayRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use fq_common::json_path::{JsonPath, JsonStep};

#[derive(Clone)]
pub(super) enum Selection {
    Single(JsonPath),
    Many(Vec<JsonPath>),
}

pub(super) enum PathArgument {
    Constant(Option<Selection>),
    Column(ArrayRef),
}

impl PathArgument {
    /// Parse constant paths once per batch and retain dynamic paths as Arrow arrays.
    pub(super) fn new(argument: &ColumnarValue) -> Result<Self> {
        match argument {
            ColumnarValue::Scalar(value) => scalar(value).map(Self::Constant),
            ColumnarValue::Array(array) => Ok(Self::Column(array.clone())),
        }
    }

    /// Resolve the path for one row, retaining SQL NULL propagation.
    pub(super) fn at(&self, row: usize) -> Result<Option<Selection>> {
        match self {
            Self::Constant(selection) => Ok(selection.clone()),
            Self::Column(array) => scalar(&ScalarValue::try_from_array(array, row)?),
        }
    }
}

impl Selection {
    /// A wildcard path and an explicit list of paths both return a list.
    pub(super) fn multiple(&self) -> bool {
        match self {
            Self::Single(path) => path.multiple(),
            Self::Many(_) => true,
        }
    }

    /// Retain one output per explicit path while wildcard lookup omits absent members.
    pub(super) fn extract(&self, document: &JsonDocument) -> Result<Vec<Option<String>>> {
        match self {
            Self::Single(path) => document.extract(path),
            Self::Many(paths) => paths.iter().map(|path| one_path(document, path)).collect(),
        }
    }
}

/// Convert the supported path signatures into a typed lookup selection.
pub(super) fn scalar(value: &ScalarValue) -> Result<Option<Selection>> {
    if value.is_null() {
        return Ok(None);
    }
    match value {
        ScalarValue::Utf8(Some(path))
        | ScalarValue::Utf8View(Some(path))
        | ScalarValue::LargeUtf8(Some(path)) => {
            parse_path(path).map(|path| Some(Selection::Single(path)))
        }
        ScalarValue::Int64(Some(index)) => Ok(Some(Selection::Single(JsonPath {
            steps: vec![JsonStep::Index(*index)],
        }))),
        ScalarValue::List(paths) => {
            list_paths(&paths.value(0)).map(|paths| Some(Selection::Many(paths)))
        }
        _ => Err(DataFusionError::Plan(
            "JSON path must be VARCHAR, BIGINT, or VARCHAR[]".into(),
        )),
    }
}

/// Validate every explicit path before evaluating any document.
fn list_paths(array: &ArrayRef) -> Result<Vec<JsonPath>> {
    let mut paths = Vec::with_capacity(array.len());
    for index in 0..array.len() {
        let path = list_path(&ScalarValue::try_from_array(array, index)?)?;
        paths.push(path);
    }
    Ok(paths)
}

/// Explicit path lists cannot contain a wildcard that would change element shape.
fn list_path(value: &ScalarValue) -> Result<JsonPath> {
    let Some(Selection::Single(path)) = scalar(value)? else {
        return Err(DataFusionError::Plan("JSON path cannot be NULL".into()));
    };
    if path.multiple() {
        return Err(DataFusionError::Plan(
            "cannot have wildcards when supplying multiple JSON paths".into(),
        ));
    }
    Ok(path)
}

/// Missing values and JSON null both produce a NULL element for an explicit path.
fn one_path(document: &JsonDocument, path: &JsonPath) -> Result<Option<String>> {
    Ok(document.extract(path)?.into_iter().next().flatten())
}
