//! A typed expansion boundary lowering to DataFusion's native physical UNNEST.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::{Column, DFSchema, DFSchemaRef, DataFusionError, Result};
use datafusion::dataframe::DataFrame;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::{
    Extension, LogicalPlan, Unnest, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::Expr;

/// Keep list input names distinct from element outputs across optimizer rewrites.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd)]
struct ListExpansion {
    native: Unnest,
    output_names: Vec<String>,
}

impl ListExpansion {
    /// Derive native list indices and replace only the resulting output names.
    fn build(input: LogicalPlan, columns: Vec<Column>, output_names: Vec<String>) -> Result<Self> {
        let options = datafusion::common::UnnestOptions::new().with_preserve_nulls(false);
        let expanded = datafusion::logical_expr::logical_plan::builder::unnest_with_options(
            input, columns, options,
        )?;
        let LogicalPlan::Unnest(mut native) = expanded else {
            return Err(DataFusionError::Internal(
                "list expansion did not produce UNNEST".to_string(),
            ));
        };
        native.schema = rename_schema(&native, &output_names)?;
        Ok(Self {
            native,
            output_names,
        })
    }
}

impl UserDefinedLogicalNodeCore for ListExpansion {
    /// Name this operator in coordinator plans.
    fn name(&self) -> &'static str {
        "FedqListExpansion"
    }
    /// Expose the prepared list inputs to recursive plan rewrites.
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.native.input]
    }
    /// Return the complete native element schema with its physical output names.
    fn schema(&self) -> &DFSchemaRef {
        &self.native.schema
    }
    /// Retain each list input as an explicit dependency of the expansion.
    fn expressions(&self) -> Vec<Expr> {
        self.native
            .exec_columns
            .iter()
            .cloned()
            .map(Expr::Column)
            .collect()
    }
    /// Describe the expansion without evaluating its input.
    fn fmt_for_explain(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "FedqListExpansion: {:?}", self.output_names)
    }
    /// Rebuild metadata from rewritten inputs and preserve every declared output.
    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "list expansion requires one input".to_string(),
            ));
        }
        let mut columns = Vec::new();
        for expression in expressions {
            let Expr::Column(column) = expression else {
                return Err(DataFusionError::Plan(
                    "list expansion dependency is not a column".to_string(),
                ));
            };
            columns.push(column);
        }
        Self::build(inputs.remove(0), columns, self.output_names.clone())
    }
}

/// Preserve all field metadata while keeping the list and element namespaces distinct.
fn rename_schema(native: &Unnest, output_names: &[String]) -> Result<DFSchemaRef> {
    if native.exec_columns.len() != output_names.len() {
        return Err(DataFusionError::Plan(
            "list expansion output arity mismatch".to_string(),
        ));
    }
    let mut fields = Vec::new();
    for (qualifier, field) in native.schema.iter() {
        let field = renamed_field(native, output_names, field);
        fields.push((qualifier.cloned(), field));
    }
    Ok(Arc::new(DFSchema::new_with_metadata(
        fields,
        native.schema.metadata().clone(),
    )?))
}

/// Rename element fields while retaining unchanged parent fields and complete metadata.
fn renamed_field(
    native: &Unnest,
    output_names: &[String],
    field: &Arc<arrow_schema::Field>,
) -> Arc<arrow_schema::Field> {
    let position = native
        .exec_columns
        .iter()
        .position(|column| column.name == *field.name());
    match position {
        Some(index) => Arc::new(field.as_ref().clone().with_name(&output_names[index])),
        None => Arc::clone(field),
    }
}

/// Introduce the optimizer boundary using DataFusion's own list-schema derivation.
pub(crate) fn expand(
    frame: DataFrame,
    inputs: &[String],
    outputs: Vec<String>,
) -> Result<DataFrame> {
    let (state, logical) = frame.into_parts();
    let columns = inputs.iter().map(Column::new_unqualified).collect();
    let node = ListExpansion::build(logical, columns, outputs)?;
    Ok(DataFrame::new(
        state,
        LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }),
    ))
}

/// Select the standard planner plus the native list-expansion lowering.
#[derive(Debug)]
pub(crate) struct ExpansionQueryPlanner;

#[async_trait]
impl QueryPlanner for ExpansionQueryPlanner {
    /// Plan every standard operator normally and lower expansion through its native kernel.
    async fn create_physical_plan(
        &self,
        logical: &LogicalPlan,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(ExpansionPlanner)])
            .create_physical_plan(logical, state)
            .await
    }
}

/// Instantiate DataFusion's UNNEST execution operator from validated expansion metadata.
struct ExpansionPlanner;

#[async_trait]
impl ExtensionPlanner for ExpansionPlanner {
    /// Delegate unknown extensions and preserve native list indices for this operator.
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(expansion) = node.as_any().downcast_ref::<ListExpansion>() else {
            return Ok(None);
        };
        if physical_inputs.len() != 1 {
            return Err(DataFusionError::Plan(
                "list expansion requires one physical input".to_string(),
            ));
        }
        Ok(Some(native_execution(
            expansion,
            Arc::clone(&physical_inputs[0]),
        )?))
    }
}

/// Instantiate the native list kernel using indices derived from the current input schema.
fn native_execution(
    expansion: &ListExpansion,
    input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut lists = Vec::new();
    for (index, column) in &expansion.native.list_type_columns {
        lists.push(ListUnnest {
            index_in_input_schema: *index,
            depth: column.depth,
        });
    }
    let execution = UnnestExec::new(
        input,
        lists,
        expansion.native.struct_type_columns.clone(),
        Arc::clone(expansion.native.schema.inner()),
        expansion.native.options.clone(),
    )?;
    Ok(Arc::new(execution))
}
