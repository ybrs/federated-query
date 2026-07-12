//! End-to-end checks for the review-hardened seams: the well-scoped-plan guard
//! that runs after decorrelation, the flag path over a Union (that its
//! passthrough columns stay in scope), the loud rejection of a boolean flag over
//! an unqualifiable input, and the correlated-HAVING-aggregate raise.

use crate::common::{catalog, decorrelate_err};
use fq_decorrelate::{decorrelate, DecorrelationError};

/// Parse + bind, then decorrelate returning the raw Result (so a test can assert
/// either success or the specific error).
fn try_decorrelate(sql: &str) -> Result<fq_plan::logical::LogicalPlan, DecorrelationError> {
    let catalog = catalog();
    let plan = fq_parse::parse_with_catalog(sql, &catalog).expect("parse");
    let bound = fq_bind::bind(&catalog, plan).expect("bind");
    decorrelate(bound)
}

#[test]
fn two_flag_subqueries_nest_and_stay_in_scope() {
    // Two boolean subqueries in the SELECT list: the second flag join sits over the
    // Union the first produced. The passthrough columns above that Union must stay
    // in scope, so the post-pass scope guard must accept the result.
    let plan = try_decorrelate(
        "SELECT o.id, \
                EXISTS (SELECT 1 FROM pg.public.products p WHERE p.id = o.product_id) AS e1, \
                EXISTS (SELECT 1 FROM pg.public.companies c WHERE c.id = o.company_id) AS e2 \
         FROM pg.public.orders o",
    )
    .expect("nested flag unions decorrelate to a well-scoped plan");
    // Sanity: no subquery expression survives.
    crate::common::assert_no_subquery(&plan);
}

#[test]
fn boolean_flag_over_aggregate_input_fails_loudly() {
    // A boolean subquery in the SELECT list of a GROUP BY query: the flag join's
    // left input is an Aggregate, whose columns cannot be relation-qualified for the
    // branch passthrough. This is a deliberate loud rejection (no star, no
    // unqualified column) rather than a wrong or star-bearing plan.
    let err = decorrelate_err(
        "SELECT o.region, \
                EXISTS (SELECT 1 FROM pg.public.products p WHERE p.category = o.region) AS e \
         FROM pg.public.orders o GROUP BY o.region",
    );
    assert!(
        matches!(&err, DecorrelationError::Unsupported(_)),
        "expected a loud Unsupported, got {err:?}"
    );
}

#[test]
fn correlated_having_aggregate_raises_rather_than_mis_qualifies() {
    // A correlated HAVING that compares an inner aggregate to an outer column: the
    // hoisted aggregate output does not cross the subquery boundary, so it must
    // raise rather than emit a join condition referencing an unexposed column.
    let result = try_decorrelate(
        "SELECT o.id FROM pg.public.orders o \
         WHERE EXISTS ( \
             SELECT 1 FROM pg.public.products p \
             WHERE p.category = o.region \
             GROUP BY p.category \
             HAVING sum(p.price) > o.price)",
    );
    match result {
        Err(DecorrelationError::Unsupported(message)) => {
            assert!(
                message.contains("HAVING") || message.contains("boundary"),
                "unexpected message: {message}"
            );
        }
        other => {
            panic!("expected a loud Unsupported for correlated HAVING aggregate, got {other:?}")
        }
    }
}
