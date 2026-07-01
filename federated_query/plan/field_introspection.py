"""Annotation-driven field introspection for plan/state models.

The single source of truth for "which of a model's fields hold Expression
values". Walkers that need every expression attached to a node derive it from
the field type annotations instead of a hand-maintained per-node list, so a new
expression-typed field is covered automatically and a field whose type cannot be
classified (e.g. ``Any``) RAISES rather than being silently assumed
expression-free. That raise is the structural guard: incompleteness is loud, not
silent.
"""

from typing import Any, List, get_args, get_origin

from .expressions import Expression


class FieldIntrospectionError(Exception):
    """Raised when a model field's type annotation cannot be classified."""


def innermost_type(annotation: Any) -> Any:
    """Peel Optional / List / Tuple wrappers down to the single inner type.

    ``Optional[List[Expression]]`` -> ``Expression``. A union with more than one
    non-None member (or any other multi-argument shape) is ambiguous and raises,
    so an annotation this code cannot reason about never slips through.
    """
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    inner_args = []
    for arg in get_args(annotation):
        if arg is type(None) or arg is Ellipsis:
            continue
        inner_args.append(arg)
    if len(inner_args) != 1:
        raise FieldIntrospectionError(
            f"cannot classify annotation {annotation!r}: expected exactly one "
            f"inner type after peeling Optional/List, got {inner_args}"
        )
    return innermost_type(inner_args[0])


def field_holds_expressions(annotation: Any) -> bool:
    """Whether a field's declared type holds Expression values.

    True for an Expression-typed field (through Optional/List/nested List),
    False for a field whose inner type is some other concrete class (a child
    plan node, a string, an enum). Raises when the inner type is ``Any`` or
    otherwise not a concrete class, because then we cannot tell whether it hides
    expressions and must not guess.
    """
    inner = innermost_type(annotation)
    if inner is Any:
        raise FieldIntrospectionError(
            "field annotation resolves to Any; its expression content cannot be "
            "determined - type it concretely or handle it explicitly"
        )
    if not isinstance(inner, type):
        raise FieldIntrospectionError(
            f"field annotation {annotation!r} resolves to a non-class type "
            f"{inner!r} that cannot be classified"
        )
    return issubclass(inner, Expression)


def flatten_expression_values(value: Any) -> List[Expression]:
    """Collect every Expression held in a field value (through lists/None)."""
    if value is None:
        return []
    if isinstance(value, Expression):
        return [value]
    if isinstance(value, (list, tuple)):
        collected: List[Expression] = []
        for item in value:
            collected.extend(flatten_expression_values(item))
        return collected
    raise FieldIntrospectionError(
        f"expression-typed field holds a non-expression value of type "
        f"{type(value).__name__}"
    )


def model_expression_values(model: Any) -> List[Expression]:
    """Every Expression attached directly to a model's fields.

    Decided by the field annotations, so a node never silently omits an
    expression it carries and a field with an unclassifiable type raises.
    """
    collected: List[Expression] = []
    for field_name, info in type(model).model_fields.items():
        if field_holds_expressions(info.annotation):
            collected.extend(flatten_expression_values(getattr(model, field_name)))
    return collected
