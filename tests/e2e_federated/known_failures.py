"""The distributions the engine currently gets wrong, and why.

Each entry names one (case, distribution) that fails today. The test still RUNS -
it is marked xfail, not skipped - so the defect stays exercised and stays visible
in the report instead of disappearing from the run.

Every entry is STRICT: if the engine starts returning the right answer the run
FAILS, which forces the entry to be deleted in the same change that fixes the
defect. That is what stops this file becoming a list of breakage nobody
remembers.

Strictness is only possible because the corpus plans without learned statistics
(see the engine fixture). With learned statistics on, a learned column NDV can
change the join order so that a broken plan stops being chosen; a verdict would
then depend on what ran before it and could be asserted neither as failing nor
as passing.

There are no entries. The registry stays because a strict registry is how the
next confirmed defect is held: an entry here keeps its distributions running and
deletes itself the moment the engine is right again.
"""


def _entries():
    """Return (case name, distribution name) -> reason for every known failure."""
    return {}


KNOWN_FAILURES = _entries()


def lookup(case_name, distribution_name):
    """Return the reason this distribution is a known failure, or None."""
    return KNOWN_FAILURES.get((case_name, distribution_name))
