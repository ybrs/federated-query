"""The plan variants that answer a query differently from the oracle, and why.

An entry names one (variant, case) whose plan returns something the reference
does not. The variant is still RUN and still COMPARED - it is recorded, not
excused - so the defect stays exercised and stays visible in the report.

Every entry is STRICT, as in ``known_failures`` and
``known_fragment_divergences``: a registered pair that starts agreeing FAILS the
run, which forces the entry to be deleted in the change that fixes the defect.

An entry here is heavier than one in the other two registries. It says the engine
computes a different answer depending on which plan it chose for one query over
one dataset, which is the failure this suite exists to detect. It is legitimate
only while the defect is recorded and being worked, never as a way to keep a run
green.

There are currently no known disagreements: every plan every variant reaches
agrees with the oracle over the whole corpus.
"""

KNOWN_DISAGREEMENTS = {}


def lookup(variant_name, case_name, distribution_name):
    """Return the reason this variant is known to disagree here, or None."""
    return KNOWN_DISAGREEMENTS.get((variant_name, case_name, distribution_name))
