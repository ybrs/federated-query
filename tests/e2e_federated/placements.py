"""Placement strategies: how a case's tables spread across data sources.

A placement names a fixed list of source SLOTS (each a kind: ``duck``, ``pg``, or
``parquet``) and a rule for assigning the case's tables to them. Given a case's
sorted table names, ``Placement.assign`` returns the slots that actually receive
tables plus a table -> slot-letter map. The environment builder in ``runtime.py``
turns abstract slots into concrete datasources (DuckDB files, Parquet dirs,
PostgreSQL schemas) and the fully qualified names the harness substitutes into
the query.

Two assignment rules exist. ``round_robin`` deals tables cyclically over the
slots (table 0 to slot 0, table 1 to slot 1, ...), which spreads a multi-table
case across every slot. ``first_isolated`` pins the first table to slot 0 and
sends the rest to slot 1; the Parquet placements use it so the leading table is
the (read-only) Parquet source and the remainder is a writable source.
"""

DUCK = "duck"
PG = "pg"
PARQUET = "parquet"

ROUND_ROBIN = "round_robin"
FIRST_ISOLATED = "first_isolated"

_KINDS = (DUCK, PG, PARQUET)
_MODES = (ROUND_ROBIN, FIRST_ISOLATED)


class Slot:
    """One abstract source slot of a placement: a stable letter and a kind.

    The letter (``a``, ``b``, ...) makes the slot's datasource name and, for
    PostgreSQL, its schema deterministic within a placement.
    """

    def __init__(self, letter, kind):
        """Store the slot letter and kind, rejecting an unknown kind loudly."""
        if kind not in _KINDS:
            raise ValueError("unknown slot kind '" + str(kind) + "'")
        self.letter = letter
        self.kind = kind


class Placement:
    """A named table-to-source strategy over a fixed list of slots."""

    def __init__(self, name, slots, mode, min_tables=1):
        """Store the placement name, its slots, mode, and minimum table count.

        ``min_tables`` is the fewest tables a case must declare for this
        placement to run it. A placement with more slots than a case has tables
        would only fill a prefix of its slots and reproduce a shape a
        smaller-slot placement already covers, so such a case is skipped
        visibly. Two-slot and single-slot placements keep the default of 1.
        """
        if mode not in _MODES:
            raise ValueError("unknown placement mode '" + str(mode) + "'")
        self.name = name
        self.slots = slots
        self.mode = mode
        self.min_tables = min_tables

    def assign(self, table_names):
        """Return (used_slots, table -> slot-letter) for a case's tables.

        ``table_names`` is sorted for a deterministic assignment. Only slots that
        receive at least one table are returned, so a case with fewer tables than
        slots yields fewer distinct sources.
        """
        ordered = sorted(table_names)
        if self.mode == ROUND_ROBIN:
            mapping = self._assign_round_robin(ordered)
        else:
            mapping = self._assign_first_isolated(ordered)
        return self._collect_used_slots(mapping), mapping

    def _assign_round_robin(self, ordered):
        """Deal tables cyclically across the slots, returning table -> letter."""
        mapping = {}
        slot_count = len(self.slots)
        for index, table in enumerate(ordered):
            mapping[table] = self.slots[index % slot_count].letter
        return mapping

    def _assign_first_isolated(self, ordered):
        """Pin table 0 to slot 0 and the rest to slot 1, returning table->letter."""
        mapping = {}
        first_letter = self.slots[0].letter
        rest_letter = self.slots[1].letter
        for index, table in enumerate(ordered):
            mapping[table] = first_letter if index == 0 else rest_letter
        return mapping

    def _collect_used_slots(self, mapping):
        """Return the slots that received a table, in this placement's order."""
        used_letters = set(mapping.values())
        used = []
        for slot in self.slots:
            if slot.letter in used_letters:
                used.append(slot)
        return used

    def slot_by_letter(self, letter):
        """Return the slot with the given letter, raising if there is none."""
        for slot in self.slots:
            if slot.letter == letter:
                return slot
        raise KeyError("placement '" + self.name + "' has no slot '" + letter + "'")


def _build_placements():
    """Construct the ordered list of the suite's ten placement strategies."""
    placements = [
        Placement("oracle_single_duck", [Slot("a", DUCK)], ROUND_ROBIN),
        Placement("duck_duck", [Slot("a", DUCK), Slot("b", DUCK)], ROUND_ROBIN),
        Placement("pg_duck", [Slot("a", PG), Slot("b", DUCK)], ROUND_ROBIN),
        Placement("duck_pg", [Slot("a", DUCK), Slot("b", PG)], ROUND_ROBIN),
        Placement("all_pg", [Slot("a", PG), Slot("b", PG)], ROUND_ROBIN),
        Placement("parquet_duck", [Slot("a", PARQUET), Slot("b", DUCK)], FIRST_ISOLATED),
        Placement("parquet_pg", [Slot("a", PARQUET), Slot("b", PG)], FIRST_ISOLATED),
    ]
    _add_multi_source_placements(placements)
    return placements


def _add_multi_source_placements(placements):
    """Append the three- and four-source placements to the placement list.

    Each deals a case's tables round-robin over three or four slots that mix
    DuckDB, Parquet, and two distinct PostgreSQL schemas, and carries a
    ``min_tables`` equal to its slot count so a case with fewer tables (which
    would only fill a prefix of the slots) is skipped rather than run as an
    already-covered shape. ``tri_source_rev`` reverses the slot kinds of
    ``tri_source`` so the same tables land on different sources.
    """
    tri = [Slot("a", DUCK), Slot("b", PG), Slot("c", PARQUET)]
    tri_rev = [Slot("a", PARQUET), Slot("b", PG), Slot("c", DUCK)]
    quad = [Slot("a", DUCK), Slot("b", PG), Slot("c", PARQUET), Slot("d", PG)]
    placements.append(Placement("tri_source", tri, ROUND_ROBIN, min_tables=3))
    placements.append(Placement("tri_source_rev", tri_rev, ROUND_ROBIN, min_tables=3))
    placements.append(Placement("quad_source", quad, ROUND_ROBIN, min_tables=4))


PLACEMENTS = _build_placements()


def placement_uses_postgres(placement, used_slots):
    """Whether any used slot of a placement is a PostgreSQL source."""
    for slot in used_slots:
        if slot.kind == PG:
            return True
    return False
