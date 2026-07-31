"""Engine assignment state machine (DESIGN.md 9.2).

Transition legality is enforced here so no other module can move an engine into
an illegal phase. The reconciler asks `can_transition` / `next_on_event`; it
never assigns a phase directly.
"""

from __future__ import annotations

from .models import EnginePhase as P

# Legal transitions from DESIGN.md 9.2, plus Orphaned as a discovery terminal
# that only an operator or a matching adoption can leave.
_LEGAL: dict[P, frozenset[P]] = {
    P.PENDING: frozenset({P.PULLING, P.STARTING, P.FAILED}),
    P.PULLING: frozenset({P.STARTING, P.FAILED}),
    P.STARTING: frozenset({P.WARMING, P.FAILED}),
    P.WARMING: frozenset({P.READY, P.FAILED}),
    P.READY: frozenset({P.DRAINING, P.FAILED}),
    P.DRAINING: frozenset({P.STOPPED, P.FAILED}),
    P.FAILED: frozenset({P.PENDING, P.QUARANTINED}),
    P.QUARANTINED: frozenset({P.PENDING}),
    P.STOPPED: frozenset({P.PENDING}),
    P.ORPHANED: frozenset({P.READY, P.STOPPED}),
}


class IllegalTransition(ValueError):
    pass


def can_transition(current: P, target: P) -> bool:
    if current == target:
        return True
    return target in _LEGAL.get(current, frozenset())


def assert_transition(current: P, target: P) -> None:
    if not can_transition(current, target):
        raise IllegalTransition(f"{current.value} -> {target.value} is not a legal engine transition")


def is_terminal(phase: P) -> bool:
    return phase == P.STOPPED
