"""In-process Transport adapter over the shared ControlCore.

The safety rules live in `meridian_node.refcontrol.ControlCore`; this is a thin
adapter that satisfies the agent's Transport contract (which does not pass the
enrollment token as an argument, hence `presenting()`), plus the operator-style
test hooks. It exists so tests and the dev server exercise one implementation of
the fencing/enrollment logic, not two.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional

from meridian_node.control import ControlError
from meridian_node.refcontrol import ControlCore
from meridian_node.refcontrol import Token as Token  # re-export for existing imports


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _b64url_decode(s: str) -> bytes:
    import base64

    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


class FakeControlPlane:
    def __init__(
        self,
        tokens: dict[str, Token],
        now: Callable[[], datetime] = _utcnow,
        lease_ttl_seconds: int = 30,
    ):
        self.core = ControlCore(tokens, now=now, lease_ttl_seconds=lease_ttl_seconds)
        self._enroll_token: Optional[str] = None

    # Test convenience: the in-process Transport.enroll(request) carries no token,
    # so tests select it here (the HTTP server reads it from the auth header).
    def presenting(self, token: str) -> "FakeControlPlane":
        self._enroll_token = token
        return self

    # Expose core collections used by existing tests.
    @property
    def nodes(self):
        return self.core.nodes

    @property
    def observations(self):
        return self.core.observations

    @property
    def stop_authorized(self):
        return self.core.stop_authorized

    def _wrap(self, fn, *args):
        try:
            return fn(*args)
        except ControlError:
            raise
        except Exception as e:  # translate core ControlError (distinct class) to the client one
            from meridian_node.refcontrol import ControlError as CoreError

            if isinstance(e, CoreError):
                raise ControlError(e.code, e.message, retryable=e.retryable) from e
            raise

    # --- Transport contract --------------------------------------------
    def enroll(self, request: dict) -> dict:
        return self._wrap(self.core.enroll, self._enroll_token, request)

    def get_claim(self, claim_id: str, signed_nonce_provider: Callable[[bytes], str]) -> dict:
        nonce = self._wrap(self.core.claim_nonce, claim_id)  # hex string
        signature = _b64url_decode(signed_nonce_provider(nonce.encode()))
        return self._wrap(self.core.resolve_claim, claim_id, signature)

    def establish_session(self, node_id: str, request: dict) -> dict:
        return self._wrap(self.core.establish_session, node_id, request)

    def heartbeat(self, node_id: str, request: dict) -> dict:
        return self._wrap(self.core.heartbeat, node_id, request)

    def fetch_desired_state(self, node_id: str, generation: int) -> dict:
        return self._wrap(self.core.fetch_desired_state, node_id, generation)

    def post_observation(self, node_id: str, observation: dict) -> dict:
        return self._wrap(self.core.post_observation, node_id, observation)

    # --- operator/test hooks -------------------------------------------
    def approve_claim(self, claim_id: str) -> None:
        self.core.approve_claim(claim_id)

    def deny_claim(self, claim_id: str) -> None:
        self.core.deny_claim(claim_id)

    def set_desired(self, node_id: str, snapshot: dict) -> None:
        self.core.set_desired(node_id, snapshot)
