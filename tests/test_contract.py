"""Contract tests: golden fixtures validate against the v1 JSON Schemas, and
unknown fields are rejected (additionalProperties: false)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

CONTRACTS = Path(__file__).resolve().parents[1] / "contracts" / "v1"
FIXTURES = Path(__file__).resolve().parent / "fixtures"

BASE = "https://meridian.dev/contracts/v1/"


def _registry() -> Registry:
    registry = Registry()
    for path in CONTRACTS.glob("*.schema.json"):
        resource = Resource.from_contents(json.loads(path.read_text()))
        registry = resource @ registry
    return registry


REGISTRY = _registry()


def _validator(ref: str) -> Draft202012Validator:
    return Draft202012Validator({"$ref": ref}, registry=REGISTRY)


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


GOLDEN = [
    ("enroll_request.json", BASE + "enrollment.schema.json#/$defs/request"),
    ("enroll_response_approved.json", BASE + "enrollment.schema.json#/$defs/response"),
    ("enroll_response_pending.json", BASE + "enrollment.schema.json#/$defs/response"),
    ("heartbeat_request.json", BASE + "heartbeat.schema.json#/$defs/request"),
    ("heartbeat_response.json", BASE + "heartbeat.schema.json#/$defs/response"),
    ("desired_state.json", BASE + "desired-state.schema.json"),
    ("observed_state.json", BASE + "observed-state.schema.json"),
]


@pytest.mark.parametrize("fixture,ref", GOLDEN)
def test_golden_fixture_valid(fixture, ref):
    _validator(ref).validate(_load(fixture))


def test_unknown_field_rejected():
    doc = _load("heartbeat_request.json")
    doc["surprise"] = "not allowed"
    v = _validator(BASE + "heartbeat.schema.json#/$defs/request")
    assert not v.is_valid(doc)


def test_unknown_enum_rejected():
    doc = _load("desired_state.json")
    doc["assignments"][0]["engine"]["kind"] = "made-up-engine"
    v = _validator(BASE + "desired-state.schema.json")
    assert not v.is_valid(doc)


def test_bad_digest_rejected():
    doc = _load("heartbeat_request.json")
    doc["inventory_digest"] = "not-a-digest"
    v = _validator(BASE + "heartbeat.schema.json#/$defs/request")
    assert not v.is_valid(doc)
