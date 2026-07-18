"""Durable, conservative stock-state classification and transition decisions.

This module deliberately contains no database or transport code.  Callers load a
``StockStateRecord``, call ``advance_state``, atomically persist the returned
record and decision, and enqueue any returned transition event in the same
transaction.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from typing import Any, Mapping
from uuid import uuid4


class StockState(str, Enum):
    UNKNOWN = "UNKNOWN"
    OUT_OF_STOCK_CANDIDATE = "OUT_OF_STOCK_CANDIDATE"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    BUYABLE_UNCONFIRMED = "BUYABLE_UNCONFIRMED"
    IN_STOCK_CONFIRMED = "IN_STOCK_CONFIRMED"
    SUPPRESSED = "SUPPRESSED"


class EvidenceSource(str, Enum):
    BATCH = "BATCH"
    FULL_PRODUCT = "FULL_PRODUCT"


class DecisionKind(str, Enum):
    PRIMED = "PRIMED"
    STALE_REJECTED = "STALE_REJECTED"
    UNKNOWN_IGNORED = "UNKNOWN_IGNORED"
    STATE_UPDATED = "STATE_UPDATED"
    OUT_OF_STOCK_PENDING = "OUT_OF_STOCK_PENDING"
    TRANSITION_OPENED = "TRANSITION_OPENED"
    DEDUPED = "DEDUPED"


_OOS_TEXT = (
    "out_of_stock",
    "out of stock",
    "currently unavailable",
    "unavailable",
    "not available",
)


def _canonical_hash(value: Mapping[str, Any]) -> str:
    """Return a stable hash suitable for storage and idempotency diagnostics."""
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class SellerPolicy:
    """Which merchant evidence is eligible for a confirmed restock."""

    require_amazon_seller: bool = True
    allowed_seller_ids: tuple[str, ...] = ()
    allowed_seller_names: tuple[str, ...] = ()

    @property
    def fingerprint(self) -> str:
        return _canonical_hash(
            {
                "require_amazon_seller": self.require_amazon_seller,
                "allowed_seller_ids": sorted(self.allowed_seller_ids),
                "allowed_seller_names": sorted(
                    name.casefold() for name in self.allowed_seller_names
                ),
            }
        )

    def qualifies(self, evidence: "StockEvidence") -> bool | None:
        """Return True, False, or None when seller identity is not established."""
        if not self.require_amazon_seller and not (
            self.allowed_seller_ids or self.allowed_seller_names
        ):
            return True
        if evidence.sold_by_amazon is True:
            return True
        if evidence.seller_id and evidence.seller_id in self.allowed_seller_ids:
            return True
        if evidence.seller_name and any(
            evidence.seller_name.casefold() == name.casefold()
            for name in self.allowed_seller_names
        ):
            return True
        if evidence.sold_by_amazon is False or evidence.seller_id or evidence.seller_name:
            return False
        return None


@dataclass(frozen=True)
class StockEvidence:
    """Normalized raw evidence from one monotonic poll sequence.

    ``response_complete`` must be False for top-level errors, partial responses,
    malformed payloads, or a missing requested ASIN.  Such evidence is ignored
    rather than treated as out of stock.
    """

    scope_key: str
    sequence: int
    observed_at: datetime
    source: EvidenceSource
    response_complete: bool
    offer_id: str | None = None
    offer_explicitly_null: bool | None = None
    buyable_signals: tuple[str, ...] = ()
    availability_condition: str | None = None
    availability_status: str | None = None
    primary_message: str | None = None
    secondary_message: str | None = None
    sold_by_amazon: bool | None = None
    seller_id: str | None = None
    seller_name: str | None = None
    price: str | None = None

    def __post_init__(self) -> None:
        if not self.scope_key:
            raise ValueError("scope_key is required")
        if self.sequence < 0:
            raise ValueError("sequence must be non-negative")
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must be timezone-aware")

    @property
    def fingerprint(self) -> str:
        # Sequence and wall-clock time identify the observation, not its content.
        return _canonical_hash(
            {
                "scope_key": self.scope_key,
                "source": self.source.value,
                "response_complete": self.response_complete,
                "offer_id": self.offer_id,
                "offer_explicitly_null": self.offer_explicitly_null,
                "buyable_signals": sorted(self.buyable_signals),
                "availability_condition": self.availability_condition,
                "availability_status": self.availability_status,
                "primary_message": self.primary_message,
                "secondary_message": self.secondary_message,
                "sold_by_amazon": self.sold_by_amazon,
                "seller_id": self.seller_id,
                "seller_name": self.seller_name,
                "price": self.price,
            }
        )


@dataclass(frozen=True)
class Classification:
    state: StockState
    reason: str
    evidence_hash: str
    seller_policy_hash: str
    strong_out_of_stock: bool = False


def _availability_text(evidence: StockEvidence) -> str:
    return " ".join(
        part for part in (
            evidence.availability_condition,
            evidence.availability_status,
            evidence.primary_message,
            evidence.secondary_message,
        ) if isinstance(part, str) and part
    ).casefold()


def _explicitly_out_of_stock(evidence: StockEvidence) -> bool:
    text = _availability_text(evidence)
    return any(pattern in text for pattern in _OOS_TEXT)


def _has_buyable_signal(evidence: StockEvidence) -> bool:
    return bool(evidence.offer_id) or bool(evidence.buyable_signals)


def classify_batch(evidence: StockEvidence, policy: SellerPolicy) -> Classification:
    """Classify a bulk endpoint result without elevating it to alert-ready stock."""
    if evidence.source is not EvidenceSource.BATCH:
        raise ValueError("classify_batch requires BATCH evidence")
    base = dict(evidence_hash=evidence.fingerprint, seller_policy_hash=policy.fingerprint)
    if not evidence.response_complete:
        return Classification(StockState.UNKNOWN, "incomplete_batch_response", **base)
    out_of_stock = _explicitly_out_of_stock(evidence)
    buyable = _has_buyable_signal(evidence)
    if out_of_stock and buyable:
        return Classification(
            StockState.UNKNOWN,
            "contradictory_batch_buyable_and_oos",
            **base,
        )
    if out_of_stock and evidence.offer_explicitly_null is True:
        return Classification(
            StockState.OUT_OF_STOCK_CANDIDATE,
            "batch_explicit_oos",
            strong_out_of_stock=True,
            **base,
        )
    if out_of_stock:
        return Classification(
            StockState.UNKNOWN,
            "batch_oos_without_explicit_null_offer",
            **base,
        )
    if buyable:
        return Classification(StockState.BUYABLE_UNCONFIRMED, "batch_buyable", **base)
    return Classification(
        StockState.UNKNOWN,
        "batch_missing_offer_or_oos_evidence",
        **base,
    )


def classify_full_product(evidence: StockEvidence, policy: SellerPolicy) -> Classification:
    """Conservatively classify an individually fetched TVSS product response."""
    if evidence.source is not EvidenceSource.FULL_PRODUCT:
        raise ValueError("classify_full_product requires FULL_PRODUCT evidence")
    base = dict(evidence_hash=evidence.fingerprint, seller_policy_hash=policy.fingerprint)
    if not evidence.response_complete:
        return Classification(StockState.UNKNOWN, "incomplete_full_response", **base)
    buyable = _has_buyable_signal(evidence)
    if _explicitly_out_of_stock(evidence):
        if buyable:
            return Classification(StockState.UNKNOWN, "contradictory_buyable_and_oos", **base)
        return Classification(
            StockState.OUT_OF_STOCK,
            "full_explicit_oos",
            strong_out_of_stock=True,
            **base,
        )
    if not buyable:
        return Classification(StockState.UNKNOWN, "no_buyable_or_explicit_oos_signal", **base)
    qualified = policy.qualifies(evidence)
    if qualified is True:
        return Classification(StockState.IN_STOCK_CONFIRMED, "buyable_policy_qualified", **base)
    if qualified is False:
        return Classification(StockState.SUPPRESSED, "buyable_policy_suppressed", **base)
    return Classification(StockState.BUYABLE_UNCONFIRMED, "buyable_seller_unverified", **base)


def classify(evidence: StockEvidence, policy: SellerPolicy) -> Classification:
    if evidence.source is EvidenceSource.BATCH:
        return classify_batch(evidence, policy)
    return classify_full_product(evidence, policy)


@dataclass(frozen=True)
class TransitionEvent:
    transition_id: str
    scope_key: str
    epoch: int
    opened_at: datetime
    reason: str
    evidence_hash: str
    seller_policy_hash: str

    def to_record(self) -> dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "scope_key": self.scope_key,
            "epoch": self.epoch,
            "opened_at": self.opened_at.isoformat(),
            "reason": self.reason,
            "evidence_hash": self.evidence_hash,
            "seller_policy_hash": self.seller_policy_hash,
        }


@dataclass(frozen=True)
class StockStateRecord:
    """Serializable durable state for a monitoring scope."""

    scope_key: str
    state: StockState = StockState.UNKNOWN
    last_sequence: int = -1
    last_observed_at: datetime | None = None
    last_evidence_hash: str | None = None
    seller_policy_hash: str | None = None
    strong_oos_count: int = 0
    last_strong_oos_at: datetime | None = None
    epoch: int = 0
    armed_for_restock: bool = False
    initialized: bool = False

    def to_record(self) -> dict[str, Any]:
        result = asdict(self)
        result["state"] = self.state.value
        result["last_observed_at"] = (
            self.last_observed_at.isoformat() if self.last_observed_at else None
        )
        result["last_strong_oos_at"] = (
            self.last_strong_oos_at.isoformat() if self.last_strong_oos_at else None
        )
        return result

    @classmethod
    def from_record(cls, record: Mapping[str, Any]) -> "StockStateRecord":
        def parse_time(value: str | None) -> datetime | None:
            return datetime.fromisoformat(value) if value else None

        return cls(
            scope_key=record["scope_key"],
            state=StockState(record.get("state", StockState.UNKNOWN)),
            last_sequence=int(record.get("last_sequence", -1)),
            last_observed_at=parse_time(record.get("last_observed_at")),
            last_evidence_hash=record.get("last_evidence_hash"),
            seller_policy_hash=record.get("seller_policy_hash"),
            strong_oos_count=int(record.get("strong_oos_count", 0)),
            last_strong_oos_at=parse_time(record.get("last_strong_oos_at")),
            epoch=int(record.get("epoch", 0)),
            armed_for_restock=bool(record.get("armed_for_restock", False)),
            initialized=bool(record.get("initialized", False)),
        )


@dataclass(frozen=True)
class TransitionDecision:
    kind: DecisionKind
    accepted: bool
    previous_state: StockState
    next_record: StockStateRecord
    classification: Classification
    reason: str
    event: TransitionEvent | None = None

    def to_record(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "accepted": self.accepted,
            "previous_state": self.previous_state.value,
            "next_record": self.next_record.to_record(),
            "classification": {
                "state": self.classification.state.value,
                "reason": self.classification.reason,
                "evidence_hash": self.classification.evidence_hash,
                "seller_policy_hash": self.classification.seller_policy_hash,
                "strong_out_of_stock": self.classification.strong_out_of_stock,
            },
            "reason": self.reason,
            "event": self.event.to_record() if self.event else None,
        }


def _with_observation(
    record: StockStateRecord, evidence: StockEvidence, classification: Classification, **changes: Any
) -> StockStateRecord:
    values = {
        "scope_key": record.scope_key,
        "state": record.state,
        "last_sequence": evidence.sequence,
        "last_observed_at": evidence.observed_at,
        "last_evidence_hash": classification.evidence_hash,
        "seller_policy_hash": classification.seller_policy_hash,
        "strong_oos_count": record.strong_oos_count,
        "last_strong_oos_at": record.last_strong_oos_at,
        "epoch": record.epoch,
        "armed_for_restock": record.armed_for_restock,
        "initialized": True,
    }
    values.update(changes)
    return StockStateRecord(**values)


def _open_event(
    record: StockStateRecord, evidence: StockEvidence, classification: Classification, reason: str
) -> tuple[StockStateRecord, TransitionEvent]:
    epoch = record.epoch + 1
    next_record = _with_observation(
        record,
        evidence,
        classification,
        state=StockState.IN_STOCK_CONFIRMED,
        epoch=epoch,
        armed_for_restock=False,
        strong_oos_count=0,
        last_strong_oos_at=None,
    )
    return next_record, TransitionEvent(
        transition_id=str(uuid4()),
        scope_key=record.scope_key,
        epoch=epoch,
        opened_at=evidence.observed_at,
        reason=reason,
        evidence_hash=classification.evidence_hash,
        seller_policy_hash=classification.seller_policy_hash,
    )


def advance_state(
    record: StockStateRecord,
    evidence: StockEvidence,
    policy: SellerPolicy,
    *,
    poll_interval: timedelta,
    oos_rearm_count: int = 2,
) -> TransitionDecision:
    """Classify one observation and produce its durable state transition.

    The caller must atomically compare/persist ``last_sequence`` (or a separate
    row version) with the returned record.  A transition event is emitted only
    for a qualified post-OOS restock or a suppressed-to-qualified seller change.
    """
    if record.scope_key != evidence.scope_key:
        raise ValueError("record and evidence scope_key must match")
    if poll_interval <= timedelta(0):
        raise ValueError("poll_interval must be positive")
    if oos_rearm_count < 2:
        raise ValueError("oos_rearm_count must be at least two")
    classification = classify(evidence, policy)
    previous = record.state
    if (
        evidence.sequence <= record.last_sequence
        or (
            record.last_observed_at is not None
            and evidence.observed_at < record.last_observed_at
        )
    ):
        return TransitionDecision(
            DecisionKind.STALE_REJECTED, False, previous, record, classification,
            "sequence_or_observation_time_stale",
        )
    if not record.initialized:
        strong_count = 1 if classification.strong_out_of_stock else 0
        primed = _with_observation(
            record,
            evidence,
            classification,
            state=(
                StockState.OUT_OF_STOCK_CANDIDATE
                if classification.strong_out_of_stock
                else classification.state
            ),
            strong_oos_count=strong_count,
            last_strong_oos_at=(
                evidence.observed_at if classification.strong_out_of_stock else None
            ),
        )
        return TransitionDecision(DecisionKind.PRIMED, True, previous, primed, classification, "first_observation")
    if classification.state is StockState.UNKNOWN:
        # Sequence advances so a malformed delayed response cannot later win;
        # stock state and OOS evidence remain untouched.
        next_record = _with_observation(record, evidence, classification)
        return TransitionDecision(DecisionKind.UNKNOWN_IGNORED, True, previous, next_record, classification, classification.reason)

    if classification.strong_out_of_stock:
        separated = (
            record.last_strong_oos_at is None
            or evidence.observed_at - record.last_strong_oos_at >= poll_interval
        )
        count = record.strong_oos_count + 1 if separated else record.strong_oos_count
        if count >= oos_rearm_count:
            next_record = _with_observation(
                record, evidence, classification, state=StockState.OUT_OF_STOCK,
                strong_oos_count=count, last_strong_oos_at=evidence.observed_at,
                armed_for_restock=True,
            )
            return TransitionDecision(DecisionKind.STATE_UPDATED, True, previous, next_record, classification, "strong_oos_confirmed")
        next_record = _with_observation(
            record, evidence, classification, state=StockState.OUT_OF_STOCK_CANDIDATE,
            strong_oos_count=count, last_strong_oos_at=evidence.observed_at if separated else record.last_strong_oos_at,
        )
        return TransitionDecision(DecisionKind.OUT_OF_STOCK_PENDING, True, previous, next_record, classification, "awaiting_second_strong_oos")

    if classification.state is StockState.IN_STOCK_CONFIRMED:
        if record.armed_for_restock:
            next_record, event = _open_event(record, evidence, classification, "out_of_stock_to_qualified")
            return TransitionDecision(DecisionKind.TRANSITION_OPENED, True, previous, next_record, classification, event.reason, event)
        if previous is StockState.SUPPRESSED:
            next_record, event = _open_event(record, evidence, classification, "suppressed_to_qualified")
            return TransitionDecision(DecisionKind.TRANSITION_OPENED, True, previous, next_record, classification, event.reason, event)
        next_record = _with_observation(
            record, evidence, classification, state=StockState.IN_STOCK_CONFIRMED,
            strong_oos_count=0, last_strong_oos_at=None, armed_for_restock=False,
        )
        return TransitionDecision(DecisionKind.DEDUPED, True, previous, next_record, classification, "continuous_qualified_buyability")

    # A batch offer cannot downgrade a confirmed or policy-suppressed state.
    # Preserve those states until a full product observation establishes a
    # different seller-qualified result.
    if (
        evidence.source is EvidenceSource.BATCH
        and classification.state is StockState.BUYABLE_UNCONFIRMED
        and previous in (StockState.IN_STOCK_CONFIRMED, StockState.SUPPRESSED)
    ):
        next_record = _with_observation(
            record,
            evidence,
            classification,
            state=previous,
            strong_oos_count=0,
            last_strong_oos_at=None,
            armed_for_restock=record.armed_for_restock,
        )
        return TransitionDecision(
            DecisionKind.DEDUPED,
            True,
            previous,
            next_record,
            classification,
            "batch_offer_preserves_durable_state",
        )

    # Batch buyability and policy suppression are useful current-state evidence,
    # but cannot on their own open a confirmed restock event.
    next_record = _with_observation(
        record, evidence, classification, state=classification.state,
        strong_oos_count=0, last_strong_oos_at=None,
        armed_for_restock=False if classification.state is StockState.SUPPRESSED else record.armed_for_restock,
    )
    return TransitionDecision(DecisionKind.STATE_UPDATED, True, previous, next_record, classification, classification.reason)


def utc_now() -> datetime:
    """Small injectable-friendly default for callers constructing evidence."""
    return datetime.now(timezone.utc)
