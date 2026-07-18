from datetime import datetime, timedelta, timezone
import unittest

from stock_state import (
    DecisionKind,
    EvidenceSource,
    SellerPolicy,
    StockEvidence,
    StockState,
    StockStateRecord,
    advance_state,
    classify_batch,
    classify_full_product,
)


BASE = datetime(2026, 7, 18, tzinfo=timezone.utc)
INTERVAL = timedelta(seconds=5)
POLICY = SellerPolicy()


def evidence(sequence, *, source=EvidenceSource.FULL_PRODUCT, seconds=0, **kwargs):
    response_complete = kwargs.pop("response_complete", True)
    return StockEvidence(
        scope_key="US:B000000001:amazon-only",
        sequence=sequence,
        observed_at=BASE + timedelta(seconds=seconds),
        source=source,
        response_complete=response_complete,
        **kwargs,
    )


def step(record, item):
    return advance_state(record, item, POLICY, poll_interval=INTERVAL)


class ClassificationTests(unittest.TestCase):
    def test_batch_buyable_is_never_confirmed(self):
        result = classify_batch(evidence(1, source=EvidenceSource.BATCH, offer_id="offer"), POLICY)
        self.assertEqual(result.state, StockState.BUYABLE_UNCONFIRMED)

    def test_batch_missing_offer_is_unknown(self):
        result = classify_batch(evidence(1, source=EvidenceSource.BATCH), POLICY)
        self.assertEqual(result.state, StockState.UNKNOWN)
        self.assertFalse(result.strong_out_of_stock)

    def test_batch_explicit_null_offer_and_oos_is_strong(self):
        result = classify_batch(
            evidence(
                1,
                source=EvidenceSource.BATCH,
                offer_explicitly_null=True,
                availability_status="OUT_OF_STOCK",
            ),
            POLICY,
        )
        self.assertEqual(result.state, StockState.OUT_OF_STOCK_CANDIDATE)
        self.assertTrue(result.strong_out_of_stock)

    def test_batch_contradiction_is_unknown(self):
        result = classify_batch(
            evidence(
                1,
                source=EvidenceSource.BATCH,
                offer_id="offer",
                offer_explicitly_null=False,
                availability_status="OUT_OF_STOCK",
            ),
            POLICY,
        )
        self.assertEqual(result.state, StockState.UNKNOWN)

    def test_incomplete_response_is_unknown_not_oos(self):
        item = evidence(1, response_complete=False)
        self.assertEqual(classify_full_product(item, POLICY).state, StockState.UNKNOWN)

    def test_full_explicit_oos_is_strong(self):
        result = classify_full_product(evidence(1, availability_status="OUT_OF_STOCK"), POLICY)
        self.assertEqual(result.state, StockState.OUT_OF_STOCK)
        self.assertTrue(result.strong_out_of_stock)

    def test_conflicting_buyable_and_oos_is_unknown(self):
        result = classify_full_product(evidence(1, offer_id="offer", availability_status="OUT_OF_STOCK"), POLICY)
        self.assertEqual(result.state, StockState.UNKNOWN)

    def test_qualified_and_suppressed_seller_classification(self):
        self.assertEqual(
            classify_full_product(evidence(1, offer_id="x", sold_by_amazon=True), POLICY).state,
            StockState.IN_STOCK_CONFIRMED,
        )
        self.assertEqual(
            classify_full_product(evidence(1, offer_id="x", sold_by_amazon=False, seller_name="Other"), POLICY).state,
            StockState.SUPPRESSED,
        )
        self.assertEqual(
            classify_full_product(evidence(1, offer_id="x"), POLICY).state,
            StockState.BUYABLE_UNCONFIRMED,
        )

    def test_evidence_and_policy_hashes_are_stable_and_sensitive(self):
        first = evidence(1, offer_id="x", sold_by_amazon=True)
        same_payload = evidence(2, seconds=10, offer_id="x", sold_by_amazon=True)
        self.assertEqual(first.fingerprint, same_payload.fingerprint)
        self.assertNotEqual(first.fingerprint, evidence(3, offer_id="y", sold_by_amazon=True).fingerprint)
        self.assertNotEqual(SellerPolicy().fingerprint, SellerPolicy(require_amazon_seller=False).fingerprint)


class StateMachineTests(unittest.TestCase):
    def test_first_observation_primes_without_event(self):
        decision = step(StockStateRecord("US:B000000001:amazon-only"), evidence(1, offer_id="x", sold_by_amazon=True))
        self.assertEqual(decision.kind, DecisionKind.PRIMED)
        self.assertIsNone(decision.event)
        self.assertEqual(decision.next_record.state, StockState.IN_STOCK_CONFIRMED)

    def test_stale_sequence_is_rejected_without_mutation(self):
        record = StockStateRecord("US:B000000001:amazon-only", initialized=True, last_sequence=4, state=StockState.OUT_OF_STOCK)
        decision = step(record, evidence(4, offer_id="x", sold_by_amazon=True))
        self.assertEqual(decision.kind, DecisionKind.STALE_REJECTED)
        self.assertEqual(decision.next_record, record)

    def test_stale_observation_time_is_rejected_without_mutation(self):
        record = StockStateRecord(
            "US:B000000001:amazon-only",
            initialized=True,
            last_sequence=4,
            last_observed_at=BASE + timedelta(seconds=10),
            state=StockState.OUT_OF_STOCK,
        )
        decision = step(
            record,
            evidence(
                5,
                seconds=5,
                offer_id="x",
                sold_by_amazon=True,
            ),
        )
        self.assertEqual(decision.kind, DecisionKind.STALE_REJECTED)
        self.assertEqual(decision.next_record, record)

    def test_two_strong_oos_must_be_separated_by_poll_interval(self):
        record = step(StockStateRecord("US:B000000001:amazon-only"), evidence(1, availability_status="OUT_OF_STOCK")).next_record
        self.assertEqual(record.state, StockState.OUT_OF_STOCK_CANDIDATE)
        # Priming suppresses an alert but still records the first OOS proof.
        # A too-close poll does not count as a second proof.
        close = step(record, evidence(2, seconds=1, availability_status="OUT_OF_STOCK"))
        self.assertEqual(close.kind, DecisionKind.OUT_OF_STOCK_PENDING)
        self.assertEqual(close.next_record.strong_oos_count, 1)
        confirmed = step(close.next_record, evidence(3, seconds=6, availability_status="OUT_OF_STOCK"))
        self.assertEqual(confirmed.next_record.state, StockState.OUT_OF_STOCK)
        self.assertTrue(confirmed.next_record.armed_for_restock)

    def test_oos_to_qualified_opens_exactly_one_epoch(self):
        record = StockStateRecord("US:B000000001:amazon-only", initialized=True, state=StockState.OUT_OF_STOCK, last_sequence=1, armed_for_restock=True)
        opened = step(record, evidence(2, offer_id="x", sold_by_amazon=True))
        self.assertEqual(opened.kind, DecisionKind.TRANSITION_OPENED)
        self.assertEqual(opened.event.epoch, 1)
        duplicate = step(opened.next_record, evidence(3, seconds=5, offer_id="x", sold_by_amazon=True))
        self.assertEqual(duplicate.kind, DecisionKind.DEDUPED)
        self.assertIsNone(duplicate.event)

    def test_batch_signal_preserves_armed_oos_baseline(self):
        record = StockStateRecord("US:B000000001:amazon-only", initialized=True, state=StockState.OUT_OF_STOCK, last_sequence=1, armed_for_restock=True)
        batch = step(record, evidence(2, source=EvidenceSource.BATCH, offer_id="x"))
        self.assertEqual(batch.next_record.state, StockState.BUYABLE_UNCONFIRMED)
        self.assertTrue(batch.next_record.armed_for_restock)
        full = step(batch.next_record, evidence(3, seconds=5, offer_id="x", sold_by_amazon=True))
        self.assertEqual(full.kind, DecisionKind.TRANSITION_OPENED)

    def test_suppressed_to_qualified_opens_epoch(self):
        record = StockStateRecord("US:B000000001:amazon-only", initialized=True, state=StockState.SUPPRESSED, last_sequence=1)
        decision = step(record, evidence(2, offer_id="x", sold_by_amazon=True))
        self.assertEqual(decision.kind, DecisionKind.TRANSITION_OPENED)
        self.assertEqual(decision.event.reason, "suppressed_to_qualified")

    def test_unknown_does_not_reset_confirmed_state(self):
        record = StockStateRecord("US:B000000001:amazon-only", initialized=True, state=StockState.IN_STOCK_CONFIRMED, last_sequence=1)
        decision = step(record, evidence(2, response_complete=False))
        self.assertEqual(decision.kind, DecisionKind.UNKNOWN_IGNORED)
        self.assertEqual(decision.next_record.state, StockState.IN_STOCK_CONFIRMED)

    def test_records_and_decisions_are_serializable(self):
        record = StockStateRecord("US:B000000001:amazon-only", initialized=True, last_sequence=1, state=StockState.OUT_OF_STOCK)
        restored = StockStateRecord.from_record(record.to_record())
        self.assertEqual(restored, record)
        decision = step(record, evidence(2, offer_id="x", sold_by_amazon=True))
        self.assertIsInstance(decision.to_record()["next_record"], dict)


if __name__ == "__main__":
    unittest.main()
