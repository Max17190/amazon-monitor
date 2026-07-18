import unittest
from uuid import uuid4

from durable_store import (
    OUTBOX_NOTIFICATION_DELIVERY_ID_LIMIT,
    _outbox_notification_payload,
)


class OutboxNotificationPayloadTests(unittest.TestCase):
    def test_payload_is_bounded_and_preserves_priority_order(self):
        delivery_ids = [uuid4() for _ in range(
            OUTBOX_NOTIFICATION_DELIVERY_ID_LIMIT + 1
        )]

        payload = _outbox_notification_payload(delivery_ids)

        self.assertLess(len(payload.encode("utf-8")), 8_000)
        self.assertEqual(
            payload,
            '{"delivery_ids":['
            + ",".join(
                f'"{delivery_id}"'
                for delivery_id in delivery_ids[
                    :OUTBOX_NOTIFICATION_DELIVERY_ID_LIMIT
                ]
            )
            + "]}",
        )
