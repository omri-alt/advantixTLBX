#!/usr/bin/env python3
"""Unit tests for Quality Flow 2 share planning."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from integrations.keitaro_feed_balance import plan_unmonetized_feed_shares


class PlanUnmonetizedFeedSharesTests(unittest.TestCase):
    def test_enabled_no_never_rewrites(self) -> None:
        plan = plan_unmonetized_feed_shares(
            enabled="no",
            slots=[(1, 70, "feed1"), (2, 30, "feed5"), (3, 0, "feed3")],
            found_by_feed={"feed1": True, "feed5": True, "feed3": False},
        )
        self.assertEqual(plan.action, "skip_disabled")
        self.assertFalse(plan.weights)

    def test_does_not_revive_operator_zeros(self) -> None:
        plan = plan_unmonetized_feed_shares(
            enabled="yes",
            slots=[(1, 60, "feed1"), (2, 40, "feed5"), (3, 0, "feed3"), (4, 0, "feed4")],
            found_by_feed={"feed1": True, "feed5": True, "feed3": True, "feed4": False},
        )
        self.assertEqual(plan.action, "skip_already_zeroed")
        self.assertFalse(plan.weights)

    def test_demote_positive_unmon_keeps_active_ratio(self) -> None:
        plan = plan_unmonetized_feed_shares(
            enabled="yes",
            slots=[(1, 50, "feed1"), (2, 30, "feed5"), (3, 20, "feed4"), (4, 0, "feed3")],
            found_by_feed={"feed1": True, "feed5": True, "feed4": False, "feed3": True},
        )
        self.assertEqual(plan.action, "apply")
        self.assertEqual(plan.weights, {1: 50.0, 2: 30.0})
        self.assertIn(3, plan.zeros)
        self.assertIn(4, plan.zeros)
        self.assertNotIn(3, plan.weights)

    def test_fallback_equal_when_all_active_are_dead(self) -> None:
        plan = plan_unmonetized_feed_shares(
            enabled="yes",
            slots=[(1, 100, "feed1"), (2, 0, "feed5"), (3, 0, "feed3")],
            found_by_feed={"feed1": False, "feed5": True, "feed3": True},
        )
        self.assertEqual(plan.action, "apply")
        self.assertEqual(plan.reason, "fallback_equal_live")
        self.assertEqual(plan.weights, {2: 1.0, 3: 1.0})
        self.assertEqual(plan.zeros, [1])

    def test_keeps_unmapped_offers_on_payload(self) -> None:
        plan = plan_unmonetized_feed_shares(
            enabled="yes",
            slots=[(1, 80, "feed1"), (2, 20, "feed4"), (9, 0, None)],
            found_by_feed={"feed1": True, "feed4": False},
        )
        self.assertEqual(plan.action, "apply")
        self.assertEqual(plan.weights, {1: 80.0})
        self.assertIn(2, plan.zeros)
        self.assertIn(9, plan.zeros)

    def test_all_unmon_pauses_sk(self) -> None:
        plan = plan_unmonetized_feed_shares(
            enabled="yes",
            slots=[(1, 50, "feed1"), (2, 50, "feed4")],
            found_by_feed={"feed1": False, "feed4": False},
        )
        self.assertEqual(plan.action, "all_unmon_pause_sk")


if __name__ == "__main__":
    raise SystemExit(unittest.main())
