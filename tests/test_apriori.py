from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from son_miner.apriori import apriori_partition, count_candidates, frequent_itemsets, generate_candidates
from son_miner.io import parse_partition, write_result


BASKETS = [
    {"bread", "milk"},
    {"bread", "diaper", "beer", "eggs"},
    {"milk", "diaper", "beer", "cola"},
    {"bread", "milk", "diaper", "beer"},
    {"bread", "milk", "diaper", "cola"},
]


class AprioriTest(unittest.TestCase):
    def test_candidate_join_and_subset_pruning(self) -> None:
        previous = {("a", "b"), ("a", "c"), ("b", "c"), ("a", "d")}
        self.assertEqual(generate_candidates(previous, 3), {("a", "b", "c")})

    def test_partition_candidates_and_global_count(self) -> None:
        candidates = apriori_partition(BASKETS, global_support=3, partition_basket_count=5, total_basket_count=5)
        frequent = frequent_itemsets(BASKETS, candidates, support=3)
        self.assertIn(("bread",), frequent)
        self.assertIn(("beer", "diaper"), frequent)
        self.assertIn(("bread", "milk"), frequent)
        self.assertNotIn(("beer", "bread"), frequent)

    def test_scaled_support_preserves_candidates(self) -> None:
        partition = BASKETS[:2]
        candidates = apriori_partition(partition, global_support=3, partition_basket_count=2, total_basket_count=5)
        self.assertIn(("bread",), candidates)

    def test_io_is_deterministic(self) -> None:
        lines = ["user,item\n", "u1,b\n", "u1,a\n", "broken\n"]
        self.assertEqual(list(parse_partition(lines, skip_header=True)), [("u1", "b"), ("u1", "a")])
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "result.json"
            write_result(output, [("a",), ("a", "b")], [("a",)])
            payload = json.loads(output.read_text())
            self.assertEqual(payload["candidate_count"], 2)
            self.assertEqual(payload["frequent_itemset_count"], 1)


if __name__ == "__main__":
    unittest.main()

