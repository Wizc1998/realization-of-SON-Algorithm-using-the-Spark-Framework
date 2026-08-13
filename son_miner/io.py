"""CSV parsing and deterministic JSON output."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, Iterator

from .apriori import Itemset


def parse_partition(lines: Iterable[str], skip_header: bool = False) -> Iterator[tuple[str, str]]:
    reader = csv.reader(lines)
    if skip_header:
        next(reader, None)
    for row in reader:
        if len(row) < 2:
            continue
        left, right = row[0].strip(), row[1].strip()
        if left and right:
            yield left, right


def write_result(path: Path, candidates: list[Itemset], frequent: list[Itemset]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "candidates": [list(itemset) for itemset in candidates],
        "frequent_itemsets": [list(itemset) for itemset in frequent],
        "candidate_count": len(candidates),
        "frequent_itemset_count": len(frequent),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

