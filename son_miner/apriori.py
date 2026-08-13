"""Deterministic, dependency-free Apriori primitives."""

from __future__ import annotations

import math
from collections import Counter
from itertools import combinations
from typing import Iterable


Itemset = tuple[str, ...]


def canonical(items: Iterable[str]) -> Itemset:
    return tuple(sorted(set(items)))


def generate_candidates(previous: set[Itemset], size: int) -> set[Itemset]:
    """Join frequent `(size-1)` itemsets and apply Apriori subset pruning."""
    if size < 2:
        raise ValueError("candidate size must be at least 2")
    candidates: set[Itemset] = set()
    ordered = sorted(previous)
    for left_index, left in enumerate(ordered):
        for right in ordered[left_index + 1 :]:
            union = canonical((*left, *right))
            if len(union) != size:
                continue
            if all(tuple(subset) in previous for subset in combinations(union, size - 1)):
                candidates.add(union)
    return candidates


def count_candidates(baskets: Iterable[set[str]], candidates: Iterable[Itemset]) -> Counter[Itemset]:
    candidate_sets = [(candidate, set(candidate)) for candidate in candidates]
    counts: Counter[Itemset] = Counter()
    for basket in baskets:
        for candidate, candidate_set in candidate_sets:
            if candidate_set.issubset(basket):
                counts[candidate] += 1
    return counts


def apriori_partition(
    baskets: Iterable[Iterable[str]],
    global_support: int,
    partition_basket_count: int,
    total_basket_count: int,
) -> list[Itemset]:
    """Generate SON candidates for one partition using scaled local support."""
    if global_support < 1 or partition_basket_count < 0 or total_basket_count < 1:
        raise ValueError("support and basket counts must be valid positive values")
    materialized = [set(basket) for basket in baskets]
    if not materialized:
        return []
    if len(materialized) != partition_basket_count:
        raise ValueError("partition_basket_count does not match materialized baskets")

    local_support = max(1, math.ceil(global_support * partition_basket_count / total_basket_count))
    single_counts = Counter(item for basket in materialized for item in basket)
    current: set[Itemset] = {(item,) for item, count in single_counts.items() if count >= local_support}
    all_frequent = set(current)
    size = 2
    while current:
        candidates = generate_candidates(current, size)
        if not candidates:
            break
        counts = count_candidates(materialized, candidates)
        current = {candidate for candidate, count in counts.items() if count >= local_support}
        all_frequent.update(current)
        size += 1
    return sorted(all_frequent, key=lambda itemset: (len(itemset), itemset))


def frequent_itemsets(
    baskets: Iterable[Iterable[str]], candidates: Iterable[Itemset], support: int
) -> list[Itemset]:
    """SON pass two: retain globally frequent candidates."""
    if support < 1:
        raise ValueError("support must be positive")
    counts = count_candidates((set(basket) for basket in baskets), candidates)
    return sorted(
        (candidate for candidate, count in counts.items() if count >= support),
        key=lambda itemset: (len(itemset), itemset),
    )

