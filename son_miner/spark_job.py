"""Distributed two-pass SON orchestration."""

from __future__ import annotations

from .apriori import apriori_partition
from .io import parse_partition


def create_context(master: str | None = None):
    from pyspark import SparkConf, SparkContext

    configuration = SparkConf().setAppName("son-frequent-itemset-miner")
    if master:
        configuration = configuration.setMaster(master)
    context = SparkContext.getOrCreate(configuration)
    context.setLogLevel("WARN")
    return context


def _partition_candidates(iterator, global_support: int, total_baskets: int):
    baskets = [set(basket) for basket in iterator]
    return iter(apriori_partition(baskets, global_support, len(baskets), total_baskets))


def _candidate_hits(basket: set[str], candidates: list[tuple[str, ...]]):
    return ((candidate, 1) for candidate in candidates if set(candidate).issubset(basket))


def mine(
    context,
    input_path: str,
    support: int,
    orientation: str = "left-baskets",
    min_basket_size: int = 1,
) -> tuple[list[tuple[str, ...]], list[tuple[str, ...]]]:
    if support < 1 or min_basket_size < 1:
        raise ValueError("support and min_basket_size must be positive")
    if orientation not in {"left-baskets", "right-baskets"}:
        raise ValueError("orientation must be left-baskets or right-baskets")

    pairs = context.textFile(input_path).mapPartitionsWithIndex(
        lambda index, lines: parse_partition(lines, skip_header=index == 0)
    )
    if orientation == "right-baskets":
        pairs = pairs.map(lambda pair: (pair[1], pair[0]))
    baskets = (
        pairs.groupByKey()
        .mapValues(lambda values: set(values))
        .filter(lambda pair: len(pair[1]) >= min_basket_size)
        .values()
        .cache()
    )
    total_baskets = baskets.count()
    if total_baskets == 0:
        return [], []

    candidates = (
        baskets.mapPartitions(
            lambda iterator: _partition_candidates(iterator, support, total_baskets)
        )
        .distinct()
        .sortBy(lambda itemset: (len(itemset), itemset))
        .collect()
    )
    broadcast_candidates = context.broadcast(candidates)
    frequent = (
        baskets.flatMap(lambda basket: _candidate_hits(basket, broadcast_candidates.value))
        .reduceByKey(lambda left, right: left + right)
        .filter(lambda pair: pair[1] >= support)
        .keys()
        .sortBy(lambda itemset: (len(itemset), itemset))
        .collect()
    )
    broadcast_candidates.unpersist()
    return candidates, frequent
