"""Shared helpers for generating reproducible synthetic data across the portfolio."""

import random
import uuid


def seeded_rng(seed: int = 42) -> random.Random:
    return random.Random(seed)


def new_id() -> str:
    return str(uuid.uuid4())


def skewed_choice(
    rng: random.Random, items: list, hot_fraction: float = 0.05, hot_weight: float = 0.6
):
    """Pick an item from `items` such that `hot_fraction` of items receive `hot_weight` of picks.

    Used to simulate the "5% of restaurants generate 60% of orders" pattern.
    """
    n_hot = max(1, int(len(items) * hot_fraction))
    hot_items, cold_items = items[:n_hot], items[n_hot:]
    if rng.random() < hot_weight and hot_items:
        return rng.choice(hot_items)
    return rng.choice(cold_items or hot_items)
