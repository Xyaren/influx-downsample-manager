from datetime import timedelta

import pytest

from manager.utils import hash_to_decimal, hash_to_integer, timedelta_to_flux_duration


class TestHashToDecimal:
    def test_returns_value_within_range(self):
        result = hash_to_decimal("test", 0.0, 1.0)
        assert 0.0 <= result <= 1.0

    def test_custom_range(self):
        result = hash_to_decimal("hello", 10.0, 20.0)
        assert 10.0 <= result <= 20.0

    def test_deterministic(self):
        a = hash_to_decimal("same_input", 0.0, 100.0)
        b = hash_to_decimal("same_input", 0.0, 100.0)
        assert a == b

    def test_different_inputs_differ(self):
        a = hash_to_decimal("input_a", 0.0, 1.0)
        b = hash_to_decimal("input_b", 0.0, 1.0)
        assert a != b

    def test_min_equals_max(self):
        assert hash_to_decimal("anything", 5.0, 5.0) == 5.0


class TestHashToInteger:
    def test_returns_value_within_range(self):
        result = hash_to_integer("test", 0, 100)
        assert 0 <= result <= 100

    def test_deterministic(self):
        a = hash_to_integer("same", 0, 50)
        b = hash_to_integer("same", 0, 50)
        assert a == b

    def test_different_inputs_differ(self):
        a = hash_to_integer("alpha", 0, 1000)
        b = hash_to_integer("beta", 0, 1000)
        assert a != b

    def test_min_equals_max(self):
        assert hash_to_integer("x", 42, 42) == 42


@pytest.mark.parametrize(
    "td, expected",
    [
        (timedelta(0), "0s"),
        (timedelta(seconds=30), "30s"),
        (timedelta(minutes=5), "5m"),
        (timedelta(hours=2), "2h"),
        (timedelta(days=3), "3d"),
        (timedelta(days=1, hours=2, minutes=3, seconds=4), "1d2h3m4s"),
        (timedelta(days=2, seconds=45), "2d45s"),
        (timedelta(hours=1, minutes=30), "1h30m"),
        (timedelta(seconds=90.6), "1m31s"),
    ],
)
def test_timedelta_to_flux_duration(td, expected):
    assert timedelta_to_flux_duration(td) == expected
