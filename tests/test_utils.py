from datetime import timedelta

import pytest

from manager.model import FieldData, MeasurementConfig
from manager.utils import filter_fields, hash_to_decimal, hash_to_integer, timedelta_to_flux_duration


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


# --- filter_fields ---

_ALL_FIELDS = {
    "cpu_usage": FieldData("float", True),
    "cpu_time_user": FieldData("float", True),
    "cpu_time_system": FieldData("float", True),
    "memory_usage": FieldData("float", True),
    "memory_rss": FieldData("float", True),
    "pid": FieldData("int", True),
    "status": FieldData("string", False),
}


class TestFilterFields:
    def test_none_config_returns_all(self):
        assert filter_fields(_ALL_FIELDS, None) == _ALL_FIELDS

    def test_empty_config_returns_all(self):
        assert filter_fields(_ALL_FIELDS, MeasurementConfig()) == _ALL_FIELDS

    def test_include_false_returns_none(self):
        assert filter_fields(_ALL_FIELDS, MeasurementConfig(include=False)) is None

    def test_include_true_returns_all(self):
        assert filter_fields(_ALL_FIELDS, MeasurementConfig(include=True)) == _ALL_FIELDS

    def test_include_fields_wildcard(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(include_fields=["cpu_*"]))
        assert set(result.keys()) == {"cpu_usage", "cpu_time_user", "cpu_time_system"}

    def test_include_fields_multiple_patterns(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(include_fields=["cpu_*", "memory_*"]))
        assert set(result.keys()) == {"cpu_usage", "cpu_time_user", "cpu_time_system", "memory_usage", "memory_rss"}

    def test_include_fields_exact_name(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(include_fields=["pid"]))
        assert set(result.keys()) == {"pid"}

    def test_exclude_fields_wildcard(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(exclude_fields=["cpu_time_*"]))
        assert "cpu_time_user" not in result
        assert "cpu_time_system" not in result
        assert "cpu_usage" in result

    def test_include_fields_then_exclude_fields(self):
        result = filter_fields(
            _ALL_FIELDS,
            MeasurementConfig(include_fields=["cpu_*"], exclude_fields=["cpu_time_*"]),
        )
        assert set(result.keys()) == {"cpu_usage"}

    def test_include_fields_no_match_returns_empty(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(include_fields=["nonexistent_*"]))
        assert result == {}

    def test_exclude_fields_all_returns_empty(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(exclude_fields=["*"]))
        assert result == {}

    def test_question_mark_wildcard(self):
        fields = {
            "a1": FieldData("float", True),
            "a2": FieldData("float", True),
            "ab": FieldData("float", True),
        }
        result = filter_fields(fields, MeasurementConfig(include_fields=["a?"]))
        assert set(result.keys()) == {"a1", "a2", "ab"}

    def test_preserves_field_data(self):
        result = filter_fields(_ALL_FIELDS, MeasurementConfig(include_fields=["status"]))
        assert result["status"] == FieldData("string", False)
