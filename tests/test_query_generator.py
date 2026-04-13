from datetime import timedelta

from manager.model import DownsampleConfiguration, FieldData
from manager.query_generator import ChainedQueryGenerator, SourceQueryGenerator


def _make_config(**overrides) -> DownsampleConfiguration:
    base: DownsampleConfiguration = {"interval": "10m", "every": "1h", "offset": "1m"}
    base.update(overrides)
    return base


NUMERIC_FIELDS = {
    "temperature": FieldData(data_type="float", numeric=True),
    "humidity": FieldData(data_type="float", numeric=True),
}
NON_NUMERIC_FIELDS = {"status": FieldData(data_type="string", numeric=False)}
MIXED_FIELDS = {**NUMERIC_FIELDS, **NON_NUMERIC_FIELDS}


# --- Task naming ---


class TestTaskName:
    def test_default_prefix(self):
        gen = SourceQueryGenerator("src", "dst", _make_config(), "cpu", NUMERIC_FIELDS)
        assert gen.task_name() == "gen_dst_cpu"

    def test_custom_prefix(self):
        gen = SourceQueryGenerator("src", "dst", _make_config(), "cpu", NUMERIC_FIELDS, task_prefix="custom_")
        assert gen.task_name() == "custom_dst_cpu"


# --- Offset calculation ---


class TestOffset:
    def test_fixed_offset(self):
        gen = SourceQueryGenerator("src", "dst", _make_config(offset="30s"), "measurement", NUMERIC_FIELDS)
        assert gen.offset_with_predictable_factor() == timedelta(seconds=30)

    def test_range_is_deterministic(self):
        cfg = _make_config(offset="10s", max_offset="60s")
        gen = SourceQueryGenerator("src", "dst", cfg, "measurement", NUMERIC_FIELDS)
        a = gen.offset_with_predictable_factor()
        b = gen.offset_with_predictable_factor()
        assert a == b
        assert 10 <= a.total_seconds() <= 60

    def test_min_equals_max(self):
        cfg = _make_config(offset="20s", max_offset="20s")
        gen = SourceQueryGenerator("src", "dst", cfg, "measurement", NUMERIC_FIELDS)
        assert gen.offset_with_predictable_factor() == timedelta(seconds=20)


# --- Field classification ---


class TestFieldClassification:
    def test_numeric_fields_extracted(self):
        gen = SourceQueryGenerator("s", "d", _make_config(), "m", MIXED_FIELDS)
        assert "temperature" in gen.numeric_fields
        assert "humidity" in gen.numeric_fields
        assert "status" not in gen.numeric_fields

    def test_non_numeric_fields_extracted(self):
        gen = SourceQueryGenerator("s", "d", _make_config(), "m", MIXED_FIELDS)
        assert "status" in gen.non_numeric_fields
        assert "temperature" not in gen.non_numeric_fields

    def test_empty_fields(self):
        gen = SourceQueryGenerator("s", "d", _make_config(), "m", {})
        assert gen.numeric_fields == []
        assert gen.non_numeric_fields == []


# --- SourceQueryGenerator ---


class TestSourceQueryGeneratorTask:
    def test_numeric_only(self):
        gen = SourceQueryGenerator("raw", "raw_1w", _make_config(), "cpu", NUMERIC_FIELDS)
        task = gen.generate_task()
        assert 'option task = {name: "gen_raw_1w_cpu"' in task
        assert 'from(bucket:"raw")' in task
        assert 'to(bucket:"raw_1w")' in task
        assert "aggregateWindow(every: 10m, fn: mean)" in task
        assert "tasks.lastSuccess" in task
        assert "fn: last" not in task

    def test_non_numeric_only(self):
        task = SourceQueryGenerator("raw", "raw_1w", _make_config(), "status", NON_NUMERIC_FIELDS).generate_task()
        assert "fn: last" in task
        assert "fn: mean" not in task

    def test_mixed_fields(self):
        task = SourceQueryGenerator("raw", "raw_1w", _make_config(), "env", MIXED_FIELDS).generate_task()
        assert "fn: mean" in task
        assert "fn: last" in task

    def test_imports(self):
        task = SourceQueryGenerator("raw", "raw_1w", _make_config(), "cpu", NUMERIC_FIELDS).generate_task()
        assert 'import "influxdata/influxdb/tasks"' in task
        assert 'import "date"' in task

    def test_measurement_filter(self):
        task = SourceQueryGenerator("raw", "raw_1w", _make_config(), "my_meas", NUMERIC_FIELDS).generate_task()
        assert 'r._measurement == "my_meas"' in task


class TestSourceQueryGeneratorQuery:
    def test_start_stop(self):
        query = SourceQueryGenerator("raw", "raw_1w", _make_config(), "cpu", NUMERIC_FIELDS).generate_query(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"
        )
        assert 'start = time(v:"2024-01-01T00:00:00Z")' in query
        assert 'stop = time(v:"2024-01-02T00:00:00Z")' in query

    def test_limit_and_yield(self):
        query = SourceQueryGenerator("raw", "raw_1w", _make_config(), "cpu", NUMERIC_FIELDS).generate_query(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"
        )
        assert "limit(n:1)" in query
        assert 'yield(name: "numeric")' in query

    def test_non_numeric_yield(self):
        query = SourceQueryGenerator("raw", "raw_1w", _make_config(), "status", NON_NUMERIC_FIELDS).generate_query(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"
        )
        assert 'yield(name: "non_numeric")' in query


class TestSourceQueryGeneratorStr:
    def test_str(self):
        gen = SourceQueryGenerator("raw", "raw_1w", _make_config(), "cpu", NUMERIC_FIELDS)
        assert str(gen) == "raw -> raw_1w: cpu"


# --- ChainedQueryGenerator ---


class TestChainedQueryGenerator:
    def test_task_name(self):
        gen = ChainedQueryGenerator("upstream", "dst", _make_config(), "cpu", NUMERIC_FIELDS)
        assert gen.task_name() == "gen_dst_cpu"

    def test_reads_from_upstream_bucket(self):
        task = ChainedQueryGenerator(
            "upstream_1w", "upstream_31d", _make_config(), "cpu", NUMERIC_FIELDS
        ).generate_task()
        assert 'from(bucket:"upstream_1w")' in task
        assert 'to(bucket:"upstream_31d")' in task

    def test_numeric_uses_mean(self):
        task = ChainedQueryGenerator("up", "dst", _make_config(), "cpu", NUMERIC_FIELDS).generate_task()
        assert "fn: mean" in task

    def test_non_numeric_uses_last(self):
        task = ChainedQueryGenerator("up", "dst", _make_config(), "status", NON_NUMERIC_FIELDS).generate_task()
        assert "fn: last" in task

    def test_mixed_fields(self):
        task = ChainedQueryGenerator("up", "dst", _make_config(), "env", MIXED_FIELDS).generate_task()
        assert "fn: mean" in task
        assert "fn: last" in task

    def test_generate_query(self):
        query = ChainedQueryGenerator("up", "dst", _make_config(), "cpu", NUMERIC_FIELDS).generate_query(
            "2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"
        )
        assert 'from(bucket:"up")' in query
        assert "limit(n:1)" in query
