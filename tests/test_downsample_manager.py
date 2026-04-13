from unittest.mock import MagicMock, patch

import pytest

from manager.downsample_manager import LABEL_DOWNSAMPLING, DownsampleManager
from manager.model import DownsampleConfiguration, LabelDef

_client_patcher = patch("manager.downsample_manager.InfluxDBClient")


@pytest.fixture
def mgr():
    """Yield a DownsampleManager with a fully mocked InfluxDB client."""
    MockClient = _client_patcher.start()
    mock_client = MockClient.return_value
    mock_org = MagicMock()
    mock_org.id = "org-id-123"
    mock_client.organizations_api.return_value.find_organizations.return_value = [mock_org]
    mock_client.api_client = MagicMock()

    manager = DownsampleManager(
        org="test-org",
        token="test-token",
        buckets=["raw"],
        bucket_configs={
            "1w": DownsampleConfiguration(
                interval="1m",
                every="15m",
                offset="30s",
                expires="1w",
                bucket_shard_group_interval="1d",
            ),
        },
        url="http://localhost:8086",
    )
    _client_patcher.stop()
    yield manager


def _mock_task(*, id: str, name: str, **kwargs) -> MagicMock:
    """Create a MagicMock task with `name` set as a real attribute.

    MagicMock treats `name` specially (it's the mock's internal label),
    so we must assign it after construction.
    """
    task = MagicMock(id=id, **kwargs)
    task.name = name
    return task


def _mock_label(*, id: str, name: str, **kwargs) -> MagicMock:
    """Same pattern for labels that need a real `name` attribute."""
    label = MagicMock(id=id, **kwargs)
    label.name = name
    return label


# --- Init & context manager ---


class TestInit:
    def test_attributes(self, mgr):
        assert mgr._buckets == ["raw"]
        assert mgr._task_prefix == "gen_"
        assert mgr._metric_detection_duration == "1d"
        assert mgr._organization.id == "org-id-123"

    def test_custom_metric_detection_duration(self):
        MockClient = _client_patcher.start()
        mock_client = MockClient.return_value
        mock_org = MagicMock()
        mock_org.id = "org-id-123"
        mock_client.organizations_api.return_value.find_organizations.return_value = [mock_org]
        mock_client.api_client = MagicMock()
        m = DownsampleManager(
            org="o",
            token="t",
            buckets=[],
            bucket_configs={},
            url="http://localhost:8086",
            metric_detection_duration="7d",
        )
        _client_patcher.stop()
        assert m._metric_detection_duration == "7d"


class TestContextManager:
    def test_enter_returns_self(self, mgr):
        assert mgr.__enter__() is mgr

    def test_exit_closes_client(self, mgr):
        mgr._client = MagicMock()
        mgr.__exit__(None, None, None)
        mgr._client.close.assert_called_once()


# --- Labels ---


class TestCreateOrGetLabel:
    def test_creates_new_label(self, mgr):
        mgr._client.labels_api.return_value.find_label_by_org.return_value = []
        mock_label = MagicMock()
        mgr._client.labels_api.return_value.create_label.return_value = mock_label

        result = mgr.create_or_get_label(LabelDef("Test", "Desc", "#fff"))
        assert result is mock_label
        mgr._client.labels_api.return_value.create_label.assert_called_once()

    def test_updates_existing_owned_label(self, mgr):
        existing = MagicMock()
        existing.name = "Test"
        existing.properties = {"creator": "influx-downsample-manager", "color": "#000", "description": "Old"}
        mgr._client.labels_api.return_value.find_label_by_org.return_value = [existing]

        result = mgr.create_or_get_label(LabelDef("Test", "New Desc", "#fff"))
        assert result is existing
        assert existing.properties["color"] == "#fff"
        assert existing.properties["description"] == "New Desc"
        mgr._client.labels_api.return_value.update_label.assert_called_once_with(existing)

    def test_raises_on_unowned_label(self, mgr):
        existing = MagicMock()
        existing.name = "Test"
        existing.properties = {"creator": "someone-else"}
        mgr._client.labels_api.return_value.find_label_by_org.return_value = [existing]

        with pytest.raises(Exception, match="not managed by this script"):
            mgr.create_or_get_label(LabelDef("Test", "Desc", "#fff"))


# --- Buckets ---


class TestCreateBucket:
    def test_creates_new_bucket(self, mgr):
        mgr._client.buckets_api.return_value.find_bucket_by_name.return_value = None
        mock_bucket = MagicMock()
        mgr._client.buckets_api.return_value.create_bucket.return_value = mock_bucket

        assert mgr.create_bucket("test-bucket", "7d", "1d") is mock_bucket
        mgr._client.buckets_api.return_value.create_bucket.assert_called_once()

    def test_updates_existing_bucket(self, mgr):
        existing = MagicMock()
        mgr._client.buckets_api.return_value.find_bucket_by_name.return_value = existing
        mgr._client.buckets_api.return_value.update_bucket.return_value = existing

        assert mgr.create_bucket("test-bucket", "7d", None) is existing
        mgr._client.buckets_api.return_value.update_bucket.assert_called_once()

    def test_no_expiry(self, mgr):
        mgr._client.buckets_api.return_value.find_bucket_by_name.return_value = None
        mock_bucket = MagicMock()
        mgr._client.buckets_api.return_value.create_bucket.return_value = mock_bucket

        assert mgr.create_bucket("test-bucket", None, None) is mock_bucket

    def test_raises_on_unparseable_expires(self, mgr):
        with pytest.raises(Exception, match="Can't parse"):
            mgr.create_bucket("b", "not_a_duration", None)


# --- Label assignment ---


class TestAddLabelToBucket:
    def test_adds_when_not_present(self, mgr):
        mgr._buckets_service = MagicMock()
        bucket, label = MagicMock(id="bucket-1"), MagicMock(id="label-1")
        mgr._buckets_service.get_buckets_id_labels.return_value = MagicMock(labels=[])

        mgr.add_label_to_bucket(bucket, label)
        mgr._buckets_service.post_buckets_id_labels.assert_called_once()

    def test_skips_if_already_present(self, mgr):
        mgr._buckets_service = MagicMock()
        bucket, label = MagicMock(id="bucket-1"), MagicMock(id="label-1")
        existing = MagicMock(id="label-1")
        mgr._buckets_service.get_buckets_id_labels.return_value = MagicMock(labels=[existing])

        mgr.add_label_to_bucket(bucket, label)
        mgr._buckets_service.post_buckets_id_labels.assert_not_called()


class TestAddLabelToTask:
    def test_adds_when_not_present(self, mgr):
        mgr._tasks_service = MagicMock()
        task, label = MagicMock(id="task-1"), MagicMock(id="label-1")
        mgr._tasks_service.get_tasks_id_labels.return_value = MagicMock(labels=[])

        mgr.add_label_to_task(task, label)
        mgr._tasks_service.post_tasks_id_labels.assert_called_once()

    def test_skips_if_already_present(self, mgr):
        mgr._tasks_service = MagicMock()
        task, label = MagicMock(id="task-1"), MagicMock(id="label-1")
        existing = MagicMock(id="label-1")
        mgr._tasks_service.get_tasks_id_labels.return_value = MagicMock(labels=[existing])

        mgr.add_label_to_task(task, label)
        mgr._tasks_service.post_tasks_id_labels.assert_not_called()


# --- Config sorting ---


class TestSortedBucketConfigs:
    def test_sorts_by_interval_ascending(self):
        configs = {
            "inf": DownsampleConfiguration(interval="1h", every="1d", offset="5m"),
            "1w": DownsampleConfiguration(interval="1m", every="15m", offset="30s"),
            "31d": DownsampleConfiguration(interval="10m", every="1h", offset="1m"),
        }
        intervals = [cfg["interval"] for _, cfg in DownsampleManager._sorted_bucket_configs(configs)]
        assert intervals == ["1m", "10m", "1h"]

    def test_raises_on_unparseable_interval(self):
        configs = {"bad": DownsampleConfiguration(interval="nope", every="1h", offset="1m")}
        with pytest.raises(Exception, match="Can't parse interval"):
            DownsampleManager._sorted_bucket_configs(configs)


# --- Task CRUD ---


class TestCreateOrUpdateTasks:
    def test_creates_new_task(self, mgr):
        mgr._client.tasks_api.return_value.find_tasks.return_value = []
        mock_task = MagicMock(id="new-task-id")
        mgr._client.tasks_api.return_value.create_task.return_value = mock_task

        created = set()
        assert mgr.create_or_update_tasks(created, "flux_query", "my_task") is mock_task
        assert "new-task-id" in created

    def test_updates_when_flux_differs(self, mgr):
        existing = _mock_task(id="existing-id", flux="old_flux", name="my_task")
        mgr._client.tasks_api.return_value.find_tasks.return_value = [existing]
        updated = MagicMock(id="existing-id")
        mgr._client.tasks_api.return_value.update_task_request.return_value = updated

        created = set()
        assert mgr.create_or_update_tasks(created, "new_flux", "my_task") is updated
        assert "existing-id" in created

    def test_skips_update_when_flux_matches(self, mgr):
        existing = _mock_task(id="existing-id", flux="same_flux", name="my_task")
        mgr._client.tasks_api.return_value.find_tasks.return_value = [existing]

        created = set()
        assert mgr.create_or_update_tasks(created, "same_flux", "my_task") is existing
        assert "existing-id" in created
        mgr._client.tasks_api.return_value.update_task_request.assert_not_called()

    def test_deduplicates_when_multiple_exist(self, mgr):
        first = _mock_task(id="first-id", flux="flux", name="my_task")
        duplicate = _mock_task(id="dup-id", name="my_task")
        mgr._client.tasks_api.return_value.find_tasks.return_value = [first, duplicate]

        created = set()
        assert mgr.create_or_update_tasks(created, "flux", "my_task") is first
        mgr._client.tasks_api.return_value.delete_task.assert_called_once_with(task_id="dup-id")


# --- Cleanup ---


class TestCleanupTasks:
    def test_deletes_orphaned_tasks(self, mgr):
        label = MagicMock()
        orphan = _mock_task(id="orphan-id", name="gen_old_task", labels=[label])
        kept = _mock_task(id="kept-id", name="gen_kept_task", labels=[label])
        mgr._client.tasks_api.return_value.find_tasks_iter.return_value = [orphan, kept]

        mgr.cleanup_tasks(label, created_tasks={"kept-id"})
        mgr._client.tasks_api.return_value.delete_task.assert_called_once_with(task_id="orphan-id")

    def test_ignores_tasks_without_label(self, mgr):
        label = MagicMock()
        task = _mock_task(id="t1", name="gen_something", labels=[])
        mgr._client.tasks_api.return_value.find_tasks_iter.return_value = [task]

        mgr.cleanup_tasks(label, created_tasks=set())
        mgr._client.tasks_api.return_value.delete_task.assert_not_called()

    def test_ignores_tasks_without_prefix(self, mgr):
        label = MagicMock()
        task = MagicMock(id="t1", labels=[label])
        task.name = "manual_task"
        mgr._client.tasks_api.return_value.find_tasks_iter.return_value = [task]

        mgr.cleanup_tasks(label, created_tasks=set())
        mgr._client.tasks_api.return_value.delete_task.assert_not_called()


class TestCleanupLabels:
    def test_deletes_orphaned_labels(self, mgr):
        orphan = _mock_label(id="orphan-label", name="Old", properties={"creator": "influx-downsample-manager"})
        kept = _mock_label(id="kept-label", name="Kept", properties={"creator": "influx-downsample-manager"})
        mgr._client.labels_api.return_value.find_label_by_org.return_value = [orphan, kept]

        mgr.cleanup_labels(created_label_ids={"kept-label"})
        mgr._client.labels_api.return_value.delete_label.assert_called_once_with(orphan)

    def test_ignores_unowned_labels(self, mgr):
        external = _mock_label(id="ext", name="External", properties={"creator": "someone-else"})
        mgr._client.labels_api.return_value.find_label_by_org.return_value = [external]

        mgr.cleanup_labels(created_label_ids=set())
        mgr._client.labels_api.return_value.delete_label.assert_not_called()


# --- Constants ---


def test_label_downsampling_constant():
    assert LABEL_DOWNSAMPLING.name == "Downsampling"
    assert LABEL_DOWNSAMPLING.color == "#ffff00"
