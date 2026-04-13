from manager.model import DownsampleConfiguration, FieldData, LabelDef


class TestFieldData:
    def test_creation(self):
        fd = FieldData(data_type="float", numeric=True)
        assert fd.data_type == "float"
        assert fd.numeric is True

    def test_equality(self):
        assert FieldData("int", True) == FieldData("int", True)

    def test_inequality(self):
        assert FieldData("float", True) != FieldData("string", False)

    def test_hashable(self):
        fd = FieldData("float", True)
        assert fd in {fd}


class TestLabelDef:
    def test_creation(self):
        label = LabelDef(name="Test", description="Desc", color="#fff")
        assert label.name == "Test"
        assert label.description == "Desc"
        assert label.color == "#fff"

    def test_hashable(self):
        label = LabelDef("A", "B", "#000")
        assert label in {label}


class TestDownsampleConfiguration:
    def test_required_fields(self):
        cfg: DownsampleConfiguration = {"interval": "1m", "every": "15m", "offset": "30s"}
        assert cfg["interval"] == "1m"
        assert cfg["every"] == "15m"
        assert cfg["offset"] == "30s"

    def test_optional_fields(self):
        cfg: DownsampleConfiguration = {
            "interval": "10m",
            "every": "1h",
            "offset": "1m",
            "max_offset": "55m",
            "expires": "31d",
            "bucket_shard_group_interval": "3d",
            "chained": True,
        }
        assert cfg["chained"] is True
        assert cfg["expires"] == "31d"
        assert cfg.get("max_offset") == "55m"
