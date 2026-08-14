import pytest

from fetcheo.registry import (
    DOWNLOADER_REGISTRY,
    PROCESSOR_REGISTRY,
    PluginRegistry,
    available_downloaders,
    available_processors,
    register_downloader,
    register_processor,
)


class DummyPlugin:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def make_registry(**builtins):
    # A private group name keeps the test independent of what is installed.
    return PluginRegistry("widget", builtins, "fetcheo.test_only_group")


def test_builtin_downloaders_are_available():
    names = available_downloaders()
    for expected in ("era5", "modis_ndvi", "sen3_openeo", "sen3_eodag"):
        assert expected in names
    # The DIVE data sources ship as ordinary downloaders.
    assert "cmems_sar_wind" in names
    assert "ifremer_raincell" in names


def test_processor_registry_starts_empty_but_exists():
    # FetchEO ships no scientific processor: the layer is there for plugins.
    assert isinstance(available_processors(), list)


def test_register_and_create():
    registry = make_registry()
    registry.register("dummy", DummyPlugin)
    assert "dummy" in registry
    assert registry.names() == ["dummy"]
    instance = registry.create("dummy", answer=42)
    assert isinstance(instance, DummyPlugin)
    assert instance.kwargs == {"answer": 42}


@pytest.mark.parametrize("target", [
    "fetcheo.registry:PluginRegistry",   # "module:Class"
    "fetcheo.registry.PluginRegistry",   # "module.Class"
])
def test_register_resolves_dotted_path_lazily(target):
    registry = make_registry()
    registry.register("lazy", target)
    # Nothing imported yet: the entry is still the string it was given.
    assert registry.as_dict()["lazy"] == target
    assert registry.load("lazy") is PluginRegistry
    # And the resolved class is cached in place of the string.
    assert registry.as_dict()["lazy"] is PluginRegistry


def test_unresolvable_target_fails_only_when_used():
    registry = make_registry()
    # Registering a plugin whose dependencies are missing must not cost anything
    # to the others: the failure happens on use, not on registration.
    registry.register("broken", "fetcheo.does_not_exist:Nope")
    assert "broken" in registry.names()
    with pytest.raises(ModuleNotFoundError):
        registry.load("broken")


def test_register_refuses_silent_override():
    registry = make_registry(dummy=DummyPlugin)

    class Other:
        pass

    with pytest.raises(ValueError):
        registry.register("dummy", Other)
    registry.register("dummy", Other, override=True)
    assert registry.load("dummy") is Other

    # Re-registering the exact same target is a no-op, not an error, so a module
    # imported twice does not blow up.
    registry.register("dummy", Other)


def test_load_unknown_name_lists_alternatives():
    registry = make_registry(dummy=DummyPlugin)
    with pytest.raises(KeyError) as excinfo:
        registry.load("nope")
    assert "dummy" in str(excinfo.value)


def test_create_many_respects_flags_and_unknown_names():
    registry = make_registry(a=DummyPlugin, b=DummyPlugin)
    instances = registry.create_many({"a": True, "b": False, "ghost": True})
    assert set(instances) == {"a"}

    with pytest.raises(KeyError):
        registry.create_many({"ghost": True}, strict=True)


def test_create_many_passes_kwargs():
    registry = make_registry(a=DummyPlugin)
    instances = registry.create_many({"a": True}, {"a": {"x": 1}})
    assert instances["a"].kwargs == {"x": 1}


def test_public_registration_helpers():
    register_downloader("test_only_downloader", DummyPlugin)
    register_processor("test_only_processor", DummyPlugin)
    try:
        assert "test_only_downloader" in available_downloaders()
        assert "test_only_processor" in available_processors()
        assert isinstance(PROCESSOR_REGISTRY.create("test_only_processor"),
                          DummyPlugin)
    finally:
        DOWNLOADER_REGISTRY._plugins.pop("test_only_downloader", None)
        PROCESSOR_REGISTRY._plugins.pop("test_only_processor", None)
