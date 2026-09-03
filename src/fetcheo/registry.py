"""Extensible plugin registries for downloaders and processors.

FetchEO ships a set of built-in plugins, but the point of this module is that an
**external package does not need to be modified into FetchEO to be usable by
it**.  A plugin can join in three ways, in increasing order of decoupling:

1. it is listed in the built-in table of this module;
2. it calls :func:`register_downloader` / :func:`register_processor` at import
   time (useful for notebooks, scripts, or a project that drives FetchEO itself);
3. it declares a ``fetcheo.downloaders`` or ``fetcheo.processors`` entry point in
   its own packaging metadata, and FetchEO discovers it with no import and no
   configuration at all::

       [project.entry-points."fetcheo.processors"]
       rain_cell_composite = "mypackage.plugins:RainCellCompositeProcessor"

Targets are resolved lazily: a registry entry may be a class, or the string
``"module.path:ClassName"`` / ``"module.path.ClassName"``.  The module is only
imported when the plugin is actually instantiated, so a heavy or optional
dependency of one plugin never costs anything to the others.
"""

from __future__ import annotations

import importlib
import logging
from importlib import metadata
from typing import Any, Dict, Iterable, Optional, Union

logger = logging.getLogger(__name__)

PluginTarget = Union[type, str]


def _import_target(target: str) -> type:
    """Resolve ``"module:Class"`` or ``"module.Class"`` to the class object."""
    if ":" in target:
        module_path, class_name = target.split(":", 1)
    else:
        module_path, class_name = target.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


class PluginRegistry:
    """A named collection of plugins that can be extended from the outside.

    Args:
        kind: What the registry holds, used in messages (e.g. ``"downloader"``).
        builtins: Mapping of name to class or dotted path, shipped with FetchEO.
        entry_point_group: Packaging entry-point group scanned for third-party
            plugins (e.g. ``"fetcheo.processors"``).
    """

    def __init__(self, kind: str, builtins: Dict[str, PluginTarget],
                 entry_point_group: str):
        self.kind = kind
        self.entry_point_group = entry_point_group
        self._plugins: Dict[str, PluginTarget] = dict(builtins)
        self._entry_points_loaded = False

    # -- registration ------------------------------------------------------

    def register(self, name: str, target: PluginTarget,
                 override: bool = False) -> None:
        """Add a plugin under *name*.

        Args:
            name: Key used by the config dicts and the CLI.
            target: Class, or ``"module:Class"`` / ``"module.Class"`` to import
                lazily.
            override: Allow replacing an existing entry.  Off by default so a
                third-party plugin cannot silently shadow a built-in one.
        """
        if name in self._plugins and not override:
            if self._plugins[name] == target:
                return
            raise ValueError(
                f"{self.kind} '{name}' is already registered as "
                f"{self._plugins[name]!r}; pass override=True to replace it"
            )
        self._plugins[name] = target

    def _load_entry_points(self) -> None:
        """Discover third-party plugins declared through packaging metadata."""
        if self._entry_points_loaded:
            return
        # Marked as loaded up front: a broken environment must not make every
        # later lookup retry the same failing discovery.
        self._entry_points_loaded = True
        try:
            entry_points = metadata.entry_points(group=self.entry_point_group)
        except Exception as exc:  # pragma: no cover - depends on the environment
            logger.warning("Could not scan %s entry points: %s",
                           self.entry_point_group, exc)
            return
        for entry_point in entry_points:
            # Entry points are resolved lazily like any other target, so a
            # plugin with missing dependencies only fails if it is used.
            try:
                self.register(entry_point.name, entry_point.value)
            except ValueError as exc:
                logger.warning("Ignoring %s entry point '%s': %s",
                               self.kind, entry_point.name, exc)

    # -- lookup ------------------------------------------------------------

    def names(self) -> list[str]:
        """All plugin names known to the registry, built-in and external."""
        self._load_entry_points()
        return sorted(self._plugins)

    def __contains__(self, name: object) -> bool:
        self._load_entry_points()
        return name in self._plugins

    def load(self, name: str) -> type:
        """Return the class registered under *name*, importing it if needed."""
        self._load_entry_points()
        if name not in self._plugins:
            raise KeyError(
                f"Unknown {self.kind} '{name}'. Available: {self.names()}")
        target = self._plugins[name]
        if isinstance(target, str):
            target = _import_target(target)
            self._plugins[name] = target   # cache the resolved class
        return target

    def create(self, name: str, **kwargs: Any):
        """Instantiate the plugin registered under *name*."""
        return self.load(name)(**kwargs)

    def create_many(self, config: Dict[str, bool],
                    kwargs_by_name: Optional[Dict[str, dict]] = None,
                    strict: bool = False) -> Dict[str, Any]:
        """Instantiate every plugin enabled in *config*.

        Args:
            config: Mapping of plugin name to enabled flag.
            kwargs_by_name: Constructor keyword arguments per plugin.
            strict: Raise on an unknown name instead of skipping it.  The
                permissive default preserves the historical loader behaviour.
        """
        kwargs_by_name = kwargs_by_name or {}
        instances = {}
        for name, enabled in (config or {}).items():
            if not enabled:
                continue
            if name not in self:
                if strict:
                    raise KeyError(
                        f"Unknown {self.kind} '{name}'. Available: {self.names()}")
                logger.warning("Skipping unknown %s '%s'", self.kind, name)
                continue
            instances[name] = self.create(name, **kwargs_by_name.get(name, {}))
        return instances

    def as_dict(self) -> Dict[str, PluginTarget]:
        """Snapshot of the registry, for display or backwards compatibility."""
        self._load_entry_points()
        return dict(self._plugins)


# ---------------------------------------------------------------------------
# The registries FetchEO itself uses
# ---------------------------------------------------------------------------

BUILTIN_DOWNLOADERS: Dict[str, PluginTarget] = {
    'era5': 'fetcheo.downloaders.era5.ERA5Downloader',
    'modis_ndvi': 'fetcheo.downloaders.modis_ndvi.MODISNDVIDownloader',
    'sen3_openeo': 'fetcheo.downloaders.sen3_openeo.Sen3WaterOpenEODownloader',
    'sen3_eodag': 'fetcheo.downloaders.sen3_eodag.Sentinel3SynergyDownloader',
    'cmems_sar_wind': 'fetcheo.downloaders.cmems_sar_wind.CMEMSSARWindDownloader',
}

BUILTIN_PROCESSORS: Dict[str, PluginTarget] = {
    # FetchEO ships no scientific processor of its own: the layer exists so
    # projects such as DIVE can plug their analysis in without being vendored.
}

DOWNLOADER_REGISTRY = PluginRegistry(
    "downloader", BUILTIN_DOWNLOADERS, "fetcheo.downloaders")
PROCESSOR_REGISTRY = PluginRegistry(
    "processor", BUILTIN_PROCESSORS, "fetcheo.processors")


def register_downloader(name: str, target: PluginTarget,
                        override: bool = False) -> None:
    """Register a downloader from outside FetchEO."""
    DOWNLOADER_REGISTRY.register(name, target, override=override)


def register_processor(name: str, target: PluginTarget,
                       override: bool = False) -> None:
    """Register a processor from outside FetchEO."""
    PROCESSOR_REGISTRY.register(name, target, override=override)


def available_downloaders() -> list[str]:
    """Names of every downloader FetchEO can use, including external ones."""
    return DOWNLOADER_REGISTRY.names()


def available_processors() -> list[str]:
    """Names of every processor FetchEO can use, including external ones."""
    return PROCESSOR_REGISTRY.names()
