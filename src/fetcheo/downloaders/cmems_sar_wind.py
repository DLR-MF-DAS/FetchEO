"""Sentinel-1 L3 ocean surface wind from the Copernicus Marine Service.

SAR-derived ocean wind at ~1 km is the reference field of the DIVE rain-cell
study (rain cells imaged by radar, wind error diagnosed against SAR), and it is
the observation the ``rain_cell_composite`` processor supervises against.  The
download itself is a plain ``copernicusmarine.subset`` per day and per dataset,
so it is generic enough to serve anyone wanting L3 SAR wind over a polygon.

Authentication
--------------
``copernicusmarine`` reads the credentials stored by ``copernicusmarine login``,
or the ``COPERNICUSMARINE_SERVICE_USERNAME`` / ``COPERNICUSMARINE_SERVICE_PASSWORD``
environment variables.
"""

from __future__ import annotations

import datetime
import logging
from pathlib import Path
from typing import List, Optional, Sequence, Union

from tqdm import tqdm

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport

logger = logging.getLogger(__name__)

#: Sentinel-1 A/B level-3 ocean wind, 0.01 degree, near-real-time.
DEFAULT_DATASET_IDS = (
    "cmems_obs-wind_glo_phy_nrt_l3-s1a-owi-0.01deg_PT1S",
    "cmems_obs-wind_glo_phy_nrt_l3-s1b-owi-0.01deg_PT1S",
)


class CMEMSSARWindDownloader(BaseDownloader):
    """Download L3 SAR ocean-wind subsets from Copernicus Marine, one file per day.

    Args:
        dataset_ids: Copernicus Marine dataset identifiers to pull. The default
            covers Sentinel-1A and Sentinel-1B; pass a single-element sequence to
            restrict it, or other identifiers for a different mission.
        variables: Variables to keep. ``None`` keeps everything the dataset
            offers, which is what the DIVE analysis expects (``eastward_wind``,
            ``northward_wind``, ``wind_speed``, ``measurement_time``).
        skip_existing: Do not re-download a file that is already present and
            readable, so re-running over a long period is cheap.
    """

    data_source = "cmems_sar_wind"

    def __init__(
        self,
        dataset_ids: Sequence[str] = DEFAULT_DATASET_IDS,
        variables: Optional[Sequence[str]] = None,
        skip_existing: bool = True,
    ):
        self.dataset_ids = list(dataset_ids)
        self.variables = list(variables) if variables else None
        self.skip_existing = skip_existing

    @property
    def frequency(self) -> str:
        return "daily"

    def fetch(
        self,
        polygon: dict,
        time_frame: tuple[datetime.datetime, datetime.datetime],
        output_dir: Path,
        cache_dir: Union[str, Path, None] = None,
        show_progress: bool = True,
    ) -> List[ItemDownloadReport]:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        bbox = self._extract_bbox(polygon)
        days = _daterange(time_frame[0], time_frame[1])
        jobs = [(day, dataset_id) for day in days for dataset_id in self.dataset_ids]

        reports = []
        iterator = tqdm(jobs, desc="CMEMS SAR wind", unit="file",
                        disable=not show_progress)
        for day, dataset_id in iterator:
            basename = f"{dataset_id}_{day:%Y_%m_%d}"
            path = output_dir / f"{basename}.nc"
            try:
                if not (self.skip_existing and self._validate_files([path])[path]):
                    self._subset(dataset_id, day, bbox, path)
                if not self._validate_files([path])[path]:
                    raise RuntimeError("no readable file produced for this day")
                reports.append(ItemDownloadReport(
                    data_source=self.data_source,
                    variable_name="wind",
                    acquisition_time=self._acquisition_time(path, day),
                    polygon=polygon,
                    bbox=bbox,
                    path=path,
                    download_successful=True,
                    metadata={"dataset_id": dataset_id},
                ))
            except Exception as exc:  # noqa: BLE001 - a missing day is normal
                # SAR coverage is intermittent: most days simply have no swath
                # over the polygon, which the service reports as an error.
                logger.debug("No CMEMS SAR wind for %s on %s: %s",
                             dataset_id, day, exc)
                reports.append(ItemDownloadReport(
                    data_source=self.data_source,
                    variable_name="wind",
                    acquisition_time=datetime.datetime(day.year, day.month, day.day),
                    polygon=polygon,
                    bbox=bbox,
                    path=path,
                    download_successful=False,
                    error=str(exc),
                    metadata={"dataset_id": dataset_id},
                ))
        return reports

    # -- internals ---------------------------------------------------------

    def _subset(self, dataset_id: str, day: datetime.date,
                bbox: List[float], path: Path) -> None:
        """Run one ``copernicusmarine.subset`` call for a single day."""
        try:
            import copernicusmarine
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise ImportError(
                "The cmems_sar_wind downloader needs the 'copernicusmarine' "
                "package: pip install fetcheo[cmems]"
            ) from exc

        path.parent.mkdir(parents=True, exist_ok=True)
        kwargs = dict(
            dataset_id=dataset_id,
            start_datetime=f"{day:%Y-%m-%d}T00:00:00",
            end_datetime=f"{day:%Y-%m-%d}T23:59:59",
            minimum_longitude=bbox[0],
            maximum_longitude=bbox[2],
            minimum_latitude=bbox[1],
            maximum_latitude=bbox[3],
            output_filename=str(path),
        )
        if self.variables:
            kwargs["variables"] = self.variables
        copernicusmarine.subset(**kwargs)

    @staticmethod
    def _acquisition_time(path: Path, day: datetime.date) -> datetime.datetime:
        """Real measurement time of the swath, falling back to the day itself."""
        try:
            import numpy as np
            import xarray as xr

            with xr.open_dataset(path) as ds:
                for name in ("measurement_time", "time"):
                    if name in ds.variables:
                        values = np.asarray(ds[name].values).ravel()
                        values = values[~_isnat(values)]
                        if values.size:
                            return _to_datetime(values[0])
        except Exception:  # noqa: BLE001 - the day is a good enough fallback
            pass
        return datetime.datetime(day.year, day.month, day.day)


def _daterange(start: datetime.datetime, end: datetime.datetime) -> List[datetime.date]:
    """Every calendar day from *start* to *end*, inclusive."""
    first, last = start.date(), end.date()
    return [first + datetime.timedelta(days=i)
            for i in range((last - first).days + 1)]


def _isnat(values):
    import numpy as np
    if np.issubdtype(values.dtype, np.datetime64):
        return np.isnat(values)
    return np.zeros(values.shape, dtype=bool)


def _to_datetime(value) -> datetime.datetime:
    import pandas as pd
    return pd.to_datetime(value).to_pydatetime()
