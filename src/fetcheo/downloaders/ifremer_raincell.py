"""IFREMER machine-learning rain-cell detections from Sentinel-1 GRD.

IFREMER publishes, per Sentinel-1 acquisition, the polygons of the rain cells a
neural network detected in the SAR image, as GeoJSON.  These are the vector
counterpart of the raster products elsewhere in FetchEO, and the detections the
DIVE study cross-checks against ground rain radar.

The archive is a plain indexed HTTP directory, so the downloader lists it, reads
the acquisition time out of each file name, and keeps what falls inside the
requested period::

    masks_lonlat_S1A_IW_GRDH_1SDV_20190305T063159_20190305T063224_..._.geojson
                                  ^ start              ^ end

Whether a file actually covers the requested polygon can only be known after
reading it, so every file in the period is downloaded and the intersection is
reported in the metadata rather than used to skip files silently.
"""

from __future__ import annotations

import datetime
import json
import logging
import re
from pathlib import Path
from typing import List, Optional, Union
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://cerweb.ifremer.fr/datarmor/tmp/sarcell_latest/dive_260526/"
DEFAULT_PREFIX = "masks_lonlat_"

# ..._20190305T063159_20190305T063224_...
_TIMES_RE = re.compile(r"_(\d{8}T\d{6})_(\d{8}T\d{6})_")


class IfremerRainCellDownloader(BaseDownloader):
    """Fetch IFREMER SAR rain-cell GeoJSON detections over a period.

    Args:
        base_url: Indexed HTTP directory holding the detections.  The default
            points at the DIVE delivery, which is a temporary path on the
            IFREMER server and is expected to move.
        prefix: File-name prefix identifying the detection files.
        suffix: File extension to keep.
        skip_existing: Do not re-download a file already present and readable.
        timeout: Per-request timeout in seconds.
    """

    data_source = "ifremer_raincell"

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        prefix: str = DEFAULT_PREFIX,
        suffix: str = ".geojson",
        skip_existing: bool = True,
        timeout: int = 60,
    ):
        self.base_url = base_url if base_url.endswith("/") else base_url + "/"
        self.prefix = prefix
        self.suffix = suffix
        self.skip_existing = skip_existing
        self.timeout = timeout

    @property
    def frequency(self) -> str:
        return "per_acquisition"

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

        session = self._make_session()
        try:
            names = self._list_directory(session)
        except requests.RequestException as exc:
            logger.error("Could not list %s: %s", self.base_url, exc)
            return [ItemDownloadReport(
                data_source=self.data_source,
                variable_name="rain_cells",
                acquisition_time=time_frame[0],
                polygon=polygon,
                bbox=bbox,
                path=output_dir,
                download_successful=False,
                error=f"directory listing failed: {exc}",
            )]

        wanted = []
        for name in names:
            acquisition_time = self._acquisition_time(name)
            if acquisition_time is None:
                continue
            if time_frame[0] <= acquisition_time <= time_frame[1]:
                wanted.append((name, acquisition_time))
        wanted.sort(key=lambda item: item[1])
        logger.info("%d IFREMER rain-cell file(s) in the requested period",
                    len(wanted))

        reports = []
        iterator = tqdm(wanted, desc="IFREMER rain cells", unit="file",
                        disable=not show_progress)
        for name, acquisition_time in iterator:
            path = output_dir / name
            try:
                if not (self.skip_existing and self._validate_files([path])[path]):
                    self._download(session, name, path)
                if not self._validate_files([path])[path]:
                    raise RuntimeError("downloaded file is not readable GeoJSON")
                reports.append(ItemDownloadReport(
                    data_source=self.data_source,
                    variable_name="rain_cells",
                    acquisition_time=acquisition_time,
                    polygon=polygon,
                    bbox=bbox,
                    path=path,
                    download_successful=True,
                    metadata=self._describe(path, bbox),
                ))
            except Exception as exc:  # noqa: BLE001 - keep going through the archive
                logger.warning("Failed on %s: %s", name, exc)
                reports.append(ItemDownloadReport(
                    data_source=self.data_source,
                    variable_name="rain_cells",
                    acquisition_time=acquisition_time,
                    polygon=polygon,
                    bbox=bbox,
                    path=path,
                    download_successful=False,
                    error=str(exc),
                ))
        return reports

    # -- internals ---------------------------------------------------------

    def _make_session(self) -> requests.Session:
        """Session that retries the throttling and gateway errors of the archive."""
        session = requests.Session()
        session.headers.update({"User-Agent": "fetcheo/ifremer_raincell"})
        retry = Retry(total=5, backoff_factor=1.0,
                      status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _list_directory(self, session: requests.Session) -> List[str]:
        """File names in the indexed HTTP directory."""
        response = session.get(self.base_url, timeout=self.timeout)
        response.raise_for_status()
        links = re.findall(r'href="([^"?#]+)"', response.text)
        return [
            link.rsplit("/", 1)[-1] for link in links
            if link.rsplit("/", 1)[-1].startswith(self.prefix)
            and link.endswith(self.suffix)
        ]

    def _download(self, session: requests.Session, name: str, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        response = session.get(urljoin(self.base_url, name), timeout=self.timeout,
                               stream=True)
        response.raise_for_status()
        with open(path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1 << 16):
                handle.write(chunk)

    @staticmethod
    def _acquisition_time(name: str) -> Optional[datetime.datetime]:
        """Mid-point of the acquisition, read from the Sentinel-1 file name."""
        match = _TIMES_RE.search(name)
        if not match:
            return None
        try:
            start = datetime.datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")
            end = datetime.datetime.strptime(match.group(2), "%Y%m%dT%H%M%S")
        except ValueError:
            return None
        return start + (end - start) / 2

    @staticmethod
    def _describe(path: Path, bbox: List[float]) -> dict:
        """Cell count and whether the detections reach the requested polygon."""
        try:
            with open(path, "r", encoding="utf-8") as handle:
                geojson = json.load(handle)
        except (OSError, json.JSONDecodeError):
            return {}

        features = geojson.get("features", [])
        lons: List[float] = []
        lats: List[float] = []

        def walk(node):
            if isinstance(node, (list, tuple)):
                if len(node) >= 2 and all(isinstance(v, (int, float)) for v in node[:2]) \
                        and not isinstance(node[0], (list, tuple)):
                    lons.append(float(node[0]))
                    lats.append(float(node[1]))
                else:
                    for child in node:
                        walk(child)

        for feature in features:
            walk((feature.get("geometry") or {}).get("coordinates", []))

        metadata = {"n_features": len(features)}
        if lons and lats and len(bbox) == 4:
            file_bbox = [min(lons), min(lats), max(lons), max(lats)]
            metadata["file_bbox"] = file_bbox
            metadata["intersects_polygon"] = not (
                file_bbox[2] < bbox[0] or file_bbox[0] > bbox[2]
                or file_bbox[3] < bbox[1] or file_bbox[1] > bbox[3]
            )
        return metadata
