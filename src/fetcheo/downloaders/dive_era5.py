from __future__ import annotations

from typing import Optional, Sequence

from fetcheo.downloaders.dive_pa import DIVEPlanktonAbundanceDownloader


class DIVEERA5Downloader(DIVEPlanktonAbundanceDownloader):
    """Download and export DIVE's daily ERA5-derived surface fields from Zenodo.

    The record contains ``spar`` (PAR, W/m²), ``tcc`` (total cloud cover), and
    ``tp`` (total precipitation, mm) on a daily ``(time, latitude, longitude)``
    grid covering 2008–2022. The source file is approximately 2.8 GB.
    """

    SOURCE_DOI = "10.5281/zenodo.23416271"
    DEFAULT_SOURCE = "https://zenodo.org/records/23416271"
    DEFAULT_VARIABLES = ("spar", "tcc", "tp")

    def __init__(
        self,
        source_url_or_path: str = DEFAULT_SOURCE,
        data_source_name: str = "DIVE_ERA5",
        variables: Optional[Sequence[str]] = DEFAULT_VARIABLES,
        time_batch_size: int = 30,
        verify_md5: bool = True,
    ):
        super().__init__(
            source_url_or_path=source_url_or_path,
            data_source_name=data_source_name,
            variables=variables,
            split_extra_dim_as_variables=False,
            time_batch_size=time_batch_size,
            verify_md5=verify_md5,
        )
