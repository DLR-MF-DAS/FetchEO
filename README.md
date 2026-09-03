# FetchEO

A work in progress repository for Earth Observation data downloading.

The pipeline has two stages, each backed by an extensible plugin registry:

```
downloaders  ->  file catalogue  ->  processors  ->  product catalogue
```

`fetcheo -d <downloader> -p <processor> --geojson_path aoi.geojson --start-date ... --end-date ...`


## Downloaders

The base downloader class is stored in src/fetcheo/downloaders/_downloader.py.

Some other notes:
- Downloaders currently expected to output geotiffs. Those that cannot (satellite
  swaths as NetCDF, vector detections as GeoJSON) validate their output with
  `BaseDownloader._validate_files` instead of `_validate_geotiff`.
- A few simpler downloaders are included, but generalisation of them could still be improved.
- Some require authentication variables (e.g. CDSE or OpenEO based downloaders).

| name | product | output |
|---|---|---|
| `era5` | ERA5-Land monthly reanalysis (CDS) | GeoTIFF |
| `modis_ndvi` | MODIS NDVI | GeoTIFF |
| `sen3_openeo` / `sen3_eodag` | Sentinel-3 water products | GeoTIFF |
| `cmems_sar_wind` | Sentinel-1 L3 ocean wind (Copernicus Marine) | NetCDF |

`cmems_sar_wind` needs an extra dependency: `pip install fetcheo[cmems]`, plus
Copernicus Marine credentials (`copernicusmarine login`).


## Processors

A processor turns downloaded files into a derived product. FetchEO ships the
stage but no scientific processor of its own: the layer exists so that an
analysis project can run inside the same pipeline without being merged into this
repository.

A processor receives a `ProcessingContext` — the polygon and time frame, where to
write, and every catalogued input file for that location, including files fetched
in **earlier** runs — and returns one `ItemProcessReport` per product. It declares
what it needs through `required_sources`, so the loader skips it with a clear
message instead of failing mid-run.

```python
from fetcheo.processors import BaseProcessor, ItemProcessReport

class NDVIStatsProcessor(BaseProcessor):
    required_sources = ("modis_ndvi",)

    @property
    def name(self):
        return "ndvi_stats"

    def process(self, context, show_progress=True):
        files = context.inputs_from("modis_ndvi")     # existing files, time-ordered
        out = context.output_dir / "ndvi_stats.csv"
        ...
        return [ItemProcessReport(
            processor=self.name,
            product_name="ndvi_stats",
            acquisition_time=None,                    # None = spans the period
            polygon=context.polygon,
            bbox=context.bbox,
            path=out,
            process_successful=True,
            inputs=[f.path for f in files],
        )]
```

Products are recorded in the `product_catalog` table, keyed on the processor that
produced them and carrying the list of input files for provenance.

A processor that raises is caught, reported as a failed product and logged: one
broken processor never sinks the run.


## Registering a plugin from outside FetchEO

Both stages use the same registry (`src/fetcheo/registry.py`). A downloader or a
processor can join in three ways, in increasing order of decoupling:

1. **Built in** — listed in `BUILTIN_DOWNLOADERS` / `BUILTIN_PROCESSORS`.

2. **Programmatic** — for a script or notebook driving FetchEO itself:

   ```python
   from fetcheo.registry import register_processor
   register_processor("ndvi_stats", NDVIStatsProcessor)
   ```

3. **Entry point** — the external package declares itself in its own packaging
   metadata and FetchEO discovers it with no import and no configuration:

   ```toml
   [project.entry-points."fetcheo.processors"]
   rain_cell_composite = "dive_fetcheo.plugin:RainCellCompositeProcessor"
   ```

   ```console
   $ fetcheo --help
     -p, --processor TEXT  ... Available: ['rain_cell_composite']
   ```

Targets are resolved lazily — a registry entry may be a class or the string
`"module:Class"`, imported only when the plugin is actually instantiated — so a
plugin with a heavy or missing optional dependency costs nothing to the others.

Third-party names cannot silently shadow a built-in one: `register(...)` refuses
to replace an existing entry unless called with `override=True`.
