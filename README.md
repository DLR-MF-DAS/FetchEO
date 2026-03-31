# FetchEO

A work in progress repository for Earth Observation data downloading.


## Downloaders

The base downloader class is stored in src/fetcheo/downloaders/_downloader.py. A few simpler downloaders are included, but generalisation of them could still be improved.

Some require authentication variables (e.g. CDSE or OpenEO based downloaders).