This is a lightweight python utility designed to aggregate data from the OCADS data portal(https://www.ncei.noaa.gov/products/ocean-carbon-acidification-data-system)

An up-to-date index of the contents of the OCADS data portal is available in JSON form at this url: https://www.ncei.noaa.gov/data/oceans/ncei/ocads/ocads_metadata.json

OCADS data can be accessed using the following url structure: https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/[accession_number]

Example:

This is the JSON object for a single dataset entry from the JSON index of the contents of the OCADS dat aportal linked above:

```JSON
{
    "accession_number" : "0000071",
    "lonlat_count" : 5796,
    "lonlat_url" : "https://www.ncei.noaa.gov/archive/accession/0000071/about/0000071_lonlat.txt",
    "modified_date" : "2021-10-15T01:19:45Z",
    "type" : [
        "Surface underway"
    ],
    "xml_url_iso-19115-2" : "https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.nodc:0000071;view=xml;responseType=text/xml",
    "xml_url_ocads" : "https://www.ncei.noaa.gov/data/oceans/ncei/ocads/metadata/xml/0000071.xml"
}
```

As you can see from the JSON object, the accession_number for this dataset is `0000071`. Data for this dataset would then be available at the following url:

https://www.ncei.noaa.gov/data/oceans/ncei/ocads/data/0000071

# Usage

1. Create the python environment: `virtualenv ocads_agg`
2. Activate the environment: `source ocads_agg/bin/activate`
3. Install the dependencies: `pip install -r requirements.txt`
4. Run the project: `python ocads_agg.py -h`

```
usage: OCADS_agg [-h] [-o OUTPUT_DIR] [-t NUM_THREADS]

options:
  -h, --help            show this help message and exit
  -o, --output_dir OUTPUT_DIR
                        A directory to store the aggregated OCADS data in. If no directory is provided the current working directory will be used.
  -t, --num_threads NUM_THREADS
                        The number of threads to be used in aggregating data
```