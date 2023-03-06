# KGX Export for Text Mined Assertions
A set of scripts for exporting the Targeted Assertions database as KGX-compatible TSV files.
```
usage: exporter.py [-h] -t TARGET -uni UNIPROT_BUCKET [-i INSTANCE] [-d DATABASE] [-u USER] [-p PASSWORD] [-c CHUNK_SIZE] [-l LIMIT] [-ao ASSERTION_OFFSET] [-al ASSERTION_LIMIT] [-v]

optional arguments:
  -h, --help            show this help message and exit
  -t TARGET, --target TARGET
                        the export target: edges, nodes, or metadata
  -uni UNIPROT_BUCKET, --uniprot_bucket UNIPROT_BUCKET
                        storage bucket for UniProt data
  -i INSTANCE, --instance INSTANCE
                        GCP DB instance name
  -d DATABASE, --database DATABASE
                        database name
  -u USER, --user USER  database username
  -p PASSWORD, --password PASSWORD
                        database password
  -c CHUNK_SIZE, --chunk_size CHUNK_SIZE
                        number of assertions to process at a time
  -l LIMIT, --limit LIMIT
                        maximum number of publications to export per edge
  -ao ASSERTION_OFFSET, --assertion_offset ASSERTION_OFFSET
                        number of assertions to skip past
  -al ASSERTION_LIMIT, --assertion_limit ASSERTION_LIMIT
                        number of assertions to output
  -v, --verbose
```
Note that, despite being listed under "optional arguments", the ```target``` and ```uniprot_bucket``` parameters are always required.
If the ```target``` is ```edges``` or ```nodes``` then the database parameters (```instance```, ```database```, ```user```, ```password```) are also required.
Additionally, the script will look for a file named ```prod-creds.json``` in the working directory, which should be a valid credentials file with permissions to access the Google Cloud Storage bucket where the exported files will be stored.