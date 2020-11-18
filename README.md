# Root: Rumble Queries

This repository hosts the queries described [here](https://github.com/iris-hep/adl-benchmarks-index). The queries are written in the JSONiq programming language, and are meant to be executed on Rumble over Spark. The primary goal is to show that JSONiq can model complex queries with relative ease and high legibility. The secondary goal of this project refers to identifying limitations of JSONiq and Rumble, and finding solutions for them.

## Prerequisites

* [Docker](https://docs.docker.com/engine/install/) or an installation of Rumble (see its [documentation](https://rumble.readthedocs.io/en/latest/Getting%20started/))
* Python 3 with pip

## Setup

1. Install the Python requirements:
   ```bash
   pip3 install -r requirements.txt
   ```
1. Configure `rumble.sh` or `start-server.sh`. The simplest is to copy the provided files that use docker:
   ```bash
   cp rumble.docker.sh rumble.sh
   cp start-server.docker.sh start-server.sh
   ```
   Alternatively, copy the `*.local.sh` variants and modify them to contain the correct paths.
1. If you want to use a long-running Rumble server, start it:
   ```bash
   ./start-server.sh
   ```

## Data

The benchmark defines the queries against the following data set:

```
root://eospublic.cern.ch//eos/root-eos/benchmark/Run2012B_SingleMu.root
```

For getting started quickly, we provide two samples of the file converted to Parquet are included in [`data/`](/data/). Since Rumble supports [reading ROOT files directly](https://rumble.readthedocs.io/en/latest/Input/#root), doing so only requires a minimal change in the query implementations.

### Local Processing

Instead of processing the file in-place through the `root://` protocol, you may download it to local storage using [`xrdcp`](https://linux.die.net/man/1/xrdcp) (which you can install by [installing the ROOT framework](https://root.cern/install/)).

### Extracting a Sample

In order to extract a samle for faster processing, you can use `rooteventselector` and its `-f` and `-l` flags (which is part of the ROOT framework as well).

### Converting to Parquet

You may convert the ROOT files into Parquet (or other formats) using `tools/root2parquet.py`. This downloads the specified package from the internet, so if you are behind a proxy, follow [this](https://stackoverflow.com/a/36676963).

```bash
spark-submit \
    --packages edu.vanderbilt.accre:laurelin:1.1.1 \
    tools/root2parquet.py \
        -i data/Run2012B_SingleMu.root \
        -0 data/Run2012B_SingleMu.parquet
```

### Naming Convention for this Implementation

`test_queries.py` looks for the input files in `data/` with names of the form `Run2012B_SingleMu{suffix}.parquet`, where `{suffix}` is empty for the full data set and `-{num_events}` for a sample of `{num_events}`. It also looks for reference results in `queries/{query_name}/ref{suffix}.csv` with the same `{suffix}`. It also looks for reference results in `queries/{query_name}/ref{suffix}.csv` with the same `{suffix}`..

## Running Queries

Run all queries on the full data set using `rumble.sh` from above with the following command:

```bash
./test_queries.py -v
```

Run following to see more options

```
$ ./test_queries.py --help
usage: test_queries.py [options] [file_or_dir] [file_or_dir] [...]

...
custom options:
  -Q QUERY_ID, --query-id=QUERY_ID
                        Folder name of query to run.
  -N NUM_EVENTS, --num-events=NUM_EVENTS
                        Number of events taken from the input file. This influences which reference file should be taken.
  -I INPUT_PATH, --input-path=INPUT_PATH
                        Path to input ROOT file.
  --rumble-cmd=RUMBLE_CMD
                        Path to spark-submit.
  --rumble-server=RUMBLE_SERVER
                        Rumble server to connect to.
  --freeze-result       Overwrite reference result.
  --plot-histogram      Plot resulting histogram as PNG file.
...
```

For example, to run all queries containing `o-` on the test data set with 150 events using a local server, do the following:

```bash
./test_queries.py -v -N 150 --rumble-server http://localhost:8001/jsoniq -k o-6-1
```

## Query Types

The `rumble` directory hosts two sub-folders: `entity_based_queries` and `index_based_queries`. Both are supposed to implement the same set of queries. The difference between the two is that `entity_based_queries` employs a transformation of the data, such that each particle will be represented by a dictionary which encapsulates the particle's properties. That is to say, each particle will be represented by an object-like entity. The `index_based_queries` on the other hand use the available data directly and employ classical indexing-based traversals of the data. The former should technically be more readable, whereas the latter should be more efficient.

## Known Issues

It may be the case that the following errors are encountered during the execution of the queries:

* `Spark java.lang.OutOfMemoryError: Java heap space` - In this case, it is suggested that the `spark.driver.memory` and `spark.executor.memory` are increased, for example to `8g` and `4g` respectively. These should be set in `<spark_install_dir>/conf/spark-defaults.conf`. 
* `Buffer overflow. Available: 0, required: xxx` - In this case, the issue likely stems from the Kryo framework. It is suggested that the `spark.kryoserializer.buffer.max` be set to something like `1024m`. This should be set in `<spark_install_dir>/conf/spark-defaults.conf`. 
