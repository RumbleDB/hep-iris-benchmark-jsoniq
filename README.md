# Root: Rumble Queries

This repository hosts the queries described [here](https://github.com/iris-hep/adl-benchmarks-index). The queries are written in the JSONiq programming language, and are meant to the executed on Rumble over Spark. The primary goal is to show that JSONiq can model complex queries with relative ease and high legibility. The secondary goal of this project refers to identifying limitations of JSONiq and Rumble, and finding solutions for them. 


## Rumble

Rumble is a framework used for executing JSONiq queries on top of Spark.

### Installation

To install it, follow these [instructions](https://rumble.readthedocs.io/en/latest/Getting%20started/).

### The Data

Make sure you obtain a copy of the ROOT / parquet data in order to execute the queries (this can be downloaded from [here](https://polybox.ethz.ch/index.php/s/piULQwSvbjkwvJt)). Then make sure the query scripts point to the relevant data by changing the `dataPath` variable to the relevant value. 

It should be noted that for the purpose of these queries, I used the parquet file, as I would get an error when reading the ROOT version of the data.

### Running Queries

The scripts required to run queries with Rumble are located in the folder `rumble/scripts`:

* `run_job.sh`: used to execute rumble scripts in normal execution mode. Make sure to change the system paths in this script, such that they fit your file structure.
* `run_job_debug.sh`: used to execute rumble scripts in debug mode. Make sure to change the system paths in this script, such that they fit your file structure.
* `add_to_path.sh`: adds the `rumble/scripts` folder to the `PATH` environment variable, such that the scripts above can be executed from anywhere. This should be executed with the `source` command.

Assuming `add_to_path.sh` was executed in the shell, one needs to navigate to a query file (e.g. `rumble/index_based_queries/q1`), and execute the following command: `run_job.sh <query_file_name>`.  

### Query types

The `rumble` directory hosts two sub-folders: `entity_based_queries` and `index_based_queries`. Both are supposed to implement the same set of queries. The difference between the two, is that `entity_based_queries` employs a transformation of the data, such that each particle will be represented by a dictionary which encapsulates the particle's properties. That is to say, each particle will be represented by an object-like entity. The `index_based_queries` on the other hand use the available data directly, and employ classical indexing based traversals of the data. The former should technically be more readable, whereas the latter should be more efficient. 

### Known Execution Issues

It may be the case that the following errors are encountered during the execution of the queries:

* `Spark java.lang.OutOfMemoryError: Java heap space` - In this case, it is suggested that the `spark.driver.memory` and `spark.executor.memory` are increased, for example to `8g` and `4g` respectively. These should be set in `<spark_install_dir>/conf/spark-defaults.conf`. 
* `Buffer overflow. Available: 0, required: xxx` - In this case, the issue likely stems from the Kryo framework. It is suggested that the `spark.kryoserializer.buffer.max` be set to something like `1024m`. This should be set in `<spark_install_dir>/conf/spark-defaults.conf`. 
