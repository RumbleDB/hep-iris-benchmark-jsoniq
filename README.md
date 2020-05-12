# Root-Rumble Queries

This repository hosts the queries described [here](https://github.com/iris-hep/adl-benchmarks-index). The queries are written in the JSONiq programming language, and are meant to the executed on Rumble over Spark. The The goal is to show that the JSONiq can model complex queries with relative ease and high legibility. 

## Execution Issues

It may be the case that the following errors are encountered during the execution of the queries:

* `Spark java.lang.OutOfMemoryError: Java heap space` - In this case, it is suggested that the `spark.driver.memory` and `spark.executor.memory` are increased, for example to `8g` and `4g` respectively. These should be set in `<spark_install_dir>/conf/spark-defaults.conf`. 
* `Buffer overflow. Available: 0, required: xxx` - In this case, the issue likely stems from the Kryo framework. It is suggested that the `spark.kryoserializer.buffer.max` be set to something like `1024m`. This should be set in `<spark_install_dir>/conf/spark-defaults.conf`. 