@echo off
setlocal

echo ==========================================
echo Run Spark with TEMP data using Conda PySpark
echo ==========================================

call conda activate tiki_bigdata

set "HADOOP_HOME=C:\Hadoop"
set "PYSPARK_PYTHON=%CONDA_PREFIX%\python.exe"
set "PYSPARK_DRIVER_PYTHON=%CONDA_PREFIX%\python.exe"
set "PATH=%HADOOP_HOME%\bin;%PATH%"

call spark-submit ^
  --master "local[1]" ^
  --conf spark.python.worker.reuse=false ^
  --conf spark.sql.shuffle.partitions=4 ^
  --conf spark.driver.memory=2g ^
  spark_jobs\process_tiki_spark.py ^
  --products hdfs://localhost:9000/ecommerce/raw_temp/products/ ^
  --comments hdfs://localhost:9000/ecommerce/raw_temp/comments/ ^
  --output hdfs://localhost:9000/ecommerce/output_temp

echo ==========================================
echo DONE Spark TEMP processing
echo ==========================================

call hdfs dfs -ls -R /ecommerce/output_temp

endlocal