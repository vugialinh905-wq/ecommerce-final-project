@echo off
setlocal

echo ==========================================
echo Run Flask Web with TEMP output
echo HDFS_OUTPUT_BASE=/ecommerce/output_temp
echo ==========================================

call conda activate tiki_bigdata

set "HADOOP_HOME=C:\Hadoop"
set "PATH=%HADOOP_HOME%\bin;%PATH%"
set "HDFS_OUTPUT_BASE=/ecommerce/output_temp"

python web\app.py

endlocal