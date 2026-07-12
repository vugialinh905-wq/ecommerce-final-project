@echo off
setlocal

echo ==========================================
echo Run Flask Web with MAIN output
echo HDFS_OUTPUT_BASE=/ecommerce/output
echo ==========================================

set "HDFS_OUTPUT_BASE=/ecommerce/output"

python web\app.py

endlocal