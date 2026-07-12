@echo off
setlocal

set "PROJECT_DIR=%CD%"
set "PROJECT_DIR_UNIX=%PROJECT_DIR:\=/%"

set "PRODUCT_FILE=%PROJECT_DIR_UNIX%/temp_data/Product.csv"
set "COMMENT_FILE=%PROJECT_DIR_UNIX%/temp_data/Comments.csv"

echo ==========================================
echo Upload TEMP data to HDFS
echo ==========================================

echo Current project dir:
echo %PROJECT_DIR%

echo Product file:
echo %PRODUCT_FILE%

echo Comment file:
echo %COMMENT_FILE%

if not exist "%PROJECT_DIR%\temp_data\Product.csv" (
    echo ERROR: Missing local file: %PROJECT_DIR%\temp_data\Product.csv
    exit /b 1
)

if not exist "%PROJECT_DIR%\temp_data\Comments.csv" (
    echo ERROR: Missing local file: %PROJECT_DIR%\temp_data\Comments.csv
    exit /b 1
)

echo Creating HDFS temp folders...

call hdfs dfs -mkdir -p /ecommerce/raw_temp/products
call hdfs dfs -mkdir -p /ecommerce/raw_temp/comments
call hdfs dfs -mkdir -p /ecommerce/output_temp

echo Cleaning old HDFS temp raw files...

call hdfs dfs -rm -r -f /ecommerce/raw_temp/products/*
call hdfs dfs -rm -r -f /ecommerce/raw_temp/comments/*

echo Uploading temp Product.csv...
call hdfs dfs -put -f "%PRODUCT_FILE%" /ecommerce/raw_temp/products/

echo Uploading temp Comments.csv...
call hdfs dfs -put -f "%COMMENT_FILE%" /ecommerce/raw_temp/comments/

echo ==========================================
echo DONE upload TEMP data to HDFS
echo ==========================================

call hdfs dfs -ls -R /ecommerce/raw_temp

endlocal