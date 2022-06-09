# Databricks notebook source
origen ='dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/ATPTX/ATPTX.txt'
path_delta = "/mnt/lakehylia-bronze/ops/regulatory/card-management/atptx"
path_procesados = 'dbfs:/mnt/lakehylia-bronze/ops/regulatory/card-management/FilesUpload/ATPTX/ATPTX_Processed/'
ts=spark.sql(""" select current_timestamp() - INTERVAL 6 HOUR  as ctime """).collect()[0]["ctime"]
dateFormat = "%Y%m%d"
atptxdia = path_procesados + 'ATPTX_' + ts.strftime(dateFormat) + '.txt'
