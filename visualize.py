import re
import config
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import regexp_extract, col, udf
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql import functions as F

raw_data_files = config.RAW_DATA_FILES
spark_context = SparkContext()
sql_context = SQLContext(spark_context)
spark = SparkSession(spark_context)
base_dataframe = spark.read.text(raw_data_files)
base_dataframe_rdd = base_dataframe.rdd
sample_logs = [item['value'] for item in base_dataframe.take(15)]


def extract_hosts():
    host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
    hosts = [re.search(host_pattern, item).group(1) if re.search(host_pattern, item) else 'no match' for item in
             sample_logs]
    return hosts, host_pattern


def extract_timestamps():
    timestamps_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
    timestamps = [re.search(timestamps_pattern, item).group(1) for item in sample_logs]
    return timestamps, timestamps_pattern


def extract_method_uri_protocol():
    method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
    method_uri_protocol = [
        re.search(method_uri_protocol_pattern, item).groups() if re.search(method_uri_protocol_pattern,
                                                                           item) else 'no match' for item in sample_logs
    ]
    return method_uri_protocol, method_uri_protocol_pattern


def extract_status():
    status_pattern = r'\s(\d{3})\s'
    status = [re.search(status_pattern, item).group(1) for item in sample_logs]
    return status, status_pattern


def extract_content_size():
    content_size_pattern = r'\s(\d+)$'
    content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]
    return content_size, content_size_pattern


def extract_logs_df():
    logs_df = base_dataframe.select(regexp_extract('value', extract_hosts()[1], 1).alias('host'),
                                    regexp_extract('value', extract_timestamps()[1], 1).alias('timestamp'),
                                    regexp_extract('value', extract_method_uri_protocol()[1], 1).alias('method'),
                                    regexp_extract('value', extract_method_uri_protocol()[1], 2).alias('endpoint'),
                                    regexp_extract('value', extract_method_uri_protocol()[1], 3).alias('protocol'),
                                    regexp_extract('value', extract_status()[1], 1).cast('integer').alias('status'),
                                    regexp_extract('value', extract_content_size()[1], 1).cast('integer').alias(
                                        'content_size'))
    # logs_df.show(10, truncate=True)
    logs_count = logs_df.count()
    logs_len = len(logs_df.columns)

    return logs_df


def find_missing_values():
    missing_values = base_dataframe.filter(base_dataframe['value'].isNull()).count()

    return missing_values


def find_bad_rows():
    logs_df = extract_logs_df()
    bad_rows_df = logs_df.filter(logs_df['host'].isNull() |
                                 logs_df['timestamp'].isNull() |
                                 logs_df['method'].isNull() |
                                 logs_df['endpoint'].isNull() |
                                 logs_df['status'].isNull() |
                                 logs_df['content_size'].isNull() |
                                 logs_df['protocol'].isNull())

    return bad_rows_df.count()


def count_null(col_names):
    return spark_sum(col(col_names).isNull().cast('integer')).alias(col_names)

    # to call this method:
    # exprs = [count_null(col_name) for col_name in logs_df.columns]


def handling_http_status_nulls():
    null_status_df = base_dataframe.filter(~base_dataframe['value'].rlike(r'\s(\d{3})\s'))
    null_status_df.count()
    null_content_size_df = base_dataframe.filter(~base_dataframe['value'].rlike(r'\s\d+$'))
    null_count_size = null_content_size_df.count()
    # null_status_df.show(truncate=False)

    return null_status_df


def find_pipeline_nulls():
    null_status_df = handling_http_status_nulls()
    bad_status_df = null_status_df.select(regexp_extract('value', extract_hosts()[1], 1).alias('host'),
                                          regexp_extract('value', extract_timestamps()[1], 1).alias(
                                              'timestamp'),
                                          regexp_extract('value', extract_method_uri_protocol()[1], 1).alias(
                                              'method'),
                                          regexp_extract('value', extract_method_uri_protocol()[1], 2).alias(
                                              'endpoint'),
                                          regexp_extract('value', extract_method_uri_protocol()[1], 3).alias(
                                              'protocol'),
                                          regexp_extract('value', extract_status()[1], 1).cast(
                                              'integer').alias(
                                              'status'),
                                          regexp_extract('value', extract_content_size()[1], 1).cast(
                                              'integer').alias(
                                              'content_size'))

    bad_status_df.show(truncate=False)


def drop_null_record():
    logs_df = extract_logs_df()
    logs_df = logs_df[logs_df['status'].isNotNull()]
    exprs = [count_null(col_name) for col_name in logs_df.columns]
    logs_df.agg(*exprs).show()

    return logs_df


def fill_null_values():
    logs_df = drop_null_record()
    logs_df = logs_df.na.fill({'content_size': 0})
    exprs = [count_null(col_names=col_name) for col_name in logs_df.columns]
    # logs_df.agg(*exprs).show()

    return logs_df


def parse_clf_time(text):
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
        int(text[7:11]),
        config.MONTH_MAP[text[3:6]],
        int(text[0:2]),
        int(text[12:14]),
        int(text[15:17]),
        int(text[18:20])
    )

udf_parse_time = udf(parse_clf_time)
logs_df = fill_null_values()
logs_df = (logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp'))
logs_df.cache()

# logs_df.show(10, truncate=True)
# print(logs_df.printSchema())
