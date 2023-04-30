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


class LogAnalyzer:
    def __init__(self,
                 raw_data_files=config.RAW_DATA_FILES):
        self.spark_context = SparkContext()
        self.sql_context = SQLContext(self.spark_context)
        self.spark = SparkSession(self.spark_context)
        self.raw_data_files = raw_data_files
        self.base_dataframe = self.spark.read.text(self.raw_data_files)
        self.base_dataframe_rdd = self.base_dataframe.rdd
        self.sample_logs = [item['value'] for item in self.base_dataframe.take(15)]

    def extract_hosts(self):
        host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
        hosts = [re.search(host_pattern, item).group(1) if re.search(host_pattern, item) else 'no match' for item in self.sample_logs]
        return hosts, host_pattern

    def extract_timestamps(self):
        timestamps_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
        timestamps = [re.search(timestamps_pattern, item).group(1) for item in self.sample_logs]
        return timestamps, timestamps_pattern

    def extract_method_uri_protocol(self):
        method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
        method_uri_protocol = [
            re.search(method_uri_protocol_pattern, item).groups() if re.search(method_uri_protocol_pattern, item) else 'no match' for item in self.sample_logs
        ]
        return method_uri_protocol, method_uri_protocol_pattern

    def extract_status(self):
        status_pattern = r'\s(\d{3})\s'
        status = [re.search(status_pattern, item).group(1) for item in self.sample_logs]
        return status, status_pattern

    def extract_content_size(self):
        content_size_pattern = r'\s(\d+)$'
        content_size = [re.search(content_size_pattern, item).group(1) for item in self.sample_logs]
        return content_size, content_size_pattern

    def extract_logs_df(self):
        logs_df = self.base_dataframe.select(regexp_extract('value', self.extract_hosts()[1], 1).alias('host'),
                                             regexp_extract('value', self.extract_timestamps()[1], 1).alias('timestamp'),
                                             regexp_extract('value', self.extract_method_uri_protocol()[1], 1).alias('method'),
                                             regexp_extract('value', self.extract_method_uri_protocol()[1], 2).alias('endpoint'),
                                             regexp_extract('value', self.extract_method_uri_protocol()[1], 3).alias('protocol'),
                                             regexp_extract('value', self.extract_status()[1], 1).cast('integer').alias('status'),
                                             regexp_extract('value', self.extract_content_size()[1], 1).cast('integer').alias(
                                                 'content_size'))
        return logs_df

    def find_missing_values(self):
        missing_values = self.base_dataframe.filter(self.base_dataframe['value'].isNull()).count()

        return missing_values

    def find_bad_rows(self):
        logs_df = self.extract_logs_df()
        bad_rows_df = logs_df.filter(logs_df['host'].isNull() |
                                     logs_df['timestamp'].isNull() |
                                     logs_df['method'].isNull() |
                                     logs_df['endpoint'].isNull() |
                                     logs_df['status'].isNull() |
                                     logs_df['content_size'].isNull() |
                                     logs_df['protocol'].isNull())

        return bad_rows_df.count()

    def count_null(self, col_names):
        return spark_sum(col(col_names).isNull().cast('integer')).alias(col_names)
        # to call this method:
        # exprs = [count_null(col_name) for col_name in logs_df.columns]

    def handling_http_status_nulls(self):
        null_status_df = self.base_dataframe.filter(~self.base_dataframe['value'].rlike(r'\s(\d{3})\s'))
        null_status_df.count()
        null_content_size_df = self.base_dataframe.filter(~self.base_dataframe['value'].rlike(r'\s\d+$'))
        # null_count_size = null_content_size_df.count()
        # null_status_df.show(truncate=False)

        return null_status_df

    def find_pipeline_nulls(self):
        null_status_df = self.handling_http_status_nulls()
        bad_status_df = null_status_df.select(regexp_extract('value', self.extract_hosts()[1], 1).alias('host'),
                                              regexp_extract('value', self.extract_timestamps()[1], 1).alias('timestamp'),
                                              regexp_extract('value', self.extract_method_uri_protocol()[1], 1).alias('method'),
                                              regexp_extract('value', self.extract_method_uri_protocol()[1], 2).alias('endpoint'),
                                              regexp_extract('value', self.extract_method_uri_protocol()[1], 3).alias('protocol'),
                                              regexp_extract('value', self.extract_status()[1], 1).cast('integer').alias(
                                                  'status'),
                                              regexp_extract('value', self.extract_content_size()[1], 1).cast('integer').alias(
                                                  'content_size'))

        return bad_status_df.show(truncate=False)

    def drop_null_record(self):
        logs_df = self.extract_logs_df()
        logs_df = logs_df[logs_df['status'].isNotNull()]
        exprs = [self.count_null(col_name) for col_name in logs_df.columns]
        logs_df.agg(*exprs).show()

        return logs_df

    def fill_null_values(self):
        logs_df = self.drop_null_record()
        logs_df = logs_df.na.fill({'content_size': 0})
        exprs = [self.count_null(col_names=col_name) for col_name in logs_df.columns]
        logs_df.agg(*exprs).show()

        return logs_df