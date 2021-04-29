# -*- coding:utf-8 -*-
# @Time : 2019-07-01
# @Author : 长风
# @Function : 小文件合并v2版本

from pyspark.sql import SparkSession
import subprocess
import math
import argparse
import logging.config
import json


def get_dtype(table_name, column_name):
    """
    获取字段类型
    """
    df = spark.sql(f"select * from {table_name}")
    return [dtype for name, dtype in df.dtypes if name == column_name][0]


def get_where_filter(table_name, part):
    """
    生成where子句
    """
    where_filters = []
    part_splits = part.split("/")
    for part_split in part_splits:
        colunm_name, value = part_split.split("=")[0], part_split.split("=")[1]
        column_type = get_dtype(table_name, colunm_name)
        if column_type in ("string", "varchar"):
            where_filters.append(f"{colunm_name}='{value}'")
        else:
            where_filters.append(f"{colunm_name}={value}")

    return " and ".join(where_filters)


def get_table_location(table_name):
    """
    获取表Location
    """
    table_location = (spark
        .sql(f"describe formatted {table_name}")
        .filter("col_name='Location' and data_type like 'hdfs://%'")
        .select("data_type").collect()[0][0])
    return table_location


def get_parts_list(table_name):
    parts = spark.sql(f"show partitions {table_name}").collect()
    parts = [r["partition"] for r in parts]

    return parts


def get_meta_part(table_location, part):
    """
    获取分区元数据
    """
    cmd = f"sudo -u hdfs hadoop fs -count {table_location}/{part} |  awk " + "'{printf $2 \"@\" $3}'"
    result = (subprocess
              .check_output(cmd
                            , shell=True)
              ).decode("utf-8").split("@")

    part_num_files = int(result[0])
    part_size = int(result[1])

    return get_where_filter(table_name, part), part_num_files, part_size


def get_meta_parts(table_location, parts):
    """
    获取所有分区元数据
    """
    parts_summary = []
    for part in parts:
        parts_summary.append(get_meta_part(table_location, part))
    return parts_summary


def compact_table(table_name, parts_summary, block_size):
    """
    合并表，仅仅合并文件数量大于计算后的分区数量，以及平均大小大于块大小
    """
    for where_filter, num_files, part_size in parts_summary:
        n_parts = math.ceil(part_size / block_size)
        avg_size = math.ceil(part_size / num_files)
        if num_files > n_parts or avg_size > block_size:
            part_df = spark.sql(f"select * from {table_name} where {where_filter}").checkpoint()
            (
                part_df
                    .repartition(n_parts)
                    .write
                    .insertInto(table_name, overwrite=True)
            )
            logger.info(
                f"Complete repartitioning for partition {where_filter} with n_parts = {n_parts} and part_size = {part_size}")
        else:
            logger.info(
                f"Skip repartition where {where_filter}; current num_files is {num_files} & avg_size is {avg_size}")


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    spark = (SparkSession
             .builder
             .appName("tool.compactor")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
             .config("hive.exec.dynamic.partition", "true")
             .config("hive.exec.dynamic.partition.mode", "nonstrict")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true")
             .config("spark.hadoop.hive.exec.stagingdir", "/tmp/compactor")
             .getOrCreate()
             )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark started")
    spark.sparkContext.setCheckpointDir("/tmp")
    parser = argparse.ArgumentParser(description="Spark application to to compact data on Hive")
    parser.add_argument("--conf", type=str, required=True, help="""
    Table config like : table_a:0:-1,table_b:0:-2 means all partitions except recent 2. 
    """)

    args = parser.parse_args()
    table_config_list = args.conf.split(",")

    for table_config in table_config_list:
        table_name, partition_start, partition_end = table_config.split(":")
        logger.info(f"Start compacting for table: {table_name}")
        spark.sql(f"REFRESH TABLE {table_name}")

        default_block_size = int(spark.sparkContext._jsc.hadoopConfiguration().get("dfs.blocksize"))
        block_size = default_block_size if default_block_size is not None else 134217728

        table_location = get_table_location(table_name)
        logger.info(f"Table location: {table_location}")

        parts = get_parts_list(table_name)
        logger.info(f"Partition list: {parts}")

        logger.info(f"Getting partition total size and number of files ...")
        parts_summary = get_meta_parts(table_location, parts)[partition_start:partition_end]
        if parts_summary:
            logger.info("Parts summary: " + parts_summary)
            logger.info("Start compacting ...")
            compact_table(table_name, parts_summary, block_size)
        else:
            logger.info("Partitions not exists ... ")

    spark.stop()
    logger.info("Spark stopped")

