# Description
A python script for PySpark which can merge small files with the DHFS block size.
# Usage
```shell
spark-submit --name MergeFiles file_merge.py tableconfig
# Table config like : table_a:0:-1,table_b:0:-2 means all partitions except recent 2. 
```