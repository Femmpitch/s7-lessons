from datetime import datetime, timedelta
import sys
 
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

import pyspark
from pyspark.sql import SparkSession

DATA_DIR = "/user/s18314377/data/events"

def input_paths(date, depth):
    
    start_date = datetime.strptime(date, "%Y-%m-%d")
    
    result = []
    for i in range(depth):
        current_date = start_date - timedelta(days=i)
        result.append(f"{DATA_DIR}/date={current_date.strftime('%Y-%m-%d')}/event_type=message")
    
    return result


def main():
    inputs_7 = input_paths(date="2022-05-31", depth=7)
    
    data_7 = spark.read.parquet(\\*inputs_7)
    
    
    conf = SparkConf().setAppName(f"EventsPartitioningJob-{date}")
    sc = SparkContext(conf=conf)
    sql = SQLContext(sc)

    
    
    
if __name__ == "__main__":
    main()