import pyspark.sql.functions as F
from common_utils.logging import get_logger

logger = get_logger("common_utils_bronze")


def read_raw(spark, raw_path, file_format, options = None):
    """
    Read Raw files from volume

    raw_path        : /Volumes/retaildataplatform/bronze/raw_data/sqlserver_customers/load_date=2026-09-07/
    file_format     : "json" or "csv" or "parquet"
    options         : key value

    returns a dataframe 
    
    """
    logger.info("Reading from path : %s", raw_path)
    logger.info("File Format : %s", file_format)

    reader = spark.read.format(file_format)
    for key,value in (options or {}).items():
        reader = reader.option(key,value)

    return reader.load(raw_path)
    logger.info("Read Complete and data stored in Dataframe df")
    

    

def bronze_ingestor(df, mode, target_table):
    """
    Add Audit Colummns and Write in bronze as delta table
    
    df              : Dataframe read from raw volume
    target_table    : catalog.schema.table
    mode            : "overwrite" or "append"

    Return the no. of rows written
    """
    logger.info("Adding Columns in dataframe")

    df = df.withColumn("last_update_ts", F.current_timestamp() )\
        .withColumn("file_path", F.col("_metadata.file_path"))

    
    logger.info("Writing Delta Table : %s", target_table)
    logger.info("Selected mode is  : %s", mode)
    df.write.format("delta").mode(mode).saveAsTable(target_table)
    return df.count()







