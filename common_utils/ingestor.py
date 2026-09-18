

def read_jdbc(spark, url, dbtable, user, password, driver="com.microsoft.sqlserver.jdbc.SQLServerDriver"):
    '''
    Read a table from relational database over JDBC

    url         : connection url from source
    dbtable     : table you want to ingest from source
    user        : username of relational database
    password    : password of relational database
    driver      : driver from source
    '''

    df = spark.read.format("jdbc") \
    .option("url",url) \
    .option("dbtable",dbtable) \
    .option("user",user)\
    .option("password",password)\
    .option("driver", driver)\
    .load()

    return df



def read_cosmosdb_json(connection_string, database_name, collection_name ):
    '''
    
    '''

    client = MongoClient(connection_string)
    db = client[database_name]
    collection = db[collection_name]
    documents = list(collection.find({}))
    json_rows = []
    for document in documents:
        json_string = json_util.dumps(document)
        json_rows.append((json_string,))
    df = spark.createDataFrame(json_rows, ["json_data"])
    return df


def read_s3_parquet(aws_access_key_id, aws_secret_access_key, path):
    '''
    
    
    
    '''
    source_path = (
        f"s3a://{aws_access_key_id}:{aws_secret_access_key}"
        f"@{path}"
    )
    df = spark.read.parquet(source_path)
    return df



def write_raw(df, target_path,file_format, mode, options = None):
    '''
    
    
    
    '''

    writer = df.write.mode(mode).format(file_format)

    for key, value in (options or {}).items():
        writer = writer.option(key,value)

    writer.save(target_path)
    return target_path














