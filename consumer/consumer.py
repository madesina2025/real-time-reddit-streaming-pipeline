from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import from_json, col
import os
from dotenv import load_dotenv

checkpoint_dir = "/tmp/checkpoint/kafka_to_mongo"
if not os.path.exists(checkpoint_dir):
  os.makedirs(checkpoint_dir)


config = {
    "mongodb": {
      "uri": "mongodb://mongodb:27017/kafka_stream.retail_data",
    }
}

# The schema matching the data coming from Kafka
kafka_data_schema = StructType([
    StructField("disaster_number", StringType()),
    StructField("declaration_date", StringType()),   # read as string first
    StructField("incident_type", StringType()),
    StructField("application_title", StringType()),
    StructField("applicant_id", StringType()),
    StructField("damage_categorycode", StringType()),
    StructField("damage_categorydescription", StringType()),
    StructField("project_status", StringType()),
    StructField("project_processStep", StringType()),
    StructField("project_size", StringType()),
    StructField("county", StringType()),
    StructField("first_obligation_date", StringType()),
    StructField("mitigation_amount", StringType()),
    StructField("gmProject_id", StringType()),
    StructField("gmApplicantId", StringType()),
    StructField("last_refresh", StringType()),
    StructField("hash", StringType())
])

spark = (SparkSession.builder
         .appName('KafkaSparkStreaming')
         .getOrCreate() 
)

df = ( spark.readStream.format('kafka')
  .option('kafka.bootstrap.servers', 'kafka:9092')
  .option('subscribe', 'retail')
  .option('startingOffsets', 'latest')
  .option('failOnDataLoss', 'false')
  .load()
)

# def process_batch(batch_df, batch_id):
#     # Convert Spark rows to plain Python dicts
#     rows = [row.asDict() for row in batch_df.collect()]
#     if rows:
#         write_batch_to_postgres(rows)


import psycopg2
from psycopg2.extras import execute_values


load_dotenv()

def write_batch_to_postgres(rows):
    # conn = psycopg2.connect(
    #     host="postgres",    # container name from docker-compose
    #     port=5432,
    #     dbname="XXXX",
    #     user="XXXX",
    #     password="XXXX",
    # )

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"), # container name from docker-compose
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()
    insert_sql = """
        INSERT INTO retail_data (
            disasternumber,
            declaration_date,
            incident_type,
            application_title,
            applicant_id,
            damage_categorycode,
            damage_categorydescription,
            project_status,
            project_processstep,
            project_size,
            county,
            first_obligation_date,
            mitigation_amount,
            last_refresh
        )
        VALUES %s
    """
    values = [
        (
            r["disasterNumber"],
            r.get("declaration_date"),
            r["incident_type"],
            r["application_title"],
            r["applicant_id"],
            r["damage_categorycode"],
            r["damage_categorydescription"],
            r["project_status"],
            r["project_processStep"],
            r["project_size"],
            r["county"],
            r.get("first_obligation_date"),
            float(r.get("mitigation_amount", 0)),
            r.get("last_refresh"),
        )
        for r in rows
    ]
    execute_values(cur, insert_sql, values)
    conn.commit()
    cur.close()
    conn.close()                                                                                                                                                                                    

def process_batch(batch_df, batch_id):
    rows = [row.asDict() for row in batch_df.collect()]
    if rows:
        write_batch_to_postgres(rows)
# spark = SparkSession.builder.appName('kafkaSparkStreaming').getOrCreate()

# df = (spark.readStream.format('kafka')
#    .option('kafka.bootstrap.servers', 'kafka:9092')
#    .option('subscribe', 'retail') 
#    .option('startingOffsets', 'latest')
#    .option('failonDataLoss', 'false')   
#    .load()
# )

#parsed_df = df.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)')


# Convert the 'value' column (which is a JSON string) into structured columns
# parsed_df = df.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)') \
#               .select(from_json(col("value"), kafka_data_schema).alias("data")) \
#               .select("data.*")



parsed_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), kafka_data_schema).alias("data"))
      .select("data.*")
      # rename & cast for DB / reporting
      .withColumnRenamed("declaration_date", "declaration_date")   # keeps same name, just explicit
      .withColumn(
          "disasterNumber",
          col("disaster_number").cast("int")                        # <– use source field here
      )
      .drop("disaster_number")                                      # optional: if you don't want both
)


# # Stream to MongoDB
# query = (
#   parsed_df.writeStream.format('mongodb')
#   .option('spark.mongodb.connection.uri', config['mongodb']['uri'])
#   # # Fix the syntax here: use dictionary access ['key'] not object access .key
#   # .option('spark.mongodb.database', config['mongodb']['database']) 
#   # .option('spark.mongodb.collection', config['mongodb']['collection'])
#   .option('checkpointLocation', checkpoint_dir)
#   .outputMode('append')
#   .start()
# )

# Stream to MongoDB
mongo_query = (
    parsed_df.writeStream.format("mongodb")
        .option("spark.mongodb.connection.uri", config["mongodb"]["uri"])
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
)

# Stream to PostgreSQL
pg_query = (
    parsed_df.writeStream
        .foreachBatch(process_batch)   # <--- this is Step 2 being used
        .outputMode("append")
        .start()
)

# Keep streams running
spark.streams.awaitAnyTermination()


# query.awaitTermination()
