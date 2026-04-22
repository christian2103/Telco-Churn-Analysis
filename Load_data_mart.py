from pyspark.sql import SparkSession

# Init Spark
spark = SparkSession.builder \
    .appName("PipelineToNeonDB") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# JDBC config (centralized)
url = "jdbc:postgresql://ep-flat-dew-a18kk5pg-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require&tcpKeepAlive=true"

properties = {
    "user": "neondb_owner",
    "password": "npg_n2udKjfDxPU6",
    "driver": "org.postgresql.Driver",
    "batchsize": "1000",
    "numPartitions": "1"
}

# Function biar clean & konsisten
def write_to_db(df, table_name):
    df = df.coalesce(1) 
    
    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", table_name) \
        .options(**properties) \
        .mode("overwrite") \
        .save()

# Read parquet
dm_service = spark.read.parquet('/opt/airflow/data/dm_service_clean')
dm_customer = spark.read.parquet('/opt/airflow/data/dm_customer_clean')
dm_payment = spark.read.parquet('/opt/airflow/data/dm_payment_clean')
dm_subscription = spark.read.parquet('/opt/airflow/data/dm_subscription_clean')

# Write (urut dari kecil → besar)
write_to_db(dm_service, "dm_service")
write_to_db(dm_payment, "dm_payment")
write_to_db(dm_customer, "dm_customer")
write_to_db(dm_subscription, "dm_subscription")