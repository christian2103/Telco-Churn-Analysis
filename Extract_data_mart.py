from pyspark.sql import SparkSession

# Inisialisasi Spark + driver PostgreSQL
spark = SparkSession.builder \
    .appName("PipelineToNeonDB") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# Konfigurasi koneksi NeonDB
NEONDB_URL = "jdbc:postgresql://ep-flat-dew-a18kk5pg-pooler.ap-southeast-1.aws.neon.tech:5432/neondb?sslmode=require"
NEONDB_USER = "neondb_owner"
NEONDB_PASSWORD = "npg_n2udKjfDxPU6"
NEONDB_DRIVER = "org.postgresql.Driver"

# Fungsi helper untuk load tabel dari NeonDB
def load_table(table_name: str):
    return spark.read \
        .format("jdbc") \
        .option("url", NEONDB_URL) \
        .option("dbtable", table_name) \
        .option("user", NEONDB_USER) \
        .option("password", NEONDB_PASSWORD) \
        .option("driver", NEONDB_DRIVER) \
        .load()

# Load tabel
dim_customer = load_table("dim_customer")
dim_payment = load_table("dim_payment")
dim_service = load_table("dim_service")
fact_subscription = load_table("fact_subscription")

# Buat dictionary untuk mapping DataFrame ke nama file parquet
dfs_to_save = {
    "/opt/airflow/data/dim_customer_dm": dim_customer,
    "/opt/airflow/data/dim_payment_dm": dim_payment,
    "/opt/airflow/data/dim_service_dm": dim_service,
    "/opt/airflow/data/fact_subscription_dm": fact_subscription
}

# Loop untuk menulis semua DataFrame ke parquet
for path, df in dfs_to_save.items():
    df.write.mode("overwrite").parquet(path)