from pyspark.sql import SparkSession #Untuk membuat Spark Session (entry point PySpark)
from pyspark.sql.functions import lower, trim, col, when #Import fungsi untuk transformasi data
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import regexp_replace, col

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("DataTransform") \
    .getOrCreate() #Membuat atau mengambil Spark session yang sudah ada

#Membaca file Parquet hasil sebelumnya
df = spark.read.parquet('/opt/airflow/data/Telco_Customer_Churn_Raw')

df = df.select([
    trim(col(c)).alias(c) if dict(df.dtypes)[c] == 'string' else col(c)
    for c in df.columns])

df = df.withColumn(
    "TotalCharges",
    when(trim(col("TotalCharges")) == "", None)
    .otherwise(col("TotalCharges"))
    .cast("double"))

df = df.withColumn(
    "TotalCharges",
    when(col("TotalCharges").isNull(), 0).otherwise(col("TotalCharges")))

df = df \
    .withColumnRenamed("customerID", "CustomerID") \
    .withColumnRenamed("gender", "Gender") \
    .withColumnRenamed("tenure", "Tenure")

dim_service = df.select('PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 
                        'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies')

dim_service = dim_service.withColumn('Service_id', monotonically_increasing_id())

dim_payment = df.select('Contract',
                        'PaperlessBilling',
                        'PaymentMethod')

dim_payment = dim_payment.withColumn('Payment_id', monotonically_increasing_id())

dim_payment = dim_payment.withColumn(
    "is_automatic",
    when(col("PaymentMethod").contains("automatic"), "yes").otherwise("no"))

dim_payment = dim_payment.withColumn(
    "PaymentMethod",
    regexp_replace(col("PaymentMethod"), r"\s*\(automatic\)", ""))

dim_customer = df.select('CustomerID', 'Gender', 'Partner', 'SeniorCitizen', 'Dependents')

fact_subscription = df.select('CustomerID', 'Tenure', 'MonthlyCharges', 'TotalCharges', 'Churn')
fact_subscription = fact_subscription.withColumn('id', monotonically_increasing_id())

fact_subscription = fact_subscription.withColumn('Service_id', monotonically_increasing_id())
fact_subscription = fact_subscription.withColumn('Payment_id', monotonically_increasing_id())

fact_subscription = fact_subscription.select('id', 'CustomerID', 'Service_id', 'Payment_id', 'Tenure', 'MonthlyCharges', 'TotalCharges', 'Churn')
dim_payment = dim_payment.select('Payment_id', 'Contract', 'PaperlessBilling', 'PaymentMethod', 'is_automatic')
dim_service = dim_service.select('Service_id', 'PhoneService', 'MultipleLines', 'InternetService', 'OnlineSecurity', 
                        'OnlineBackup', 'DeviceProtection', 'TechSupport', 'StreamingTV', 'StreamingMovies')

dim_customer.write.mode("overwrite").parquet("/opt/airflow/data/dim_customer")
dim_service.write.mode("overwrite").parquet("/opt/airflow/data/dim_service")
dim_payment.write.mode("overwrite").parquet("/opt/airflow/data/dim_payment")
fact_subscription.write.mode("overwrite").parquet("/opt/airflow/data/fact_subscription")