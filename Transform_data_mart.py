from pyspark.sql import SparkSession #Untuk membuat Spark Session (entry point PySpark)
from pyspark.sql.functions import lower, trim, col, when #Import fungsi untuk transformasi data
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import regexp_replace, col

# Inisialisasi Spark
spark = SparkSession.builder \
    .appName("DataTransform") \
    .getOrCreate() #Membuat atau mengambil Spark session yang sudah ada

#Membaca file Parquet hasil sebelumnya
dm_customer = spark.read.parquet('/opt/airflow/data/dim_customer_dm')
dm_payment = spark.read.parquet('/opt/airflow/data/dim_payment_dm')
dm_service = spark.read.parquet('/opt/airflow/data/dim_service_dm')
dm_subscription = spark.read.parquet('/opt/airflow/data/fact_subscription_dm')

dm_subscription_clean = dm_subscription.select('CustomerID', 'Service_id', 'Payment_id', 'Tenure', 'MonthlyCharges', 'TotalCharges', 'Churn')
dm_payment_clean = dm_payment.select('Payment_id', 'Contract', 'PaymentMethod')
dm_service_clean = dm_service.select('Service_id', 'InternetService')
dm_customer_clean = dm_customer.select('CustomerID', 'Gender', 'Partner', 'SeniorCitizen', 'Dependents')

dm_customer_clean.write.mode("overwrite").parquet("/opt/airflow/data/dm_customer_clean")
dm_service_clean.write.mode("overwrite").parquet("/opt/airflow/data/dm_service_clean")
dm_payment_clean.write.mode("overwrite").parquet("/opt/airflow/data/dm_payment_clean")
dm_subscription_clean.write.mode("overwrite").parquet("/opt/airflow/data/dm_subscription_clean")