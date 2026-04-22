import kagglehub #Library untuk download dataset langsung dari Kaggle
import os #Library untuk operasi sistem (path, file, dll)
from pyspark.sql import SparkSession #Untuk membuat session PySpark

#Membuat Spark Session (entry point untuk menggunakan PySpark)
spark = SparkSession.builder \
    .appName("FinalProjectRMT015") \
    .getOrCreate() #Membuat atau mengambil session yang sudah ada

#Download dataset dari Kaggle menggunakan kagglehub
path = kagglehub.dataset_download("blastchar/telco-customer-churn")

#Membaca file CSV ke dalam DataFrame PySpark
df = spark.read.csv(
    path + "/WA_Fn-UseC_-Telco-Customer-Churn.csv",
    header=True,
    inferSchema=True
)

#Menyimpan DataFrame ke format Parquet
df.write.mode("overwrite").parquet("/opt/airflow/data/Telco_Customer_Churn_Raw")
#mode("overwrite") artinya jika folder sudah ada, akan ditimpa