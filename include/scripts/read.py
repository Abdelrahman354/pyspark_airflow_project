from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    
    # Use the mounted path available to all containers
    df = spark.read.csv("/usr/local/airflow/include/data.csv", header=True, inferSchema=True)
    print("✅ number of rows:", df.count())
    df.show(5)
    
    spark.stop()

if __name__ == "__main__":
    main()