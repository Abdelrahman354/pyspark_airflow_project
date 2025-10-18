from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SilverLayer:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("Silver Layer Cleaning") \
            .master("spark://spark-master:7077") \
            .config("spark.hadoop.fs.permissions.umask-mode", "000") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.bronze = "/usr/local/airflow/include/bronze_layer"
        self.silver = "/usr/local/airflow/include/silver_layer"
        logger.info("Spark session initialized")

    # ------------------- 1️1- Read DataFrame --------------------
    def read_parquet(self, dataset_name):
        path = f"{self.bronze}/{dataset_name}"
        df = self.spark.read.parquet(path)
        logger.info(f"Read {dataset_name}: {df.count()} rows")
        return df

    # -------------------2- Cleaning Functions --------------------
    def clean_customers(self, df):
        df_clean = df.select(
                col("customer_id"),
                col("customer_unique_id"),
                col("customer_zip_code_prefix").cast(IntegerType()).alias("zip_code"),
                initcap(col("customer_city")).alias("city"),
                upper(col("customer_state")).alias("state")
            ).dropDuplicates(["customer_id"]).na.drop()
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/customers")
        logger.info(f"Cleaned customers: {df_clean.count()} rows")

    def clean_geolocation(self, df):
        df_clean = df.select(
            col("geolocation_zip_code_prefix").cast(IntegerType()).alias("zip_prefix"),
            round(col("geolocation_lat"), 3).alias("latitude"),
            round(col("geolocation_lng"), 3).alias("longitude"),
            initcap(col("geolocation_city")).alias("city"),
            upper(col("geolocation_state")).alias("state")
        ).dropDuplicates(["zip_prefix"])
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/geolocation")
        logger.info(f"Cleaned geolocation: {df_clean.count()} rows")

    def clean_orders(self, df):
        df_clean = df.withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp")) \
            .withColumn("order_approved_at", to_timestamp("order_approved_at")) \
            .withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date")) \
            .withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date")) \
            .withColumn("delivery_delay", datediff("order_delivered_customer_date", "order_estimated_delivery_date")) \
            .na.drop(subset=["order_id", "customer_id"])
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/orders")
        logger.info(f"Cleaned orders: {df_clean.count()} rows")

    def clean_order_items(self, df):
        df_clean = df.withColumn("price", col("price").cast(DoubleType())) \
            .withColumn("freight_value", col("freight_value").cast(DoubleType())) \
            .na.drop(subset=["order_id", "product_id", "seller_id"])
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/order_items")
        logger.info(f"Cleaned order_items: {df_clean.count()} rows")

    def clean_payments(self, df):
        df_clean = (
        df.withColumn("payment_value", col("payment_value").cast(DoubleType()))
          .withColumn("payment_installments", col("payment_installments").cast(IntegerType()))
          .dropna(subset=["order_id"])
          .withColumn(
              "payment_type",
              when(col("payment_type") == "boleto", "bank slip")
              .otherwise(col("payment_type"))
          ))

        df_clean.write.mode("overwrite").parquet(f"{self.silver}/payments")
        logger.info(f"Cleaned payments: {df_clean.count()} rows")


    def clean_reviews(self, df):
        df_clean = df.withColumn("review_score", col("review_score").cast(IntegerType())) \
            .withColumn("review_creation_date", to_date("review_creation_date")) \
            .withColumn("review_answer_timestamp", to_timestamp("review_answer_timestamp")) \
            .na.drop(subset=["review_id", "order_id"])
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/reviews")
        logger.info(f"Cleaned reviews: {df_clean.count()} rows")

    def clean_sellers(self, df):
        df_clean = df.select(
            col("seller_id"),
            col("seller_zip_code_prefix").cast(IntegerType()).alias("zip_code"),
            initcap(col("seller_city")).alias("city"),
            upper(col("seller_state")).alias("state")
        ).dropDuplicates(["seller_id"])
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/sellers")
        logger.info(f"Cleaned sellers: {df_clean.count()} rows")

    def clean_products(self, df_prod, df_trans):
        df_clean = df_prod.join(df_trans, on="product_category_name", how="left") \
            .withColumn("product_category", coalesce("product_category_name_english", "product_category_name")) \
            .withColumn("product_weight_kg", col("product_weight_g") / 1000.0) \
            .withColumn("product_volume_cm3", col("product_length_cm") * col("product_height_cm") * col("product_width_cm")) \
            .na.fill({"product_category": "Unknown"})
        df_clean.write.mode("overwrite").parquet(f"{self.silver}/products")
        logger.info(f"Cleaned products: {df_clean.count()} rows")

    # -------------------- 3️3- Run ETL -------------------
    def run(self):
        # Read DataFrames
        df_customers = self.read_parquet("olist_customers_dataset")
        df_geolocation = self.read_parquet("olist_geolocation_dataset")
        df_orders = self.read_parquet("olist_orders_dataset")
        df_order_items = self.read_parquet("olist_order_items_dataset")
        df_payments = self.read_parquet("olist_order_payments_dataset")
        df_reviews = self.read_parquet("olist_order_reviews_dataset")
        df_sellers = self.read_parquet("olist_sellers_dataset")
        df_products = self.read_parquet("olist_products_dataset")
        df_trans = self.read_parquet("product_category_name_translation")

        # Clean DataFrames
        self.clean_customers(df_customers)
        self.clean_geolocation(df_geolocation)
        self.clean_orders(df_orders)
        self.clean_order_items(df_order_items)
        self.clean_payments(df_payments)
        self.clean_reviews(df_reviews)
        self.clean_sellers(df_sellers)
        self.clean_products(df_products, df_trans)

        logger.info("Silver layer cleaning completed successfully")

    def close(self):
        self.spark.stop()
        logger.info("Spark session closed")


if __name__ == "__main__":
    silver = SilverLayer()
    try:
        silver.run()
    finally:
        silver.close()
