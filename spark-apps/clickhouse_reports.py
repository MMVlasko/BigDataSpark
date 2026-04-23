from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window


class ClickHouseReports:
    def __init__(self):
        self.spark = self.create_spark_session()
        self.clickhouse_url = "jdbc:clickhouse://clickhouse:8123/default"
        self.clickhouse_properties = {
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
            "user": "clickhouse",
            "password": "clickhouse"
        }

    def create_spark_session(self):
        spark = SparkSession.builder \
            .appName("ClickHouse_Reports") \
            .config("spark.jars.packages",
                    "org.postgresql:postgresql:42.7.1,"
                    "com.clickhouse:clickhouse-jdbc:0.4.6") \
            .getOrCreate()
        return spark

    def read_fact_table(self):
        pg_url = "jdbc:postgresql://postgres-db:5432/postgres"
        pg_properties = {
            "user": "postgres",
            "password": "postgres",
            "driver": "org.postgresql.Driver"
        }

        query = """
        SELECT 
            f.sale_date,
            f.sale_quantity,
            f.sale_total_price,
            c.category as product_category,
            p.id as product_id,
            p.product_name,
            p.product_rating,
            p.product_reviews,
            p.product_price,
            cust.customer_first_name,
            cust.customer_last_name,
            cust.customer_email,
            cust.customer_country,
            s.store_name,
            s.store_city,
            s.store_country as store_country,
            sup.supplier_name,
            sup.supplier_country
        FROM fact_sale f
        LEFT JOIN dim_category c ON f.category_id = c.id
        LEFT JOIN dim_product p ON f.product_id = p.id
        LEFT JOIN dim_customer cust ON f.customer_id = cust.id
        LEFT JOIN dim_store s ON f.store_id = s.id
        LEFT JOIN dim_supplier sup ON f.supplier_id = sup.id
        WHERE f.sale_date IS NOT NULL
        """

        return self.spark.read.jdbc(url=pg_url, table=f"({query}) as sales_data",
                                    properties=pg_properties)

    def write_to_clickhouse(self, df, table_name):
        numeric_columns = [field.name for field in df.schema.fields
                           if field.dataType.typeName() in ['integer', 'long', 'float', 'double', 'decimal']]

        for col_name in numeric_columns:
            df = df.withColumn(col_name, when(col(col_name).isNull(), lit(0)).otherwise(col(col_name)))

        df.write \
            .format("jdbc") \
            .option("url", self.clickhouse_url) \
            .option("dbtable", table_name) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .option("user", "clickhouse") \
            .option("password", "clickhouse") \
            .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()") \
            .mode("overwrite") \
            .save()

    def create_reports(self):
        df = self.read_fact_table()

        print("\n=== Creating Product Sales Reports ===")

        top_10_products = df.groupBy("product_name") \
            .agg(
            sum("sale_quantity").alias("total_quantity_sold"),
            sum("sale_total_price").alias("total_revenue"),
            count("*").alias("number_of_transactions")
        ) \
            .orderBy(col("total_quantity_sold").desc()) \
            .limit(10)
        self.write_to_clickhouse(top_10_products, "top_10_products")
        print("- top_10_products created")

        revenue_by_category = df.groupBy("product_category") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_quantity_sold"),
            count("*").alias("number_of_sales"),
            avg("sale_total_price").alias("avg_order_value")
        ) \
            .orderBy(col("total_revenue").desc())
        self.write_to_clickhouse(revenue_by_category, "revenue_by_category")
        print("- revenue_by_category created")

        product_ratings_reviews = df.groupBy("product_id", "product_name") \
            .agg(
            avg("product_rating").alias("avg_rating"),
            max("product_rating").alias("max_rating"),
            min("product_rating").alias("min_rating"),
            first("product_reviews").alias("total_reviews"),
            sum("sale_quantity").alias("times_sold"),
            sum("sale_total_price").alias("total_revenue")
        ) \
            .orderBy(col("avg_rating").desc())
        self.write_to_clickhouse(product_ratings_reviews, "product_ratings_reviews")
        print("- product_ratings_reviews created")

        top_10_customers = df.groupBy("customer_email", "customer_first_name",
                                      "customer_last_name", "customer_country") \
            .agg(
            sum("sale_total_price").alias("total_spent"),
            count("*").alias("number_of_purchases"),
            avg("sale_total_price").alias("avg_order_value"),
            sum("sale_quantity").alias("total_items_bought")
        ) \
            .orderBy(col("total_spent").desc()) \
            .limit(10)
        self.write_to_clickhouse(top_10_customers, "top_10_customers")
        print("- top_10_customers created")

        customer_distribution = df.groupBy("customer_country") \
            .agg(
            countDistinct("customer_email").alias("customer_count"),
            sum("sale_total_price").alias("total_revenue_from_country"),
            avg("sale_total_price").alias("avg_customer_spending")
        ) \
            .orderBy(col("customer_count").desc())
        self.write_to_clickhouse(customer_distribution, "customer_distribution")
        print("- customer_distribution created")

        avg_check_per_customer = df.groupBy("customer_email", "customer_first_name",
                                            "customer_last_name") \
            .agg(
            avg("sale_total_price").alias("avg_check_value"),
            sum("sale_total_price").alias("total_spent"),
            count("*").alias("number_of_purchases")
        ) \
            .withColumn("median_check_value", col("avg_check_value")) \
            .orderBy(col("avg_check_value").desc())
        self.write_to_clickhouse(avg_check_per_customer, "avg_check_per_customer")
        print("- avg_check_per_customer created")

        monthly_trends = df.withColumn("year_month", date_format(col("sale_date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_quantity"),
            avg("sale_total_price").alias("avg_order_value"),
            count("*").alias("number_of_orders"),
            countDistinct("customer_email").alias("unique_customers")
        ) \
            .orderBy("year_month")
        self.write_to_clickhouse(monthly_trends, "monthly_trends")
        print("- monthly_trends created")

        yearly_trends = df.withColumn("year", year("sale_date")) \
            .groupBy("year") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_quantity"),
            avg("sale_total_price").alias("avg_order_value"),
            count("*").alias("number_of_orders"),
            countDistinct("customer_email").alias("unique_customers")
        ) \
            .orderBy("year")
        self.write_to_clickhouse(yearly_trends, "yearly_trends")
        print("- yearly_trends created")

        monthly_with_prev = df.withColumn("year_month", date_format(col("sale_date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(sum("sale_total_price").alias("current_revenue")) \
            .orderBy("year_month")

        window_spec = Window.orderBy("year_month")
        monthly_comparison = monthly_with_prev \
            .withColumn("prev_month_revenue", lag("current_revenue", 1).over(window_spec)) \
            .withColumn("prev_month_revenue",
                        when(col("prev_month_revenue").isNull(), lit(0))
                        .otherwise(col("prev_month_revenue"))) \
            .withColumn("revenue_growth",
                        when(col("prev_month_revenue") == 0, lit(0))
                        .otherwise(
                            ((col("current_revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100)) \
            .withColumn("revenue_change",
                        when(col("prev_month_revenue").isNull(), lit(0))
                        .otherwise(col("current_revenue") - col("prev_month_revenue"))) \
            .withColumn("revenue_change",
                        when(col("revenue_change").isNull(), lit(0))
                        .otherwise(col("revenue_change")))

        self.write_to_clickhouse(monthly_comparison, "monthly_revenue_comparison")
        print("- monthly_revenue_comparison created")

        avg_order_size_monthly = df.withColumn("year_month", date_format(col("sale_date"), "yyyy-MM")) \
            .groupBy("year_month") \
            .agg(
            avg("sale_quantity").alias("avg_items_per_order"),
            sum("sale_quantity").alias("total_items"),
            count("*").alias("total_orders")
        ) \
            .withColumn("median_items_per_order", col("avg_items_per_order")) \
            .orderBy("year_month")
        self.write_to_clickhouse(avg_order_size_monthly, "avg_order_size_monthly")
        print("- avg_order_size_monthly created")

        print("\n=== Creating Store Sales Reports ===")

        top_5_stores = df.groupBy("store_name") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_items_sold"),
            count("*").alias("number_of_transactions"),
            countDistinct("customer_email").alias("unique_customers"),
            avg("sale_total_price").alias("avg_order_value")
        ) \
            .orderBy(col("total_revenue").desc()) \
            .limit(5)
        self.write_to_clickhouse(top_5_stores, "top_5_stores")
        print("- top_5_stores created")

        sales_by_city = df.groupBy("store_city", "store_country") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_items_sold"),
            count("*").alias("number_of_transactions"),
            countDistinct("store_name").alias("number_of_stores"),
            avg("sale_total_price").alias("avg_order_value")
        ) \
            .orderBy(col("total_revenue").desc())
        self.write_to_clickhouse(sales_by_city, "sales_by_city")
        print("- sales_by_city created")

        sales_by_country = df.groupBy("store_country") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_items_sold"),
            count("*").alias("number_of_transactions"),
            countDistinct("store_name").alias("number_of_stores"),
            countDistinct("store_city").alias("number_of_cities")
        ) \
            .orderBy(col("total_revenue").desc())
        self.write_to_clickhouse(sales_by_country, "sales_by_country")
        print("- sales_by_country created")

        avg_check_per_store = df.groupBy("store_name", "store_city", "store_country") \
            .agg(
            avg("sale_total_price").alias("avg_check_value"),
            sum("sale_total_price").alias("total_revenue"),
            count("*").alias("number_of_transactions")
        ) \
            .withColumn("median_check_value", col("avg_check_value")) \
            .withColumn("min_check_value", col("avg_check_value")) \
            .withColumn("max_check_value", col("avg_check_value")) \
            .orderBy(col("avg_check_value").desc())
        self.write_to_clickhouse(avg_check_per_store, "avg_check_per_store")
        print("- avg_check_per_store created")

        print("\n=== Creating Supplier Sales Reports ===")

        top_5_suppliers = df.groupBy("supplier_name", "supplier_country") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_items_sold"),
            count("*").alias("number_of_transactions"),
            avg("sale_total_price").alias("avg_order_value"),
            countDistinct("product_name").alias("distinct_products_sold")
        ) \
            .orderBy(col("total_revenue").desc()) \
            .limit(5)
        self.write_to_clickhouse(top_5_suppliers, "top_5_suppliers")
        print("- top_5_suppliers created")

        avg_price_per_supplier = df.groupBy("supplier_name") \
            .agg(
            avg("product_price").alias("avg_product_price"),
            min("product_price").alias("min_product_price"),
            max("product_price").alias("max_product_price"),
            countDistinct("product_name").alias("distinct_products"),
            avg("product_rating").alias("avg_product_rating")
        ) \
            .orderBy(col("avg_product_price").desc())
        self.write_to_clickhouse(avg_price_per_supplier, "avg_price_per_supplier")
        print("- avg_price_per_supplier created")

        supplier_sales_by_country = df.groupBy("supplier_country") \
            .agg(
            sum("sale_total_price").alias("total_revenue"),
            sum("sale_quantity").alias("total_items_sold"),
            count("*").alias("number_of_transactions"),
            countDistinct("supplier_name").alias("number_of_suppliers"),
            avg("sale_total_price").alias("avg_order_value"),
            avg("product_price").alias("avg_product_price")
        ) \
            .orderBy(col("total_revenue").desc())
        self.write_to_clickhouse(supplier_sales_by_country, "supplier_sales_by_country")
        print("- supplier_sales_by_country created")

        print("\n=== Creating Product Quality Reports ===")

        highest_rated_products = df.select("product_id", "product_name", "product_rating", "product_reviews") \
            .distinct() \
            .filter(col("product_rating").isNotNull()) \
            .orderBy(col("product_rating").desc())
        self.write_to_clickhouse(highest_rated_products, "highest_rated_products")
        print("- highest_rated_products created")

        lowest_rated_products = df.select("product_id", "product_name", "product_rating", "product_reviews") \
            .distinct() \
            .filter((col("product_rating") > 0) & col("product_rating").isNotNull()) \
            .orderBy(col("product_rating").asc())
        self.write_to_clickhouse(lowest_rated_products, "lowest_rated_products")
        print("- lowest_rated_products created")

        rating_sales_correlation = df.groupBy("product_rating") \
            .agg(
            avg("sale_quantity").alias("avg_quantity_by_rating"),
            avg("sale_total_price").alias("avg_revenue_by_rating"),
            countDistinct("product_id").alias("number_of_products_in_rating")
        ) \
            .filter(col("product_rating").isNotNull()) \
            .orderBy("product_rating")
        self.write_to_clickhouse(rating_sales_correlation, "rating_sales_correlation")
        print("- rating_sales_correlation created")

        most_reviewed_products = df.select("product_id", "product_name", "product_reviews", "product_rating") \
            .distinct() \
            .filter(col("product_reviews").isNotNull()) \
            .orderBy(col("product_reviews").desc())
        self.write_to_clickhouse(most_reviewed_products, "most_reviewed_products")
        print("- most_reviewed_products created")

        print("\n" + "=" * 60)
        print("- All ClickHouse reports created successfully!")
        print("=" * 60)
