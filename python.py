
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.master("local[*]").appName("ProductCategoryExample").getOrCreate()

products_data = [
    (1, "Product1"),
    (2, "Product2"),
    (3, "Product3"),
    (4, "Product4")
]
products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])

categories_data = [
    (1, "Category1"),
    (2, "Category2"),
    (3, "Category3")
]
categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])

product_category_data = [
    (1, 1),
    (1, 2),
    (2, 1),
    (3, 3)
]
product_category_df = spark.createDataFrame(product_category_data, ["product_id", "category_id"])

def get_product_category_pairs_and_orphans(products_df, categories_df, product_category_df):
    product_category_joined_df = products_df.join(product_category_df, "product_id", "left")

    product_category_pairs_df = product_category_joined_df.join(categories_df, "category_id", "left")
    
    product_category_pairs = product_category_pairs_df.select("product_name", "category_name").filter(col("category_name").isNotNull())
    
    products_no_category = product_category_pairs_df.select("product_name").filter(col("category_name").isNull())
    
    return product_category_pairs, products_no_category

product_category_pairs, products_no_category = get_product_category_pairs_and_orphans(products_df, categories_df, product_category_df)

print("Product-Category Pairs:")
product_category_pairs.show()

print("Products with No Category:")
products_no_category.show()