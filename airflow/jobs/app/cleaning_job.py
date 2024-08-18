from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
from pyspark.sql.functions import col, explode, flatten, concat, substring, lit, to_date

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Wikipedia-cleaning") \
    .getOrCreate()

def read_landing_data(path):
    df = spark.read.json(path)
    return df

## Article Length
df_articles_length = read_landing_data("lake/landing/articles_length.json")
df_flatten = df_articles_length.select(
    col("query.pages.`31113163`.pageid").alias("pageid"),
    col("query.pages.`31113163`.ns").alias("namespace"),
    col("query.pages.`31113163`.title").alias("title"),
    col("query.pages.`31113163`.contentmodel").alias("content_model"),
    col("query.pages.`31113163`.pagelanguage").alias("page_language"),
    col("query.pages.`31113163`.pagelanguagehtmlcode").alias("page_language_html_code"),
    col("query.pages.`31113163`.pagelanguagedir").alias("page_language_dir"),
    col("query.pages.`31113163`.touched").alias("touched"),
    col("query.pages.`31113163`.lastrevid").alias("last_revision_id"),
    col("query.pages.`31113163`.length").alias("length")
)

write_path = "lake/cleaning/articles_length/"
try:
    df_flatten.write.mode('overwrite').parquet(write_path)
    print("Cleaning - articles_length - criada com sucesso!")
except Exception as err:
    print(err)


## contributors by language
df_contributors = read_landing_data("lake/landing/contributors_by_language.json")
df_contributors = df_contributors.select(explode("values"))
df_contributors_final = df_contributors.select(
    col("col")[0].alias("language"),
    col("col")[1].alias("qtd")
)
try:
    write_path = "lake/cleaning/contributors_by_languade/"
    df_contributors_final.write.mode('overwrite').parquet(write_path)
    print("Cleaning - contributors_by_language")
except Exception as err:
    print(err)

## Reviews
df_reviews = read_landing_data("lake/landing/reviews.json")
df_reviews = df_reviews.select("query.pages.31113163.*")
df_exploded = df_reviews.select(explode("revisions").alias("revisions"))
df_reviews_final = df_exploded.select(
    col("revisions.anon").alias("anon"),
    col("revisions.comment").alias("comment"),
    col("revisions.minor").alias("minor"),
    col("revisions.parentid").alias("parentid"),
    col("revisions.revid").alias("revid"),
    col("revisions.timestamp").alias("timestamp"),
    col("revisions.user").alias("user"),
)

try:
    write_path = "lake/cleaning/reviews/"
    df_reviews_final.write.mode('overwrite').parquet(write_path)
    print("Cleaning - reviews - criada com sucesso!")
except Exception as err:
    print(err)

## Statistic Page

df_statistic = read_landing_data("lake/landing/statistic_page.json")
df_statistic_exploded = df_statistic.select(explode("items").alias("items"))
df_statistic_final = df_statistic_exploded.select(
    col("items.access").alias("access"),
    col("items.agent").alias("agent"),
    col("items.article").alias("article"),
    col("items.granularity").alias("granularity"),
    col("items.project").alias("project"),
    to_date(col("items.timestamp").substr(1, 8), "yyyyMMdd").alias("formatted_access_date"),
    col("items.views").alias("views")
)

try:
    write_path = "lake/cleaning/statistic_page/"
    df_statistic_final.write.mode('overwrite').parquet(write_path)
    print("Cleaning - statistic_page - criada com sucesso!")
except Exception as err:
    print(err)