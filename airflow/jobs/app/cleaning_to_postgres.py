from pyspark.sql import SparkSession

# Criando a sessão Spark
spark = SparkSession.builder \
    .appName("Parquet to PostgreSQL") \
    .config("spark.jars", "postgresql-42.5.6.jar") \
    .getOrCreate()

# Ler o arquivo parquet do path
tables_list = [
    {'table_name': 'statistic_page', 'path': 'lake/cleaning/statistic_page'},
    {'table_name': 'articles_length', 'path': 'lake/cleaning/articles_length'},
    {'table_name': 'contributors_by_languade', 'path': 'lake/cleaning/contributors_by_languade'},
    {'table_name': 'reviews', 'path': 'lake/cleaning/reviews'}
]

for tl in tables_list:
    table_name = tl['table_name']
    path = tl['path']
    df = spark.read.parquet(path)
    
    # Configurações de conexão com o PostgreSQL
    postgres_url = "jdbc:postgresql://localhost:5432/postgres"
    postgres_properties = {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    
    # Escrever o DataFrame no PostgreSQL
    df.write.format("jdbc") \
        .option("url", postgres_url) \
        .option("dbtable", f"public.{table_name}") \
        .option("user", postgres_properties["user"]) \
        .option("password", postgres_properties["password"]) \
        .option("driver", postgres_properties["driver"]) \
        .mode("overwrite") \
        .save()
    
    print("Dados gravados com sucesso no PostgreSQL.")