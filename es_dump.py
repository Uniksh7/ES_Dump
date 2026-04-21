from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType

spark = SparkSession.builder \
    .appName("Healthcare_Reindex") \
    .config("spark.jars.packages",
            "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0") \
    .getOrCreate()


def read_csv(path):
    return spark.read.csv(path, header=True, inferSchema=True)


elig_df = read_csv("file:///home/unik/Downloads/did_data/elig_202305181247.csv")
med_df  = read_csv("file:///home/unik/Downloads/did_data/med_202305181247.csv")
rx_df   = read_csv("file:///home/unik/Downloads/did_data/rx_202305181249.csv")


def clean_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, c.lower().replace(" ", "_"))
    return df


elig_df = clean_columns(elig_df)
med_df  = clean_columns(med_df)
rx_df   = clean_columns(rx_df)


def convert_decimal(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, DecimalType):
            df = df.withColumn(field.name, col(field.name).cast("double"))
    return df


elig_df = convert_decimal(elig_df)
med_df  = convert_decimal(med_df)
rx_df   = convert_decimal(rx_df)


def ensure_pid(df):
    if "pid" not in df.columns:
        raise Exception("pid column missing")
    return df


elig_df = ensure_pid(elig_df)
med_df  = ensure_pid(med_df)
rx_df   = ensure_pid(rx_df)


elig_struct = elig_df.select(
    "pid",
    struct(*[col(c) for c in elig_df.columns if c != "pid"]).alias("elig")
)

med_struct = med_df.select(
    "pid",
    struct(*[col(c) for c in med_df.columns if c != "pid"]).alias("med")
).groupBy("pid").agg(collect_list("med").alias("med"))

rx_struct = rx_df.select(
    "pid",
    struct(*[col(c) for c in rx_df.columns if c != "pid"]).alias("rx")
).groupBy("pid").agg(collect_list("rx").alias("rx"))


final_df = elig_struct \
    .join(med_struct, "pid", "left") \
    .join(rx_struct, "pid", "left")


final_df = final_df \
    .withColumn("med", when(col("med").isNull(), array()).otherwise(col("med"))) \
    .withColumn("rx", when(col("rx").isNull(), array()).otherwise(col("rx")))


final_df.printSchema()
final_df.show(5, truncate=False)


final_df.write \
    .format("org.elasticsearch.spark.sql") \
    .option("es.nodes", "localhost") \
    .option("es.port", "9200") \
    .option("es.resource", "members_data") \
    .option("es.mapping.id", "pid") \
    .option("es.nodes.wan.only", "true") \
    .option("es.net.ssl", "false") \
    .mode("overwrite") \
    .save()


print("Members_data index successfully created")