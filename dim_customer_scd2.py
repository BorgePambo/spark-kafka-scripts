from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =========================
# PATHS
# =========================
silver_path = "abfss://container@storage.dfs.core.windows.net/silver/customer"
gold_path = "abfss://container@storage.dfs.core.windows.net/gold/dim_customer"

# =========================
# 1. CRIAR TABELA GOLD SE NÃO EXISTIR (ENTERPRISE)
# =========================
spark.sql(f"""
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id STRING,
    customer_name STRING,
    email STRING,
    phone STRING,
    birthdate DATE,
    row_hash STRING,
    is_current BOOLEAN,
    effective_from TIMESTAMP,
    end_date TIMESTAMP
)
USING DELTA
LOCATION '{gold_path}'
""")

# =========================
# 2. LER SILVER (pode ser CDC também se habilitado)
# =========================
silver_df = spark.read.format("delta").load(silver_path)

# pegar última versão por cliente
w = Window.partitionBy("customer_id").orderBy(F.col("regis_date").desc())

silver_df = silver_df.withColumn("rn", F.row_number().over(w)) \
    .filter("rn = 1") \
    .drop("rn")

# =========================
# 3. STAGING + HASH (detecção de mudança)
# =========================
staging_df = silver_df.withColumn(
    "row_hash",
    F.sha2(F.concat_ws("||",
        F.coalesce(F.lower("customer_name"), F.lit("NA")),
        F.coalesce(F.lower("email"), F.lit("NA")),
        F.coalesce(F.lower("phone"), F.lit("NA"))
    ), 256)
).withColumn("is_current", F.lit(True)) \
 .withColumn("effective_from", F.current_timestamp()) \
 .withColumn("end_date", F.lit(None).cast("timestamp"))

# =========================
# 4. MERGE SCD2 (ENTERPRISE PATTERN)
# =========================
gold_table = DeltaTable.forPath(spark, gold_path)

gold_table.alias("t").merge(
    staging_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    condition="t.row_hash <> s.row_hash",
    set={
        "end_date": "current_timestamp()",
        "is_current": "false"
    }
).whenNotMatchedInsert(
    values={
        "customer_id": "s.customer_id",
        "customer_name": "s.customer_name",
        "email": "s.email",
        "phone": "s.phone",
        "birthdate": "s.birthdate",
        "row_hash": "s.row_hash",
        "is_current": "true",
        "effective_from": "current_timestamp()",
        "end_date": "null"
    }
).execute()

# =========================
# 5. OTIMIZAÇÃO (IMPORTANTE EM PRODUÇÃO)
# =========================
spark.sql(f"OPTIMIZE dim_customer ZORDER BY (customer_id, is_current)")
