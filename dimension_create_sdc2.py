from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# =========================
# CONFIG
# =========================

silver_path = "abfss://container@storage.dfs.core.windows.net/silver/patient"
gold_dim_patient = "abfss://container@storage.dfs.core.windows.net/gold/dim_patient"
gold_dim_department = "abfss://container@storage.dfs.core.windows.net/gold/dim_department"
gold_fact = "abfss://container@storage.dfs.core.windows.net/gold/fact_admission"

# =========================
# 1. LEITURA INCREMENTAL
# =========================

silver_df = spark.read.format("delta").load(silver_path)

silver_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .load(silver_path)


# =========================
# 2. DEDUPLICAÇÃO
# =========================

w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())

silver_df = (
    silver_df
    .withColumn("rn", F.row_number().over(w))
    .filter("rn = 1")
    .drop("rn")
)

# =========================
# 3. STAGING (PATIENT)
# =========================

staging_patient = (
    silver_df
    .select("patient_id", "gender", "age")
    .withColumn(
        "row_hash",
        F.sha2(
            F.concat_ws("||",
                F.coalesce("gender", F.lit("NA")),
                F.coalesce(F.col("age").cast("string"), F.lit("NA"))
            ),
            256
        )
    )
)

# =========================
# 4. CREATE TABLE (SE NÃO EXISTIR)
# =========================

if not DeltaTable.isDeltaTable(spark, gold_dim_patient):

    spark.sql(f"""
    CREATE TABLE delta.`{gold_dim_patient}` (
        surrogate_key BIGINT GENERATED ALWAYS AS IDENTITY,
        patient_id INT,
        gender STRING,
        age INT,
        row_hash STRING,
        effective_from TIMESTAMP,
        effective_to TIMESTAMP,
        is_current BOOLEAN
    )
    """)

    staging_patient \
        .withColumn("effective_from", F.current_timestamp()) \
        .withColumn("effective_to", F.lit(None).cast("timestamp")) \
        .withColumn("is_current", F.lit(True)) \
        .write.format("delta").mode("append").save(gold_dim_patient)

# =========================
# 5. SCD2 → FECHAR REGISTROS
# =========================

dim_patient = DeltaTable.forPath(spark, gold_dim_patient)

(
    dim_patient.alias("t")
    .merge(
        staging_patient.alias("s"),
        "t.patient_id = s.patient_id AND t.is_current = true"
    )
    .whenMatchedUpdate(
        condition="t.row_hash <> s.row_hash",
        set={
            "is_current": "false",
            "effective_to": "current_timestamp()"
        }
    )
    .execute()
)

# =========================
# 6. INSERT → NOVOS + ALTERADOS
# =========================

updates_inserts = (
    staging_patient.alias("s")
    .join(
        spark.read.format("delta").load(gold_dim_patient).alias("t"),
        (F.col("s.patient_id") == F.col("t.patient_id")) & (F.col("t.is_current") == True),
        "left"
    )
    .where(
        F.col("t.patient_id").isNull() |
        (F.col("s.row_hash") != F.col("t.row_hash"))
    )
    .select("s.*")
)

(
    updates_inserts
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("effective_to", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
    .write.format("delta")
    .mode("append")
    .save(gold_dim_patient)
)

# =========================
# 7. DIM DEPARTMENT (SCD1)
# =========================

dim_department = (
    silver_df
    .select("department", "hospital_id")
    .dropDuplicates()
)

dim_department.write.format("delta").mode("append").save(gold_dim_department)

# =========================
# 8. FACT TABLE (INCREMENTAL)
# =========================

dim_patient_df = (
    spark.read.format("delta").load(gold_dim_patient)
    .filter("is_current = true")
    .select(
        F.col("surrogate_key").alias("patient_sk"),
        "patient_id"
    )
)

dim_dept_df = (
    spark.read.format("delta").load(gold_dim_department)
    .select(
        F.monotonically_increasing_id().alias("department_sk"),
        "department",
        "hospital_id"
    )
)

fact_df = (
    silver_df
    .join(dim_patient_df, "patient_id", "left")
    .join(dim_dept_df, ["department", "hospital_id"], "left")
    .withColumn("length_of_stay_hours",
        (F.unix_timestamp("discharge_time") - F.unix_timestamp("admission_time")) / 3600
    )
    .withColumn("event_time", F.current_timestamp())
)

fact_df.write.format("delta").mode("append").save(gold_fact)

# =========================
# 9. OTIMIZAÇÃO
# =========================

spark.sql(f"OPTIMIZE delta.`{gold_dim_patient}` ZORDER BY (patient_id)")
spark.sql(f"OPTIMIZE delta.`{gold_fact}` ZORDER BY (patient_sk)")
