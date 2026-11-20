"""
Spark: limpieza + enriquecimiento de transacciones (zona RAW -> zona CURATED).

Entrada : JSONL/CSV crudo (S3/MinIO con s3a://... o carpeta local)
Salidas : - <salida>/curated/transacciones (parquet, particionado por fecha)
          - <salida>/export/transacciones_enriquecidas.csv   -> BigQuery / Snowflake
          - <salida>/export/cuentas.csv                      -> Neo4j (nodos) + BigQuery
          - <salida>/export/aristas.csv                      -> Neo4j (relaciones)

Uso local (sin S3):
  python spark/limpieza_enriquecimiento.py --entrada data/dataset_fraude.csv --salida out
Uso con MinIO/S3:
  spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/limpieza_enriquecimiento.py \
      --entrada s3a://fraude-datalake/raw/ --salida out --s3
"""
import argparse
import glob
import os
import shutil

from pyspark.sql import SparkSession, functions as F


def exportar_csv(df, carpeta, nombre):
    """Escribe un unico CSV (coalesce 1) con nombre fijo."""
    tmp = os.path.join(carpeta, "_tmp_" + nombre)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
    parte = glob.glob(os.path.join(tmp, "part-*.csv"))[0]
    destino = os.path.join(carpeta, nombre)
    if os.path.exists(destino):
        os.remove(destino)
    shutil.move(parte, destino)
    shutil.rmtree(tmp)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", required=True)
    ap.add_argument("--salida", default="out")
    ap.add_argument("--s3", action="store_true", help="leer desde MinIO/S3 (s3a)")
    ap.add_argument("--endpoint", default="http://localhost:9000")
    ap.add_argument("--access-key", default="rustfsadmin")
    ap.add_argument("--secret-key", default="rustfsadmin")
    a = ap.parse_args()

    b = (SparkSession.builder.appName("fraude-limpieza-enriquecimiento")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "2g")
         .config("spark.ui.enabled", "false"))
    if a.s3:
        b = (b.config("spark.hadoop.fs.s3a.endpoint", a.endpoint)
              .config("spark.hadoop.fs.s3a.access.key", a.access_key)
              .config("spark.hadoop.fs.s3a.secret.key", a.secret_key)
              .config("spark.hadoop.fs.s3a.path.style.access", "true")
              .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------ 1. LECTURA (RAW)
    if a.entrada.endswith(".csv"):
        raw = spark.read.option("header", True).csv(a.entrada)
    else:
        raw = spark.read.json(a.entrada)
    n_raw = raw.count()

    # ------------------------------------------------------------ 2. LIMPIEZA
    tx = (raw
          .select([F.trim(F.col(c).cast("string")).alias(c) for c in raw.columns])
          .withColumn("monto", F.col("monto").cast("double"))
          .withColumn("fecha_hora", F.to_timestamp("fecha_hora"))
          .withColumn("es_fraude", F.col("es_fraude").cast("int"))
          .dropna(subset=["id_tx", "fecha_hora", "monto", "cuenta_origen", "cuenta_destino"])
          .filter(F.col("monto") > 0)
          .filter(F.col("cuenta_origen") != F.col("cuenta_destino"))
          .dropDuplicates(["id_tx"]))

    # ------------------------------------------------------------ 3. ENRIQUECIMIENTO
    tx = (tx
          .withColumn("fecha", F.to_date("fecha_hora"))
          .withColumn("hora", F.hour("fecha_hora"))
          .withColumn("dia_semana", F.date_format("fecha_hora", "E"))
          .withColumn("es_nocturna", ((F.col("hora") >= 23) | (F.col("hora") <= 4)).cast("int"))
          .withColumn("ts_epoch", F.col("fecha_hora").cast("long")))
    tx.cache()
    n_ok = tx.count()
    print(f"[LIMPIEZA] filas crudas={n_raw} | filas validas={n_ok} | descartadas={n_raw - n_ok}")

    # features por cuenta (solo TRANSFER: es lo que forma el grafo)
    tr = tx.filter(F.col("tipo") == "TRANSFER")
    sal = tr.groupBy(F.col("cuenta_origen").alias("id_cuenta")).agg(
        F.count("*").alias("tx_enviadas"),
        F.sum("monto").alias("monto_enviado"),
        F.countDistinct("cuenta_destino").alias("destinatarios_distintos"),
        F.avg("es_nocturna").alias("pct_nocturno"))
    salidas = tx.filter(F.col("tipo").isin("TRANSFER", "CASH_OUT")).groupBy(
        F.col("cuenta_origen").alias("id_cuenta")).agg(
        F.sum("monto").alias("monto_salida_total"),
        F.max("ts_epoch").alias("ultima_salida"))
    ent = tr.groupBy(F.col("cuenta_destino").alias("id_cuenta")).agg(
        F.count("*").alias("tx_recibidas"),
        F.sum("monto").alias("monto_recibido"),
        F.countDistinct("cuenta_origen").alias("remitentes_distintos"),
        F.min("ts_epoch").alias("primera_entrada"))
    ciudad = tx.groupBy(F.col("cuenta_origen").alias("id_cuenta")).agg(F.first("ciudad").alias("ciudad"))
    etiqueta = tx.groupBy(F.col("cuenta_origen").alias("id_cuenta")).agg(F.max("es_fraude").alias("etiqueta_fraude"))

    cuentas = (sal.join(salidas, "id_cuenta", "full_outer")
               .join(ent, "id_cuenta", "full_outer")
               .join(ciudad, "id_cuenta", "left")
               .join(etiqueta, "id_cuenta", "left")
               .fillna(0, subset=["tx_enviadas", "monto_enviado", "destinatarios_distintos", "pct_nocturno",
                                  "tx_recibidas", "monto_recibido", "remitentes_distintos", "etiqueta_fraude",
                                  "monto_salida_total"])
               .withColumn("monto_enviado", F.round("monto_enviado", 2))
               .withColumn("monto_salida_total", F.round("monto_salida_total", 2))
               .withColumn("monto_recibido", F.round("monto_recibido", 2))
               .withColumn("pct_nocturno", F.round("pct_nocturno", 3))
               .withColumn("ratio_salida_entrada",
                           F.when(F.col("monto_recibido") > 0,
                                  F.round(F.col("monto_salida_total") / F.col("monto_recibido"), 3)))
               # horas entre la primera entrada y la ultima salida (una mula vacia la cuenta rapido)
               .withColumn("horas_entrada_salida",
                           F.round((F.col("ultima_salida") - F.col("primera_entrada")) / 3600, 2)))

    # reglas de riesgo (0-100): sencillas y explicables para la defensa
    cuentas = (cuentas
        .withColumn("regla_mula",
                    ((F.col("remitentes_distintos") >= 10) &
                     (F.col("ratio_salida_entrada").between(0.8, 1.1)) &
                     (F.col("horas_entrada_salida") <= 48)).cast("int"))
        .withColumn("regla_smurfing",
                    ((F.col("destinatarios_distintos") >= 15) & (F.col("tx_recibidas") <= 2)).cast("int"))
        .withColumn("regla_nocturna", (F.col("pct_nocturno") >= 0.6).cast("int"))
        .withColumn("score_riesgo",
                    F.least(F.lit(100),
                            F.col("regla_mula") * 60 + F.col("regla_smurfing") * 40 + F.col("regla_nocturna") * 20))
        .withColumn("regla_mula", F.coalesce("regla_mula", F.lit(0)))
        .withColumn("score_riesgo", F.coalesce("score_riesgo", F.lit(0)))
        .drop("ultima_salida", "primera_entrada"))

    # aristas agregadas (una relacion por par origen -> destino)
    aristas = tr.groupBy("cuenta_origen", "cuenta_destino").agg(
        F.count("*").alias("n_tx"),
        F.round(F.sum("monto"), 2).alias("monto_total"),
        F.min("ts_epoch").alias("primera_ts"),
        F.max("ts_epoch").alias("ultima_ts"),
        F.max("es_fraude").alias("etiqueta_fraude"))

    # ------------------------------------------------------------ 4. ESCRITURA (CURATED + EXPORT)
    curated = os.path.join(a.salida, "curated", "transacciones")
    tx.drop("ts_epoch").write.mode("overwrite").partitionBy("fecha").parquet(curated)
    export = os.path.join(a.salida, "export")
    os.makedirs(export, exist_ok=True)
    exportar_csv(tx.drop("ts_epoch", "fecha").withColumn("fecha_hora", F.date_format("fecha_hora", "yyyy-MM-dd HH:mm:ss")),
                 export, "transacciones_enriquecidas.csv")
    exportar_csv(cuentas, export, "cuentas.csv")
    exportar_csv(aristas, export, "aristas.csv")
    print(f"[EXPORT] cuentas={cuentas.count()} | aristas={aristas.count()} -> {export}")
    spark.stop()


if __name__ == "__main__":
    main()
