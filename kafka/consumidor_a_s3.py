"""
Kafka -> S3 (MinIO): guarda el historico CRUDO en el data lake, tal cual llega, sin transformar.
Layout:  s3://fraude-datalake/raw/fecha=AAAA-MM-DD/lote-<n>.jsonl   (particionado por dia)

  python kafka/consumidor_a_s3.py --lote 20000
Termina solo cuando no llegan mensajes nuevos durante --espera segundos.
"""
import argparse, json, time
from collections import defaultdict
import boto3
from botocore.exceptions import ClientError
from kafka import KafkaConsumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="transacciones")
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--endpoint", default="http://localhost:9000")
    ap.add_argument("--bucket", default="fraude-datalake")
    ap.add_argument("--lote", type=int, default=20000)
    ap.add_argument("--espera", type=int, default=20)
    a = ap.parse_args()

    s3 = boto3.client("s3", endpoint_url=a.endpoint, aws_access_key_id="rustfsadmin",
                    aws_secret_access_key="rustfsadmin", region_name="us-east-1")
    try:
        s3.head_bucket(Bucket=a.bucket)
    except ClientError:
        s3.create_bucket(Bucket=a.bucket)

    c = KafkaConsumer(a.topic, bootstrap_servers=a.broker, group_id="escritor-raw",
                      auto_offset_reset="earliest", enable_auto_commit=False,
                      value_deserializer=lambda v: json.loads(v.decode()),
                      consumer_timeout_ms=a.espera * 1000)
    buf, n_lote, total = defaultdict(list), 0, 0

    def volcar():
        nonlocal n_lote
        for fecha, filas in buf.items():
            n_lote += 1
            cuerpo = "\n".join(json.dumps(x, ensure_ascii=False) for x in filas)
            s3.put_object(Bucket=a.bucket, Key=f"raw/fecha={fecha}/lote-{n_lote:05d}.jsonl", Body=cuerpo.encode())
        buf.clear()
        c.commit()

    pendientes = 0
    for msg in c:
        tx = msg.value
        buf[tx["fecha_hora"][:10]].append(tx)
        pendientes += 1
        total += 1
        if pendientes >= a.lote:
            volcar(); pendientes = 0
            print(f"guardadas {total} en s3://{a.bucket}/raw/", flush=True)
    volcar()
    print(f"[OK] {total} transacciones crudas en s3://{a.bucket}/raw/")


if __name__ == "__main__":
    main()
