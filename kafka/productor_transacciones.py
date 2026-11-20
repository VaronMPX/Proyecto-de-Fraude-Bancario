"""
Kafka - PRODUCTOR: simula el flujo de transacciones del banco en tiempo (casi) real.
Lee dataset_fraude.csv y publica cada fila como JSON en el topic 'transacciones'.

  python kafka/productor_transacciones.py --csv data/dataset_fraude.csv --tps 2000 --limite 600000
La llave del mensaje es cuenta_origen: asi todas las transacciones de una cuenta caen en la
misma particion y conservan su orden (importante para detectar ciclos y mulas).
"""
import argparse, csv, json, time
from kafka import KafkaProducer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="data/dataset_fraude.csv")
    ap.add_argument("--topic", default="transacciones")
    ap.add_argument("--broker", default="localhost:9092")
    ap.add_argument("--tps", type=int, default=2000, help="transacciones por segundo (0 = sin limite)")
    ap.add_argument("--limite", type=int, default=0, help="maximo de filas (0 = todas)")
    a = ap.parse_args()

    p = KafkaProducer(bootstrap_servers=a.broker, linger_ms=20, batch_size=64 * 1024,
                      key_serializer=lambda k: k.encode(),
                      value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
    enviados, t0 = 0, time.time()
    with open(a.csv, encoding="utf-8-sig", newline="") as f:
        for fila in csv.DictReader(f):
            fila["monto"] = float(fila["monto"])
            fila["es_fraude"] = int(fila["es_fraude"])
            p.send(a.topic, key=fila["cuenta_origen"], value=fila)
            enviados += 1
            if a.limite and enviados >= a.limite:
                break
            if a.tps and enviados % a.tps == 0:           # control simple de velocidad
                dormir = enviados / a.tps - (time.time() - t0)
                if dormir > 0:
                    time.sleep(dormir)
                print(f"enviadas {enviados}", flush=True)
    p.flush()
    print(f"[OK] {enviados} transacciones publicadas en '{a.topic}' en {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
