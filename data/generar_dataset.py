#!/usr/bin/env python3
"""
Generador de dataset sintético de transacciones financieras (Reto 1 - Detección de fraude).

Produce un unico archivo simple: dataset_fraude.csv (10 columnas, una fila por transaccion)

Patrones de fraude inyectados (columna `patron_fraude`):
  CICLO       cuentas que se pasan dinero en círculo (A->B->C->A), montos casi iguales
  MULA        muchas cuentas víctimas -> cuenta mula -> retiro/transferencia rápida a cuenta final
  COMUNIDAD   grupo cerrado de cuentas que se transfieren entre sí de forma coordinada (ráfagas)
  SMURFING    una cuenta fragmenta un monto grande en muchas transferencias pequeñas (< umbral)

Uso:  python generar_dataset.py [--n 60000] [--seed 42] [--salida .]
Solo usa la librería estándar de Python.
"""
import argparse
import csv
import os
import zlib
import random
from datetime import datetime, timedelta

CIUDADES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga",
            "Ibagué", "Pereira", "Manizales", "Cúcuta", "Villavicencio", "Pasto"]
SEGMENTOS = [("Joven", 0.25), ("Asalariado", 0.40), ("Independiente", 0.20),
             ("Pyme", 0.10), ("Premium", 0.05)]
CANALES = ["APP_MOVIL", "WEB", "CAJERO", "SUCURSAL", "API_ALIADO"]
UMBRAL_REPORTE = 10_000_000  # COP, umbral hipotético de reporte (para smurfing)


def elegir_ponderado(rng, pares):
    r, acc = rng.random(), 0.0
    for v, p in pares:
        acc += p
        if r <= acc:
            return v
    return pares[-1][0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=60000, help="número aproximado de transacciones")
    ap.add_argument("--cuentas", type=int, default=6000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--salida", default=".")
    a = ap.parse_args()
    rng = random.Random(a.seed)

    inicio = datetime(2026, 1, 1)
    dias = 90
    fin = inicio + timedelta(days=dias)

    # ------------------------------------------------------------------ CUENTAS
    cuentas = []
    for i in range(a.cuentas):
        ciudad = rng.choice(CIUDADES)
        seg = elegir_ponderado(rng, SEGMENTOS)
        base = {"Joven": 8e5, "Asalariado": 3e6, "Independiente": 4e6,
                "Pyme": 2e7, "Premium": 6e7}[seg]
        cuentas.append({
            "id_cuenta": f"ACC{i+1:06d}",
            "ciudad": ciudad,
            "segmento": seg,
            "fecha_apertura": (inicio - timedelta(days=rng.randint(30, 3000))).date().isoformat(),
            "saldo_base": round(rng.lognormvariate(0, 0.6) * base, 2),
            "es_fraudulenta": 0,
            "rol_fraude": "",
        })
    por_id = {c["id_cuenta"]: c for c in cuentas}
    ids = list(por_id)

    # Comunidades "legítimas" (por ciudad) para que Louvain tenga estructura real
    por_ciudad = {}
    for c in cuentas:
        por_ciudad.setdefault(c["ciudad"], []).append(c["id_cuenta"])

    # Cuentas reservadas para fraude (se sacan del pool normal con cierta probabilidad)
    reservadas = set()

    def tomar(k):
        libres = [x for x in ids if x not in reservadas]
        sel = rng.sample(libres, k)
        reservadas.update(sel)
        return sel

    txs = []

    def add(ts, origen, destino, monto, tipo, canal=None, fraude=0, patron="", grupo=""):
        txs.append({
            "timestamp": ts, "tipo": tipo, "monto": round(monto, 2),
            "cuenta_origen": origen, "cuenta_destino": destino,
            "canal": canal or rng.choice(CANALES),
            "ciudad_tx": por_id[origen]["ciudad"],
            "es_fraude": fraude, "patron_fraude": patron, "id_grupo_fraude": grupo,
        })

    def ts_aleatorio(sesgo_noche=False):
        d = rng.random() * dias
        t = inicio + timedelta(days=d)
        if sesgo_noche:
            t = t.replace(hour=rng.choice([0, 1, 2, 3, 4, 23]), minute=rng.randint(0, 59),
                          second=rng.randint(0, 59))
        return t

    # ------------------------------------------------------------------ FRAUDE
    n_fraude_obj = int(a.n * 0.04)  # ~4 % de transacciones fraudulentas

    gid = 0
    # --- CICLOS
    while sum(1 for t in txs if t["es_fraude"]) < n_fraude_obj * 0.30:
        gid += 1
        k = rng.choice([3, 3, 4, 5])
        miembros = tomar(k)
        monto = rng.uniform(2e6, 4e7)
        t0 = ts_aleatorio(sesgo_noche=True)
        vueltas = rng.randint(2, 4)
        for v in range(vueltas):
            for j in range(k):
                o, d = miembros[j], miembros[(j + 1) % k]
                monto *= rng.uniform(0.97, 0.995)  # comisión entre saltos
                t0 += timedelta(minutes=rng.randint(1, 25))
                add(t0, o, d, monto, "TRANSFER", rng.choice(["APP_MOVIL", "WEB"]),
                    1, "CICLO", f"G{gid:03d}")
        for m in miembros:
            por_id[m]["es_fraudulenta"] = 1
            por_id[m]["rol_fraude"] = "CICLO"

    # --- MULAS
    cuenta_mulas = 0
    while sum(1 for t in txs if t["patron_fraude"] == "MULA") < n_fraude_obj * 0.30:
        gid += 1
        mula, final = tomar(2)
        victimas = rng.sample([x for x in ids if x not in reservadas], rng.randint(12, 30))
        t0 = ts_aleatorio(sesgo_noche=True)
        total = 0.0
        for v in victimas:
            m = rng.uniform(3e5, 5e6)
            total += m
            t0 += timedelta(minutes=rng.randint(1, 40))
            add(t0, v, mula, m, "TRANSFER", rng.choice(["APP_MOVIL", "WEB", "API_ALIADO"]),
                1, "MULA", f"G{gid:03d}")
        # la mula vacía la cuenta en minutos
        resto = total * rng.uniform(0.9, 0.97)
        t0 += timedelta(minutes=rng.randint(2, 30))
        add(t0, mula, final, resto * 0.7, "TRANSFER", "APP_MOVIL", 1, "MULA", f"G{gid:03d}")
        t0 += timedelta(minutes=rng.randint(1, 10))
        add(t0, mula, final, resto * 0.3, "CASH_OUT", "CAJERO", 1, "MULA", f"G{gid:03d}")
        por_id[mula]["es_fraudulenta"] = 1
        por_id[mula]["rol_fraude"] = "MULA"
        por_id[final]["es_fraudulenta"] = 1
        por_id[final]["rol_fraude"] = "DESTINO_FINAL"
        cuenta_mulas += 1

    # --- COMUNIDADES COORDINADAS
    while sum(1 for t in txs if t["patron_fraude"] == "COMUNIDAD") < n_fraude_obj * 0.25:
        gid += 1
        k = rng.randint(8, 14)
        miembros = tomar(k)
        # ráfagas: la actividad se concentra en 2-3 ventanas cortas
        ventanas = [ts_aleatorio(sesgo_noche=True) for _ in range(rng.randint(2, 3))]
        for _ in range(rng.randint(50, 90)):
            o, d = rng.sample(miembros, 2)
            base = rng.choice(ventanas)
            add(base + timedelta(minutes=rng.randint(0, 90)), o, d,
                rng.uniform(5e5, 8e6), "TRANSFER", rng.choice(["WEB", "API_ALIADO"]),
                1, "COMUNIDAD", f"G{gid:03d}")
        for m in miembros:
            por_id[m]["es_fraudulenta"] = 1
            por_id[m]["rol_fraude"] = "COMUNIDAD"

    # --- SMURFING (fan-out de montos pequeños)
    while sum(1 for t in txs if t["patron_fraude"] == "SMURFING") < n_fraude_obj * 0.15:
        gid += 1
        origen = tomar(1)[0]
        receptores = rng.sample([x for x in ids if x not in reservadas], rng.randint(15, 35))
        t0 = ts_aleatorio()
        for r in receptores:
            t0 += timedelta(minutes=rng.randint(1, 60))
            add(t0, origen, r, rng.uniform(0.80, 0.97) * UMBRAL_REPORTE / 4, "TRANSFER",
                rng.choice(["APP_MOVIL", "WEB"]), 1, "SMURFING", f"G{gid:03d}")
        por_id[origen]["es_fraudulenta"] = 1
        por_id[origen]["rol_fraude"] = "SMURFING"

    # ------------------------------------------------------------------ TRÁFICO NORMAL
    n_normal = a.n - len(txs)
    pool = [x for x in ids if x not in reservadas]
    pool_set = set(pool)
    ciudad_pool = {c: [x for x in v if x in pool_set] for c, v in por_ciudad.items()}
    tipos = [("TRANSFER", 0.40), ("PAYMENT", 0.30), ("CASH_OUT", 0.12),
             ("CASH_IN", 0.10), ("DEBIT", 0.08)]
    # contactos frecuentes por cuenta (da estructura de comunidad realista)
    contactos = {}
    for x in pool:
        c = por_id[x]["ciudad"]
        contactos[x] = rng.sample(ciudad_pool[c], min(len(ciudad_pool[c]), rng.randint(3, 8)))

    for _ in range(n_normal):
        o = rng.choice(pool)
        tipo = elegir_ponderado(rng, tipos)
        r = rng.random()
        if r < 0.80:
            d = rng.choice(contactos[o])
        elif r < 0.95:
            d = rng.choice(ciudad_pool[por_id[o]["ciudad"]])
        else:
            d = rng.choice(pool)
        if d == o:
            d = rng.choice(pool)
        base = {"Joven": 6e4, "Asalariado": 2.5e5, "Independiente": 3.5e5,
                "Pyme": 2.5e6, "Premium": 4e6}[por_id[o]["segmento"]]
        monto = min(rng.lognormvariate(0, 0.9) * base, 9e6)
        # actividad diurna dominante
        t = ts_aleatorio()
        t = t.replace(hour=int(min(23, max(0, rng.gauss(14, 4.5)))),
                      minute=rng.randint(0, 59), second=rng.randint(0, 59))
        add(t, o, d, monto, tipo, None, 0, "", "")

    # ------------------------------------------------------------------ ORDEN + SALDOS + ENRIQUECIMIENTO
    txs.sort(key=lambda x: x["timestamp"])
    saldo = {c["id_cuenta"]: c["saldo_base"] for c in cuentas}
    dispositivos = {c["id_cuenta"]: f"DEV{rng.randint(10000, 99999)}" for c in cuentas}
    # las comunidades/ciclos comparten dispositivo (señal de coordinación)
    grupo_dev = {}

    filas = []
    for i, t in enumerate(txs, 1):
        o, d = t["cuenta_origen"], t["cuenta_destino"]
        saldo_o_ant, saldo_d_ant = saldo[o], saldo[d]
        m = t["monto"]
        if t["tipo"] == "CASH_IN":
            saldo[o] += m
        else:
            if saldo[o] < m:                   # los fraudes pueden dejar la cuenta en 0
                saldo[o] = max(saldo[o], 0)
                m_efectivo = min(m, saldo[o])
            saldo[o] = max(0.0, saldo[o] - m)
            if t["tipo"] in ("TRANSFER", "PAYMENT"):
                saldo[d] += m
        dev = dispositivos[o]
        if t["id_grupo_fraude"]:
            dev = grupo_dev.setdefault(t["id_grupo_fraude"], f"DEV{rng.randint(10000, 99999)}")
            if t["patron_fraude"] == "COMUNIDAD" and rng.random() < 0.2:
                dev = dispositivos[o]
        ip_base = rng.randint(1, 254)
        ip = f"181.{ip_base}.{rng.randint(0,255)}.{rng.randint(1,254)}"
        if t["id_grupo_fraude"] and rng.random() < 0.7:
            ip = f"45.{zlib.crc32(t["id_grupo_fraude"].encode()) % 250}.{rng.randint(0,255)}.{rng.randint(1,254)}"
        filas.append({
            "id_tx": f"TX{i:08d}",
            "step": int((t["timestamp"] - inicio).total_seconds() // 3600),
            "timestamp": t["timestamp"].strftime("%Y-%m-%dT%H:%M:%S"),
            "tipo": t["tipo"],
            "monto": f"{t['monto']:.2f}",
            "moneda": "COP",
            "cuenta_origen": o,
            "cuenta_destino": d,
            "saldo_origen_antes": f"{saldo_o_ant:.2f}",
            "saldo_origen_despues": f"{saldo[o]:.2f}",
            "saldo_destino_antes": f"{saldo_d_ant:.2f}",
            "saldo_destino_despues": f"{saldo[d]:.2f}",
            "canal": t["canal"],
            "ciudad_tx": t["ciudad_tx"],
            "id_dispositivo": dev,
            "ip": ip,
            "es_fraude": t["es_fraude"],
            "patron_fraude": t["patron_fraude"],
            "id_grupo_fraude": t["id_grupo_fraude"],
        })

    # Dataset simple: solo las columnas necesarias para el grafo y el analisis
    simple = []
    for fila in filas:
        simple.append({
            "id_tx": fila["id_tx"],
            "fecha_hora": fila["timestamp"],
            "tipo": fila["tipo"],
            "monto": fila["monto"],
            "canal": fila["canal"],
            "cuenta_origen": fila["cuenta_origen"],
            "cuenta_destino": fila["cuenta_destino"],
            "ciudad": por_id[fila["cuenta_origen"]]["ciudad"],
            "es_fraude": fila["es_fraude"],
            "patron_fraude": fila["patron_fraude"] or "NORMAL",
        })
    filas = simple

    os.makedirs(a.salida, exist_ok=True)
    with open(os.path.join(a.salida, "dataset_fraude.csv"), "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0]))
        w.writeheader()
        w.writerows(filas)

    nf = sum(int(x["es_fraude"]) for x in filas)
    print(f"filas: {len(filas)} | fraude: {nf} ({nf/len(filas):.2%}) | cuentas: {len(cuentas)}")


if __name__ == "__main__":
    main()
