"""
Mide que tan bien detectan las reglas y el grafo los fraudes sembrados en el dataset.
Sirve como EVIDENCIA para el informe (precision / cobertura). Usa lo mismo que las consultas Cypher.

  python validacion/validar_deteccion.py            (rapido, ~1 min)
  python validacion/validar_deteccion.py --louvain  (agrega Louvain, ~2 min)
Requiere: pip install pandas networkx
"""
import argparse
import pandas as pd
import networkx as nx

ap = argparse.ArgumentParser()
ap.add_argument("--export", default="salida_spark/export")
ap.add_argument("--louvain", action="store_true")
a = ap.parse_args()

cu = pd.read_csv(f"{a.export}/cuentas.csv")
ar = pd.read_csv(f"{a.export}/aristas.csv")
tx = pd.read_csv(f"{a.export}/transacciones_enriquecidas.csv",
                 usecols=["cuenta_origen", "cuenta_destino", "tipo", "es_fraude", "patron_fraude"])

print("== MULAS (regla Spark) ==")
reales = set(tx[(tx.patron_fraude == "MULA") & (tx.tipo == "CASH_OUT")].cuenta_origen)
det = set(cu[cu.regla_mula == 1].id_cuenta)
print(f"detectadas={len(det)} | reales={len(reales)} | aciertos={len(det & reales)} "
      f"| precision={len(det & reales)/max(len(det),1):.1%} | cobertura={len(det & reales)/len(reales):.1%}")

print("== SMURFING (regla Spark) ==")
reales = set(tx[tx.patron_fraude == "SMURFING"].cuenta_origen)
det = set(cu[cu.regla_smurfing == 1].id_cuenta)
print(f"detectadas={len(det)} | reales={len(reales)} | aciertos={len(det & reales)} "
      f"| precision={len(det & reales)/max(len(det),1):.1%} | cobertura={len(det & reales)/len(reales):.1%}")

print("== CICLOS (grafo: aristas repetidas, ventana de 6 h, ciclos de 3 a 5) ==")
sub = ar[(ar.n_tx >= 2) & ((ar.ultima_ts - ar.primera_ts) <= 6 * 3600)]
G = nx.DiGraph()
for r in sub.itertuples():
    G.add_edge(r.cuenta_origen, r.cuenta_destino, m=r.monto_total)
def monto_parecido(c):
    ms = [G[c[i]][c[(i + 1) % len(c)]]["m"] for i in range(len(c))]
    return min(ms) / max(ms) >= 0.6
ciclos = [c for c in nx.simple_cycles(G, length_bound=5) if len(c) >= 3 and monto_parecido(c)]
fr = set(tx[tx.patron_fraude == "CICLO"].cuenta_origen)
puros = sum(all(x in fr for x in c) for c in ciclos)
print(f"ciclos encontrados={len(ciclos)} | 100% cuentas fraudulentas={puros} ({puros/max(len(ciclos),1):.1%})")

print("== GRUPOS AISLADOS (componentes conectados) ==")
H = nx.from_pandas_edgelist(ar, "cuenta_origen", "cuenta_destino")
comps = list(nx.connected_components(H))
cic = set(tx[tx.patron_fraude == "CICLO"].cuenta_origen)
com = set(tx[tx.patron_fraude == "COMUNIDAD"].cuenta_origen)
peq = [c for c in comps if 3 <= len(c) <= 14]
print(f"componentes={len(comps)} | el mayor tiene {max(len(c) for c in comps)} cuentas | "
      f"grupos de 3-14 cuentas={len(peq)} | 100% fraude={sum(c <= (cic | com) for c in peq)}")

if a.louvain:
    print("== COMUNIDADES (Louvain) ==")
    W = nx.Graph()
    for r in ar.itertuples():
        W.add_edge(r.cuenta_origen, r.cuenta_destino, weight=r.n_tx)
    coms = nx.community.louvain_communities(W, weight="weight", seed=7)
    cf = set(tx[tx.patron_fraude == "COMUNIDAD"].cuenta_origen) | set(tx[tx.patron_fraude == "COMUNIDAD"].cuenta_destino)
    bien = [c for c in coms if 8 <= len(c) <= 40 and len(c & cf) / len(c) >= 0.8]
    cub = len(set().union(*bien) & cf) if bien else 0
    print(f"comunidades={len(coms)} | comunidades pequenas casi 100% fraude={len(bien)} "
          f"| cuentas de comunidades fraudulentas cubiertas={cub} de {len(cf)} ({cub/len(cf):.0%})")
