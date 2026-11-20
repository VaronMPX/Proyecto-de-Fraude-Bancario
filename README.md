# Reto 1 - Detección de fraude en transacciones financieras

Arquitectura de datos para detectar **ciclos de dinero, cuentas mula y comunidades coordinadas**
en un flujo de transacciones bancarias. Usa **5 de las 8 tecnologías** del curso:
**Kafka, S3, Spark, Neo4j y BigQuery**.

## 1. Arquitectura

```
 dataset_fraude.csv        ┌────────┐   ┌─────────────────┐   ┌─────────┐   ┌──────────────┐
 (600.000 tx, generador) ─►│ KAFKA  │──►│ S3 (data lake)  │──►│  SPARK  │──►│   NEO4J      │ ciclos, mulas,
   productor                │ topic  │   │ raw/fecha=.../  │   │ limpia  │   │ grafo + GDS  │ caminos, comunidades
                            └────────┘   │ JSONL crudo     │   │ enriquece│  └──────────────┘
                                         └─────────────────┘   │ features │   ┌──────────────┐
                                                               └────┬─────┴──►│  BIGQUERY    │ reporte de riesgo
                                                                    ▼          │ 5 consultas  │ (negocio)
                                                           curated/ (parquet)  └──────────────┘
```

| Paso | Qué pasa | Archivo |
|---|---|---|
| 1. Ingesta | Se simula el flujo de transacciones y se publica en Kafka (llave = cuenta origen) | `kafka/productor_transacciones.py` |
| 2. Data lake | Se guarda el histórico **crudo**, sin transformar, en S3 particionado por día | `kafka/consumidor_a_s3.py` |
| 3. Procesamiento | Spark limpia, enriquece, calcula features por cuenta y reglas de riesgo | `spark/limpieza_enriquecimiento.py` |
| 4. Grafo | Neo4j modela cuentas y transferencias; detecta ciclos, caminos y comunidades | `neo4j/*.cypher` |
| 5. Analítica | BigQuery consolida métricas de riesgo con 5 consultas de negocio | `bigquery/*` |

## 2. Por qué cada tecnología (y no otra)

| Tecnología | Rol | Por qué esta y no otra |
|---|---|---|
| **Kafka** | Ingesta en tiempo real | Es un log distribuido: soporta miles de transacciones por segundo, guarda los mensajes y permite **reprocesarlos**. Con la cuenta origen como llave se conserva el orden por cuenta. Una cola tradicional (RabbitMQ) borra el mensaje al consumirlo, y consultar una base de datos cada segundo no escala. |
| **S3** | Data lake del histórico crudo | Almacena datos tal cual llegan, baratos, inmutables y en formato abierto (JSONL/Parquet). Si cambian las reglas de fraude se puede reprocesar todo el histórico. Una base relacional sería más cara y exigiría fijar un esquema desde el inicio. (Localmente se usa MinIO, compatible con S3.) |
| **Spark** | Limpieza, enriquecimiento y features | Hace joins y agregaciones por cuenta sobre grandes volúmenes y **el mismo código escala** de 600 mil filas a miles de millones. Pandas se queda sin memoria a esa escala. |
| **Neo4j** | Almacenamiento operativo en grafo | Ciclos, caminos cortos y comunidades son consultas sobre **relaciones**. En SQL serían joins recursivos muy costosos; en Cypher se escriben como un patrón `(a)->(b)->(c)->(a)`. Además Graph Data Science trae Louvain y componentes conectados. Cassandra no sirve aquí: no recorre relaciones. |
| **BigQuery** | Almacén analítico | Es columnar y sin servidor: ideal para agregaciones de negocio (tasa de fraude por canal, ciudad, mes) y para conectar herramientas de BI. Neo4j no está hecho para reportes agregados. Snowflake serviría igual; el SQL de `bigquery/` es portable. |

**No se usaron:** Cassandra (no hay un caso de escrituras masivas por clave con baja latencia) y las otras dos tecnologías del curso, porque no aportan al problema.

## 3. Cumplimiento de las reglas de integración

- ✅ Mínimo 4 tecnologías: **5** (Kafka, S3, Spark, Neo4j, BigQuery), cada una con justificación.
- ✅ Componente de procesamiento: **Kafka y Spark**.
- ✅ Almacenamiento operativo: **Neo4j**.
- ✅ Componente analítico / masivo: **S3 y BigQuery**.
- ✅ Consulta basada en grafos: ciclos, mulas, caminos cortos, Louvain y componentes conectados en Neo4j.

## 4. Cómo ejecutarlo

```bash
pip install -r requirements.txt
docker compose up -d                                   # Kafka + MinIO + Neo4j

# 1) Ingesta + data lake
python kafka/productor_transacciones.py --tps 5000     # terminal 1
python kafka/consumidor_a_s3.py                        # terminal 2 (guarda en s3://fraude-datalake/raw/)

# 2) Spark (desde S3/MinIO)
spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 spark/limpieza_enriquecimiento.py \
    --entrada s3a://fraude-datalake/raw/ --salida salida_spark --s3
#    (sin Docker, directo del CSV:  python spark/limpieza_enriquecimiento.py --entrada data/dataset_fraude.csv --salida salida_spark)

# 3) Neo4j: abrir http://localhost:7474 (neo4j / fraude1234) y correr, bloque por bloque:
#    neo4j/01_cargar_grafo.cypher  y  neo4j/02_deteccion_fraude.cypher

# 4) BigQuery
bash bigquery/cargar_bigquery.sh   # y luego correr bigquery/consultas_negocio.sql

# 5) Evidencia para el informe
python validacion/validar_deteccion.py --louvain
```

La carpeta `salida_spark/export/` ya incluye los CSV que produce Spark (cuentas, aristas y transacciones enriquecidas), por si quieres saltarte los pasos 1 y 2.

## 5. Datos (generador propio documentado)

`data/generar_dataset.py` genera 600.000 transacciones y 40.000 cuentas (semilla fija = reproducible).
Inyecta ~4 % de fraude en 4 patrones: **CICLO**, **MULA**, **COMUNIDAD** y **SMURFING**.
Columnas: `id_tx, fecha_hora, tipo, monto, canal, cuenta_origen, cuenta_destino, ciudad, es_fraude, patron_fraude`.

## 6. Qué hace Spark

- **Limpieza:** recorta texto, convierte tipos, elimina nulos, montos <= 0, auto-transferencias y duplicados por `id_tx` (en este dataset sintético no descartó filas: 600.000 de 600.000).
- **Enriquecimiento:** hora, día de la semana, bandera nocturna (23:00-04:59).
- **Features por cuenta:** remitentes y destinatarios distintos, monto recibido y enviado, ratio salida/entrada, horas entre la primera entrada y la última salida, % nocturno.
- **Reglas de riesgo (score 0-100):** mula = 60 pts (>= 10 remitentes, salida entre 80 % y 110 % de lo recibido, en <= 48 h); smurfing = 40 pts (>= 15 destinatarios con <= 2 recibidas); nocturna = 20 pts.
- **Salidas:** parquet particionado por fecha (zona *curated*) y CSV para Neo4j y BigQuery.

## 7. Resultados medidos (`validacion/validar_deteccion.py`)

| Patrón | Método | Resultado |
|---|---|---|
| Mulas | Regla en Spark | 303 detectadas, **100 % correctas**, cobertura 94,7 % (303 de 320) |
| Smurfing | Regla en Spark | 150 detectadas, **100 %** correctas y **100 %** de cobertura |
| Ciclos (3-5 cuentas) | Grafo: aristas repetidas, ventana de 6 h | 647 ciclos, **642 con todas sus cuentas fraudulentas (99,2 %)**, que son todos los sembrados |
| Grupos aislados | Componentes conectados | 652 grupos de 3 a 14 cuentas, **100 % fraude** (642 ciclos + 10 comunidades) |
| Comunidades | Louvain | 49 comunidades pequeñas con >= 80 % de fraude, que cubren **606 de 928** cuentas de comunidades fraudulentas (65 %) |

Las comunidades fraudulentas no quedan aisladas del todo: algunas de sus cuentas también fueron víctimas de una mula, así que se conectan con la red principal. Por eso los componentes conectados solo encuentran las más aisladas y Louvain cubre más.

## 8. Reporte de negocio (BigQuery)

1. Canal que concentra más fraude (tasa y monto en riesgo).
2. Ciudades con más dinero fraudulento y su participación.
3. Comportamiento por tipo de fraude (volumen, ticket promedio, % nocturno).
4. Evolución mensual de la tasa de fraude y variación frente al mes anterior.
5. Top 10 cuentas de mayor riesgo para revisión manual.

Hallazgos con este dataset: WEB (8,46 %) y APP_MOVIL (6,40 %) tienen la mayor tasa de fraude y SUCURSAL 0 %; los ciclos mueven el mayor monto (129 mil millones de COP) y son nocturnos en 76 % de los casos; las comunidades son nocturnas en 86 %.

## 9. Limitaciones (para la defensa)

- Los datos son **sintéticos**: los patrones salen más limpios que en un banco real y por eso la precisión es tan alta.
- El flujo "tiempo real" es simulado: el productor lee un CSV a una velocidad configurable.
- Las reglas de Spark usan umbrales fijos; en producción se calibrarían o se reemplazarían por un modelo de ML.
- Los ciclos de 4 y 5 cuentas con `*4..5` pueden ser costosos en grafos grandes; por eso se restringen a aristas repetidas y a una ventana de 6 horas.
