// =====================================================================
// 01 - CARGA DEL GRAFO EN NEO4J  (Neo4j Browser: http://localhost:7474  usuario neo4j / fraude1234)
// Modelo:  (:Cuenta {id, ciudad, score, ...}) -[:ENVIA {n_tx, monto_total, primera, ultima}]-> (:Cuenta)
// Una relacion ENVIA resume TODAS las transferencias de un origen a un destino (180 mil aristas
// en vez de 600 mil filas): el grafo es mas liviano y las consultas de ciclos mas rapidas.
// Ejecutar cada bloque por separado. Los CSV los dejo Spark en ./salida_spark/export (carpeta import).
// =====================================================================

CREATE CONSTRAINT cuenta_id IF NOT EXISTS FOR (c:Cuenta) REQUIRE c.id IS UNIQUE;

// Nodos (usar ":auto" al inicio en Neo4j Browser para permitir IN TRANSACTIONS)
:auto
LOAD CSV WITH HEADERS FROM 'file:///cuentas.csv' AS r
CALL {
  WITH r
  MERGE (c:Cuenta {id: r.id_cuenta})
  SET c.ciudad         = r.ciudad,
      c.score          = toInteger(r.score_riesgo),
      c.regla_mula     = toInteger(r.regla_mula),
      c.regla_smurfing = toInteger(r.regla_smurfing),
      c.remitentes     = toInteger(r.remitentes_distintos),
      c.destinatarios  = toInteger(r.destinatarios_distintos),
      c.etiqueta       = toInteger(r.etiqueta_fraude)      // solo para VALIDAR, no para detectar
} IN TRANSACTIONS OF 5000 ROWS;

// Relaciones
:auto
LOAD CSV WITH HEADERS FROM 'file:///aristas.csv' AS r
CALL {
  WITH r
  MATCH (a:Cuenta {id: r.cuenta_origen}), (b:Cuenta {id: r.cuenta_destino})
  CREATE (a)-[:ENVIA {n_tx: toInteger(r.n_tx), monto_total: toFloat(r.monto_total),
                      primera: toInteger(r.primera_ts), ultima: toInteger(r.ultima_ts),
                      etiqueta: toInteger(r.etiqueta_fraude)}]->(b)
} IN TRANSACTIONS OF 5000 ROWS;

// Verificacion (esperado: ~40.000 cuentas y ~180.000 relaciones)
MATCH (c:Cuenta) RETURN count(c) AS cuentas;
MATCH ()-[e:ENVIA]->() RETURN count(e) AS relaciones;
