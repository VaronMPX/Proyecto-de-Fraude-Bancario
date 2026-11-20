// =====================================================================================
// TODAS LAS CONSULTAS PARA VER LOS GRAFOS EN NEO4J (Neo4j 5.x)
// COMO USARLO: copia UN bloque a la vez (hasta el ';'), pegalo en la barra negra de arriba
// y ejecuta con Ctrl+Enter. Los bloques que empiezan con :auto se pegan completos.
// Para ver el dibujo: en el resultado, pulsa el boton "Graph" de la izquierda (no "Table").
// =====================================================================================


// =====================================================================================
// PARTE 0 - VERIFICAR QUE TODO ESTE LISTO
// =====================================================================================

// 0.1 Plugin de grafos (debe devolver una version, ej. "2.x.x")
RETURN gds.version();

// 0.2 Archivos CSV visibles para Neo4j (debe devolver 1 fila; si da error, mira la nota 1 del chat)
LOAD CSV WITH HEADERS FROM 'file:///cuentas.csv' AS r RETURN r.id_cuenta, r.score_riesgo LIMIT 1;

// 0.3 Cuantos datos hay ahora (si ya cargaste, veras ~39.999 y ~179.667)
MATCH (c:Cuenta) RETURN count(c) AS cuentas;
MATCH ()-[e:ENVIA]->() RETURN count(e) AS relaciones;


// =====================================================================================
// PARTE 1 - CARGAR EL GRAFO (solo si 0.3 dio 0; si ya hay datos, salta a la PARTE 2)
// =====================================================================================

// 1.0 (OPCIONAL) Borrar todo para empezar limpio. Usar solo si cargaste a medias o duplicado.
MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS;

// 1.1 Restriccion de unicidad
CREATE CONSTRAINT cuenta_id IF NOT EXISTS FOR (c:Cuenta) REQUIRE c.id IS UNIQUE;

// 1.2 Nodos (cuentas)
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
      c.etiqueta       = toInteger(r.etiqueta_fraude)
} IN TRANSACTIONS OF 5000 ROWS;

// 1.3 Relaciones (transferencias agregadas por par origen -> destino). Tarda 1-2 minutos.
:auto
LOAD CSV WITH HEADERS FROM 'file:///aristas.csv' AS r
CALL {
  WITH r
  MATCH (a:Cuenta {id: r.cuenta_origen}), (b:Cuenta {id: r.cuenta_destino})
  CREATE (a)-[:ENVIA {n_tx: toInteger(r.n_tx), monto_total: toFloat(r.monto_total),
                      primera: toInteger(r.primera_ts), ultima: toInteger(r.ultima_ts),
                      etiqueta: toInteger(r.etiqueta_fraude)}]->(b)
} IN TRANSACTIONS OF 5000 ROWS;

// 1.4 Verificacion (esperado: ~39.999 y ~179.667)
MATCH (c:Cuenta) RETURN count(c) AS cuentas;
MATCH ()-[e:ENVIA]->() RETURN count(e) AS relaciones;


// =====================================================================================
// PARTE 2 - PREPARAR LOS DATOS PARA VISUALIZAR (se ejecuta una sola vez)
// =====================================================================================

// 2.1 Marcar las aristas "rapidas": se repiten (n_tx >= 2) y ocurren en una ventana de 6 horas.
//     Asi los ciclos se buscan solo entre ~3.000 aristas en vez de 180.000 (mucho mas rapido).
MATCH ()-[r:ENVIA]->() WHERE r.n_tx >= 2 AND r.ultima - r.primera <= 21600 SET r.rapida = true;

// 2.2 Proyectar el grafo para los algoritmos (si da error de que ya existe, ejecuta antes:
//     CALL gds.graph.drop('fraude', false); )
CALL gds.graph.project('fraude', 'Cuenta', {ENVIA: {orientation: 'UNDIRECTED', properties: 'n_tx'}});

// 2.3 Louvain: comunidades (debe mostrar ~700)
CALL gds.louvain.write('fraude', {relationshipWeightProperty: 'n_tx', writeProperty: 'comunidad'})
YIELD communityCount, modularity;

// 2.4 Componentes conectados: grupos aislados (debe mostrar ~650)
CALL gds.wcc.write('fraude', {writeProperty: 'componente'}) YIELD componentCount;

// 2.5 Etiquetas para que el navegador las pinte de colores distintos
MATCH (c:Cuenta {regla_mula: 1}) SET c:Mula;
MATCH (c:Cuenta {regla_smurfing: 1}) SET c:Smurfing;
MATCH (c:Cuenta) WHERE c.etiqueta = 1 SET c:Fraude;   // etiqueta real del dataset: solo para validar/colorear

// 2.6 Marcar con la etiqueta :Ciclo las cuentas de los ciclos detectados (3, 4 y 5 cuentas)
MATCH (a:Cuenta)-[r1:ENVIA {rapida: true}]->(b:Cuenta)-[r2:ENVIA {rapida: true}]->(c:Cuenta)-[r3:ENVIA {rapida: true}]->(a)
WHERE a.id < b.id AND a.id < c.id
  AND abs(r1.primera - r2.primera) <= 21600 AND abs(r2.primera - r3.primera) <= 21600
  AND r2.monto_total / r1.monto_total >= 0.8 AND r2.monto_total / r1.monto_total <= 1.25
  AND r3.monto_total / r2.monto_total >= 0.8 AND r3.monto_total / r2.monto_total <= 1.25
SET a:Ciclo, b:Ciclo, c:Ciclo;

MATCH p = (a:Cuenta)-[:ENVIA*4..5 {rapida: true}]->(a)
WHERE all(n IN nodes(p)[1..-1] WHERE a.id < n.id)
UNWIND nodes(p) AS n
SET n:Ciclo;


// =====================================================================================
// PARTE 3 - LOS GRAFOS (en cada resultado pulsa "Graph")
// Consejo: abajo del dibujo hay chips con los nombres de etiquetas (Cuenta, Mula, Fraude...).
// Haz clic en "Cuenta" y elige "Caption: id" para ver el codigo de cada cuenta en los circulos.
// =====================================================================================

// 3.1 CICLOS de 3 cuentas (el dinero da vueltas A -> B -> C -> A)
MATCH p = (a:Cuenta)-[r1:ENVIA {rapida: true}]->(b:Cuenta)-[r2:ENVIA {rapida: true}]->(c:Cuenta)-[r3:ENVIA {rapida: true}]->(a)
WHERE a.id < b.id AND a.id < c.id
  AND abs(r1.primera - r2.primera) <= 21600 AND abs(r2.primera - r3.primera) <= 21600
  AND r2.monto_total / r1.monto_total >= 0.8 AND r2.monto_total / r1.monto_total <= 1.25
  AND r3.monto_total / r2.monto_total >= 0.8 AND r3.monto_total / r2.monto_total <= 1.25
RETURN p LIMIT 12;

// 3.2 CICLOS de 4 y 5 cuentas
MATCH p = (a:Cuenta)-[:ENVIA*4..5 {rapida: true}]->(a)
WHERE all(n IN nodes(p)[1..-1] WHERE a.id < n.id)
RETURN p LIMIT 10;

// 3.3 GRUPOS AISLADOS pequenos (3 a 5 cuentas): los anillos de ciclos separados del resto de la red
MATCH (c:Cuenta)
WITH c.componente AS componente, count(*) AS cuentas
WHERE cuentas >= 3 AND cuentas <= 5
WITH componente LIMIT 8
MATCH p = (x:Cuenta {componente: componente})-[:ENVIA]->(y:Cuenta {componente: componente})
RETURN p;

// 3.4 UNA CUENTA MULA: muchas victimas -> mula -> destino final (la mula con mas remitentes)
MATCH (m:Cuenta {regla_mula: 1})
WITH m ORDER BY m.remitentes DESC LIMIT 1
MATCH p = (v:Cuenta)-[:ENVIA]->(m)-[:ENVIA]->(f:Cuenta)
RETURN p;

// 3.5 CINCO MULAS A LA VEZ con sus victimas y destinos
MATCH (m:Cuenta {regla_mula: 1})
WITH m ORDER BY m.remitentes DESC LIMIT 5
MATCH p = (v:Cuenta)-[:ENVIA]->(m)-[:ENVIA]->(f:Cuenta)
RETURN p;

// 3.6 SMURFING: una cuenta que reparte en muchas transferencias pequenas (3 cuentas)
MATCH (s:Cuenta {regla_smurfing: 1})
WITH s LIMIT 3
MATCH p = (s)-[:ENVIA]->(x:Cuenta)
RETURN p;

// 3.7 CAMINO MAS CORTO sospechoso: de una victima a un destino final, pasando por una mula
MATCH (m:Cuenta {regla_mula: 1}) WITH m LIMIT 1
MATCH (m)-[:ENVIA]->(f:Cuenta) WITH m, f LIMIT 1
MATCH (v:Cuenta)-[:ENVIA]->(m) WITH v, f LIMIT 1
MATCH p = shortestPath((v)-[:ENVIA*..6]->(f))
RETURN p;

// 3.8 COMUNIDAD coordinada detectada con LOUVAIN (la mas grande con 80 % o mas de cuentas fraudulentas)
MATCH (c:Cuenta)
WITH c.comunidad AS comunidad, count(*) AS cuentas, avg(c.etiqueta) AS pct_fraude
WHERE cuentas >= 8 AND cuentas <= 40 AND pct_fraude >= 0.8
WITH comunidad, cuentas ORDER BY cuentas DESC LIMIT 1
MATCH p = (x:Cuenta {comunidad: comunidad})-[:ENVIA]->(y:Cuenta {comunidad: comunidad})
RETURN p;

// 3.9 GRUPOS COORDINADOS AISLADOS por componentes conectados (8 a 14 cuentas)
MATCH (c:Cuenta)
WITH c.componente AS componente, count(*) AS cuentas
WHERE cuentas >= 8 AND cuentas <= 14
WITH componente LIMIT 3
MATCH p = (x:Cuenta {componente: componente})-[:ENVIA]->(y:Cuenta {componente: componente})
RETURN p;

// 3.10 CUENTAS DE ALTO RIESGO (score >= 60) con sus vecinos
MATCH p = (a:Cuenta)-[:ENVIA]-(b:Cuenta)
WHERE a.score >= 60
RETURN p LIMIT 150;

// 3.11 VECINDAD DE UNA CUENTA CUALQUIERA (cambia el id por uno que veas en otros resultados)
MATCH p = (a:Cuenta {id: 'ACC000006'})-[:ENVIA]-(b:Cuenta)
RETURN p LIMIT 60;

// 3.12 COMPARACION: un trozo de red NORMAL (para contrastar con los fraudes)
MATCH p = (a:Cuenta)-[:ENVIA]->(b:Cuenta)
WHERE a.etiqueta = 0 AND b.etiqueta = 0 AND a.score = 0 AND a.ciudad = 'Ibagué'
RETURN p LIMIT 80;


// =====================================================================================
// PARTE 4 - TABLAS PARA EL INFORME (mirar en vista "Table")
// =====================================================================================

// 4.1 Resumen general
MATCH (c:Cuenta)
RETURN count(c) AS cuentas,
       sum(c.regla_mula) AS mulas_detectadas,
       sum(c.regla_smurfing) AS smurfing_detectadas,
       sum(CASE WHEN c:Ciclo THEN 1 ELSE 0 END) AS cuentas_en_ciclos,
       sum(c.etiqueta) AS cuentas_con_fraude_real;

// 4.2 Top 10 mulas
MATCH (c:Cuenta {regla_mula: 1})
RETURN c.id AS cuenta, c.remitentes AS remitentes, c.ciudad AS ciudad, c.score AS score
ORDER BY remitentes DESC LIMIT 10;

// 4.3 Comunidades mas grandes de Louvain
MATCH (c:Cuenta)
RETURN c.comunidad AS comunidad, count(*) AS cuentas, round(avg(c.etiqueta) * 100) AS pct_fraude
ORDER BY cuentas DESC LIMIT 10;

// 4.4 Tamanos de los grupos aislados (componentes conectados pequenos)
MATCH (c:Cuenta)
WITH c.componente AS componente, count(*) AS cuentas
WHERE cuentas <= 14
RETURN cuentas AS tamano_del_grupo, count(*) AS grupos
ORDER BY tamano_del_grupo;

// 4.5 Validacion: cuantos grupos aislados (3 a 14 cuentas) son 100 % fraude (esperado: ~652 de ~652)
MATCH (c:Cuenta)
WITH c.componente AS componente, count(*) AS cuentas, sum(c.etiqueta) AS con_fraude
WHERE cuentas >= 3 AND cuentas <= 14
RETURN count(*) AS grupos, sum(CASE WHEN con_fraude = cuentas THEN 1 ELSE 0 END) AS grupos_100pct_fraude;
