// =====================================================================
// 02 - DETECCION DE FRAUDE CON GRAFOS
// Ejecutar cada consulta por separado. Se detecta SIN usar las etiquetas de fraude;
// las etiquetas (c.etiqueta) solo se usan al final para medir que tan bien se detecto.
// =====================================================================

// ---------- A. CICLOS DE 3 CUENTAS (A -> B -> C -> A) -----------------------------------
// Dinero que da vueltas: cada salto repetido (n_tx >= 2), todo en una ventana de 6 horas
// y con montos casi iguales (el fraude solo "pierde" una comision pequena en cada salto).
MATCH (a:Cuenta)-[r1:ENVIA]->(b:Cuenta)-[r2:ENVIA]->(c:Cuenta)-[r3:ENVIA]->(a)
WHERE a.id < b.id AND a.id < c.id
  AND r1.n_tx >= 2 AND r2.n_tx >= 2 AND r3.n_tx >= 2
  AND abs(r1.primera - r2.primera) <= 21600 AND abs(r2.primera - r3.primera) <= 21600
  AND r2.monto_total / r1.monto_total >= 0.8 AND r2.monto_total / r1.monto_total <= 1.25
  AND r3.monto_total / r2.monto_total >= 0.8 AND r3.monto_total / r2.monto_total <= 1.25
RETURN a.id AS cuenta_1, b.id AS cuenta_2, c.id AS cuenta_3,
       round(r1.monto_total) AS monto_salto_1, r1.n_tx AS vueltas
ORDER BY monto_salto_1 DESC
LIMIT 50;

// ---------- B. CICLOS DE 4 A 5 CUENTAS ----------------------------------------------------
// Mismo criterio; el predicado sobre las relaciones se evalua mientras se expande el camino.
MATCH p = (a:Cuenta)-[rs:ENVIA*4..5]->(a)
WHERE all(r IN rs WHERE r.n_tx >= 2 AND r.ultima - r.primera <= 21600)
  AND all(n IN nodes(p)[1..-1] WHERE a.id < n.id)
RETURN [n IN nodes(p)[..-1] | n.id] AS cuentas_del_ciclo, length(p) AS largo
LIMIT 50;

// ---------- C. CUENTAS MULA: muchos remitentes -> una cuenta -> sale casi todo a un destino --
MATCH (v:Cuenta)-[e:ENVIA]->(m:Cuenta)
WITH m, count(DISTINCT v) AS remitentes, sum(e.monto_total) AS recibido
WHERE remitentes >= 10
MATCH (m)-[s:ENVIA]->(f:Cuenta)
WHERE s.monto_total >= 0.5 * recibido
RETURN m.id AS cuenta_mula, remitentes, round(recibido) AS recibido_cop,
       f.id AS destino_final, round(s.monto_total) AS enviado_al_destino
ORDER BY remitentes DESC
LIMIT 50;

// ---------- D. CAMINOS CORTOS SOSPECHOSOS: victima -> mula -> destino final (2 saltos) -----
// Que cuentas finales reciben dinero a traves de varias mulas.
MATCH (m:Cuenta {regla_mula: 1})-[:ENVIA]->(f:Cuenta)
MATCH (v:Cuenta)-[:ENVIA]->(m)
RETURN f.id AS cuenta_final, count(DISTINCT m) AS mulas_intermedias, count(DISTINCT v) AS victimas_alcanzadas
ORDER BY victimas_alcanzadas DESC
LIMIT 20;

// Camino mas corto entre dos cuentas cualquiera (cambiar los ids). Util para investigar un caso:
MATCH (a:Cuenta {id: 'ACC000006'}), (b:Cuenta {id: 'ACC000100'})
MATCH p = shortestPath((a)-[:ENVIA*..6]->(b))
RETURN [n IN nodes(p) | n.id] AS ruta, length(p) AS saltos;

// ---------- E. COMUNIDADES CON LOUVAIN (Graph Data Science) ----------------------------------
CALL gds.graph.drop('fraude', false);
CALL gds.graph.project('fraude', 'Cuenta',
     {ENVIA: {orientation: 'UNDIRECTED', properties: 'n_tx'}});
CALL gds.louvain.write('fraude', {relationshipWeightProperty: 'n_tx', writeProperty: 'comunidad', seed: 7})
YIELD communityCount, modularity;

// Comunidades pequenas y muy activas por dentro = grupos coordinados
MATCH (a:Cuenta)-[e:ENVIA]->(b:Cuenta)
WHERE a.comunidad = b.comunidad
WITH a.comunidad AS comunidad, count(DISTINCT a) AS cuentas, count(e) AS aristas, sum(e.n_tx) AS tx_internas
WHERE cuentas >= 8 AND cuentas <= 40 AND tx_internas * 1.0 / cuentas >= 5
RETURN comunidad, cuentas, aristas, tx_internas, round(tx_internas * 1.0 / cuentas, 1) AS tx_por_cuenta
ORDER BY tx_por_cuenta DESC
LIMIT 30;

// ---------- F. COMPONENTES CONECTADOS: grupos aislados del resto de la red ---------------------
CALL gds.wcc.write('fraude', {writeProperty: 'componente'}) YIELD componentCount;
MATCH (c:Cuenta)
WITH c.componente AS componente, count(*) AS cuentas, collect(c.id)[..5] AS ejemplo
WHERE cuentas >= 3 AND cuentas <= 14
RETURN cuentas AS tamano_del_grupo, count(*) AS grupos_aislados, collect(ejemplo)[0] AS cuentas_de_ejemplo
ORDER BY tamano_del_grupo;

// ---------- G. VALIDACION (aqui si se usan las etiquetas): ¿que % de los grupos aislados es fraude? --
MATCH (c:Cuenta)
WITH c.componente AS componente, count(*) AS cuentas, sum(c.etiqueta) AS con_fraude
WHERE cuentas >= 3 AND cuentas <= 14
RETURN count(*) AS grupos, sum(CASE WHEN con_fraude = cuentas THEN 1 ELSE 0 END) AS grupos_100pct_fraude;
