-- =====================================================================
-- Reporte de riesgo (BigQuery / Snowflake). Tablas: fraude.transacciones y fraude.cuentas_riesgo
-- SQL estandar: funciona igual en BigQuery y Snowflake (cambiando solo el nombre del esquema).
-- =====================================================================

-- Q1. ¿Que canal concentra mas fraude? (tasa y dinero en riesgo)
SELECT canal,
       COUNT(*)                                             AS transacciones,
       SUM(es_fraude)                                       AS transacciones_fraude,
       ROUND(100.0 * SUM(es_fraude) / COUNT(*), 2)          AS tasa_fraude_pct,
       ROUND(SUM(CASE WHEN es_fraude = 1 THEN monto END), 0) AS monto_en_riesgo_cop
FROM fraude.transacciones
GROUP BY canal
ORDER BY monto_en_riesgo_cop DESC;

-- Q2. ¿Que ciudades tienen mas dinero fraudulento y cual es su participacion?
SELECT ciudad,
       SUM(es_fraude)                                        AS transacciones_fraude,
       ROUND(SUM(CASE WHEN es_fraude = 1 THEN monto END), 0) AS monto_fraude_cop,
       ROUND(100.0 * SUM(CASE WHEN es_fraude = 1 THEN monto END)
             / SUM(SUM(CASE WHEN es_fraude = 1 THEN monto END)) OVER (), 2) AS pct_del_total_fraude
FROM fraude.transacciones
GROUP BY ciudad
ORDER BY monto_fraude_cop DESC
LIMIT 10;

-- Q3. ¿Como se comporta cada tipo de fraude? (volumen, ticket promedio y horario nocturno)
SELECT patron_fraude,
       COUNT(*)                                   AS transacciones,
       ROUND(SUM(monto), 0)                       AS monto_total_cop,
       ROUND(AVG(monto), 0)                       AS ticket_promedio_cop,
       ROUND(100.0 * AVG(es_nocturna), 1)         AS pct_nocturnas
FROM fraude.transacciones
WHERE es_fraude = 1
GROUP BY patron_fraude
ORDER BY monto_total_cop DESC;

-- Q4. Evolucion mensual de la tasa de fraude y variacion frente al mes anterior
WITH mensual AS (
  SELECT EXTRACT(MONTH FROM fecha_hora)                       AS mes,
         COUNT(*)                                             AS transacciones,
         ROUND(100.0 * SUM(es_fraude) / COUNT(*), 2)          AS tasa_fraude_pct
  FROM fraude.transacciones
  GROUP BY EXTRACT(MONTH FROM fecha_hora)
  HAVING COUNT(*) >= 1000          -- se omite el mes incompleto del borde del periodo
)
SELECT mes, transacciones, tasa_fraude_pct,
       ROUND(tasa_fraude_pct - LAG(tasa_fraude_pct) OVER (ORDER BY mes), 2) AS variacion_vs_mes_anterior
FROM mensual
ORDER BY mes;

-- Q5. Top 10 cuentas de mayor riesgo para revision manual (score de Spark + metricas)
SELECT id_cuenta, ciudad, score_riesgo, remitentes_distintos, destinatarios_distintos,
       monto_recibido, monto_salida_total, ratio_salida_entrada, etiqueta_fraude
FROM fraude.cuentas_riesgo
WHERE score_riesgo >= 60
ORDER BY score_riesgo DESC, remitentes_distintos DESC, monto_recibido DESC
LIMIT 10;
