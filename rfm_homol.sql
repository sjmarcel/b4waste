SELECT 1;

--------------------------------------
-- ANALYTICS RFM TOTAL
--------------------------------------

-- DROP TABLE IF EXISTS analytics_rfm;

CREATE TABLE analytics_rfm AS
WITH base_1 AS (
SELECT cliente_id
	,COUNT(compra_id) AS qtd_compra
FROM analytics_compras
WHERE status_pedido = '1'
	AND data_compra BETWEEN '20220101' AND NOW()
GROUP BY cliente_id
ORDER BY 1 DESC)
	, base_2 AS (
	SELECT cliente_id
		,SUM(valor) AS valor_total_compras 
	FROM analytics_compras
	WHERE status_pedido = '1'
		AND data_compra BETWEEN '20220101' AND NOW()	
	GROUP BY cliente_id
	ORDER BY 1 DESC)
		, base_3 AS (
		WITH base AS(
		SELECT compra_id, cliente_id, nome, email, valor, data_compra AS data_ultima_compra
			,ROW_NUMBER() OVER(PARTITION BY cliente_id, nome ORDER BY data_ultima_compra DESC) AS row_num
		FROM analytics_compras
		WHERE status_pedido = '1'
			AND data_compra BETWEEN '20220101' AND NOW()
		ORDER BY cliente_id, row_num ASC)
			SELECT cliente_id, nome, email, data_ultima_compra
				,DATEDIFF(CURRENT_DATE(), data_ultima_compra) ultima_compra_dias
				,TIMESTAMPDIFF(MONTH, data_ultima_compra, CURRENT_DATE()) ultima_compra_meses
			FROM base b
			WHERE row_num = '1'
			GROUP BY cliente_id
			ORDER BY 1 DESC)
		SELECT b1.cliente_id, nome, email, data_ultima_compra
			,ultima_compra_meses
			,ultima_compra_dias R
			,TRUNCATE(((ultima_compra_dias - AVG(ultima_compra_dias) OVER ()) / (STDDEV(ultima_compra_dias) OVER ())),3) AS deviation_R
			,qtd_compra F
			,TRUNCATE(((qtd_compra - AVG(qtd_compra) OVER ()) / (STDDEV(qtd_compra) OVER ())),3) AS deviation_F
			,valor_total_compras M
			,TRUNCATE(((valor_total_compras - AVG(valor_total_compras) OVER ()) / (STDDEV(valor_total_compras) OVER ())),3) AS deviation_M
			FROM base_1 b1
			JOIN base_2 b2 ON b2.cliente_id = b1.cliente_id
			JOIN base_3 b3 ON b3.cliente_id = b1.cliente_id
			ORDER BY 4 DESC;
		
SELECT * FROM analytics_rfm;

--------------------------------------
-- GROWTH RFM TOTAL
--------------------------------------

-- DROP TABLE IF EXISTS growth_rfm;
-- DROP TABLE IF EXISTS growth_rfm_retail;
-- DROP TABLE IF EXISTS growth_rfm_wholesale;

-- CREATE TABLE growth_rfm AS
-- CREATE TABLE growth_rfm_retail AS
-- CREATE TABLE growth_rfm_wholesale AS
WITH base_1 AS (
SELECT *
	,TRUNCATE((M / F),2) AS ticket
FROM analytics_rfm),
	base_2 AS (
	SELECT cliente_id
		,quarter
	FROM analytics_compras
	GROUP BY 1),
		base_3 AS (
		SELECT cliente_id
-- 		,idade
		,celular
		,purchase_type
		,data_cadastro
		FROM analytics_clientes
		WHERE status_cliente = '1'
		GROUP BY 1)
			SELECT b1.cliente_id, b1.nome, b1.email, b3.celular, b3.purchase_type
			,b3.data_cadastro
			,b1.data_ultima_compra
			,b2.quarter
			,b1.ticket
			,b1.ultima_compra_meses
			,b1.R
-- 			,b1.deviation_R
			,b1.F
-- 			,b1.deviation_F
			,b1.M
-- 			,b1.deviation_M
			,CASE
				WHEN b1.deviation_R <= '-1.01' THEN 5
				WHEN b1.deviation_R BETWEEN '-1.01' AND '-0.42' THEN 4
				WHEN b1.deviation_R BETWEEN '-0.42' AND '-0.62' THEN 3
				WHEN b1.deviation_R BETWEEN '-0.62' AND '0.14' THEN 2
				WHEN b1.deviation_R >= '0.14' THEN 1
			ELSE 0 END score_R
			,CASE
				WHEN b1.deviation_F <= '-0.26' THEN 1
				WHEN b1.deviation_F BETWEEN '-0.26' AND '-0.17' THEN 2
				WHEN b1.deviation_F BETWEEN '-0.17' AND '0.26' THEN 3
				WHEN b1.deviation_F BETWEEN '0.26' AND '1.00' THEN 4
				WHEN b1.deviation_F >= '1.0' THEN 5
			ELSE 0 END score_F
			,CASE
				WHEN b1.deviation_M <= '-0.02' THEN 1
				WHEN b1.deviation_M BETWEEN '-0.02' AND '0.06' THEN 2
				WHEN b1.deviation_M BETWEEN '0.06' AND '0.24' THEN 3
				WHEN b1.deviation_M BETWEEN '0.24' AND '1.00' THEN 4
				WHEN b1.deviation_M >= '1.00' THEN 5
			ELSE 0 END score_M
			FROM base_1 b1
				LEFT JOIN base_2 b2 ON b2.cliente_id = b1.cliente_id
				LEFT JOIN base_3 b3 ON b3.cliente_id = b1.cliente_id
				WHERE R > '0'
					AND data_cadastro BETWEEN '20220101' AND NOW()
-- 					AND b3.purchase_type = 'VAREJO'
					AND b3.purchase_type = 'ATACADO'
			ORDER BY data_ultima_compra DESC;

SELECT *, (score_R * score_F * score_M) score FROM growth_rfm;
SELECT *, (score_R * score_F * score_M) score FROM growth_rfm_retail;
SELECT *, (score_R * score_F * score_M) score FROM growth_rfm_wholesale;

--------------------------------------

-- DROP TABLE IF EXISTS base1_retail;
-- DROP TABLE IF EXISTS base2_retail;

-- CREATE TABLE base1_retail AS
CREATE TABLE base2_retail AS
SELECT gr.*
	,(score_R * score_F * score_M) score
	,compra_id
	,nome_fantasia AS seller
	,valor AS valor_compra
	,data_compra
FROM growth_rfm_retail gr
	LEFT JOIN analytics_compras ac ON gr.cliente_id = ac.cliente_id
WHERE F > '5'
	AND status_pedido = '1'
ORDER BY 1 DESC, 19 ASC;

-------------------------

SELECT * FROM base1_retail;
SELECT * FROM base2_retail;

-- DROP TABLE IF EXISTS b1_retail;
-- DROP TABLE IF EXISTS b2_retail;

-- CREATE TABLE b1_retail AS
-- CREATE TABLE b2_retail AS
SELECT br.cliente_id, br.nome, br.email, bairro, cidade, br.celular, br.purchase_type, br.data_cadastro, br.ultima_compra_meses
	,br.R, br.F, br.M, br.score_R, br.score_F, br.score_M, br.score, br.compra_id, br.seller, br.valor_compra, br.data_compra
	,concat(WEEKOFYEAR(data_compra),"-", YEAR (data_compra)) semana, DAY(data_compra) dia,  MONTH (data_compra) mes, YEAR (data_compra) ano
FROM base1_retail br
-- FROM base2_retail br
	LEFT JOIN analytics_clientes ac ON br.cliente_id = ac.cliente_id;

SELECT * FROM b1_retail;
SELECT * FROM b2_retail;

-------------------------

SELECT b2.cliente_id, b2.compra_id, b2.nome, b2.seller,b2.valor_compra
	,c.produto_id
FROM b2_retail b2
	LEFT JOIN carrinho c ON b2.cliente_id = c.cliente_id
ORDER BY 1 DESC;

SELECT b2.cliente_id, b2.compra_id, b2.nome, b2.seller,b2.valor_compra
	,resumo
	,SUBSTRING_INDEX(resumo, '       ', 1)
	,SUBSTRING_INDEX(resumo, '       ', 2)
	,SUBSTRING_INDEX(resumo, '       ', -2)
-- 	,SUBSTRING(resumo, '       ')
-- 	,REPLACE (resumo , '       ', ';')
-- 	,trim(BOTH '       ' FROM resumo)
-- 	,trim(BOTH '¶' FROM resumo)	
FROM b2_retail b2
	LEFT JOIN pedidos p ON b2.cliente_id = p.id
ORDER BY 1 DESC
LIMIT 10;

--------------------------------------

SELECT cliente_id
	,M total_compras
	,F qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
FROM base1_retail
-- FROM b2_retail
GROUP BY cliente_id
ORDER BY 1 DESC;
	
SELECT
	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM base1_retail
-- FROM b2_retail
-- WHERE F BETWEEN '6' AND '10'
-- WHERE F BETWEEN '11' AND '20'
-- WHERE F > '20'
GROUP BY mes_ano
ORDER BY 1 ASC;

SELECT cliente_id
	,count(compra_id) qtd_compras
	,count(DISTINCT semana) qtd_semanas
	,count(DISTINCT mes) qtd_meses
	,truncate(F / count(DISTINCT semana),2) recorrencia_semanal
	,truncate(F / count(DISTINCT mes),2) recorrencia_mensal
FROM b1_retail
-- FROM b2_retail
GROUP BY cliente_id
ORDER BY 1 DESC;

-------------------------

SELECT
	bairro
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b1_retail
-- FROM b2_retail
GROUP BY bairro
ORDER BY 2 DESC;

SELECT bairro
	,data_compra
-- 	,CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
FROM b2_retail
-- GROUP BY mes_ano
ORDER BY 1,2 ASC;

-------------------------

SELECT
	cidade
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b1_retail
-- FROM b2_retail
GROUP BY cidade
ORDER BY 2 DESC;

SELECT
	cidade
	,CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b1_retail
-- FROM b2_retail
GROUP BY mes_ano
ORDER BY 2 DESC;

-------------------------

SELECT
	seller
-- 	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b1_retail
-- FROM b2_retail
GROUP BY seller
-- GROUP BY mes_ano
ORDER BY 2 DESC;

---------------------------------------------
-- ANALYTICS RFM TRIMESTRE POR data_compra --
---------------------------------------------

DROP TABLE IF EXISTS analytics_rfm_quarter;

CREATE TABLE analytics_rfm_quarter AS
WITH base_1 AS (
SELECT cliente_id
	,COUNT(compra_id) AS qtd_compra
FROM analytics_compras
WHERE status_pedido = '1'
	AND data_compra BETWEEN '20221001' AND NOW() 
GROUP BY cliente_id
ORDER BY 2 DESC
	), base_2 AS (
	SELECT cliente_id
		,SUM(valor) AS valor_total_compras 
	FROM analytics_compras
	WHERE status_pedido = '1'
		AND data_compra BETWEEN '20221001' AND NOW()
	GROUP BY cliente_id
	ORDER BY 1 DESC
		), base_3 AS (
		WITH base AS(
		SELECT compra_id, cliente_id, nome, email, valor, data_compra AS data_ultima_compra
			,ROW_NUMBER() OVER(PARTITION BY cliente_id, nome ORDER BY data_ultima_compra DESC) AS row_num
		FROM analytics_compras
		WHERE status_pedido = '1'
			AND data_compra BETWEEN '20221001' AND NOW()
		ORDER BY cliente_id, row_num ASC)
				SELECT cliente_id, nome, email, data_ultima_compra
					,TIMESTAMPDIFF(MONTH, data_ultima_compra, CURRENT_DATE()) ultima_compra_meses
					,DATEDIFF(CURRENT_DATE(), data_ultima_compra) ultima_compra_dias
				FROM base b
				WHERE row_num = '1'
				GROUP BY cliente_id
				ORDER BY 1 DESC)
			SELECT b1.cliente_id, nome, email, data_ultima_compra
				,ultima_compra_meses
				,ultima_compra_dias R
				,TRUNCATE(((ultima_compra_dias - AVG(ultima_compra_dias) OVER ()) / (STDDEV(ultima_compra_dias) OVER ())),3) AS deviation_R
				,qtd_compra F
				,TRUNCATE(((qtd_compra - AVG(qtd_compra) OVER ()) / (STDDEV(qtd_compra) OVER ())),3) AS deviation_F
				,valor_total_compras M
				,TRUNCATE(((valor_total_compras - AVG(valor_total_compras) OVER ()) / (STDDEV(valor_total_compras) OVER ())),3) AS deviation_M
			FROM base_1 b1
				JOIN base_2 b2 ON b2.cliente_id = b1.cliente_id
				JOIN base_3 b3 ON b3.cliente_id = b1.cliente_id
				WHERE ultima_compra_dias > '0'
			ORDER BY 6 DESC;

SELECT * FROM analytics_rfm_quarter;

--------------------------------------------
-- GROWTH RFM TRIMESTRE POR data_cadastro --
--------------------------------------------

-- DROP TABLE IF EXISTS growth_rfm_quarter;
-- DROP TABLE IF EXISTS growth_rfm_retail_quarter;
-- DROP TABLE IF EXISTS growth_rfm_wholesale_quarter;

-- CREATE TABLE growth_rfm_quarter AS
-- CREATE TABLE growth_rfm_retail_quarter AS
-- CREATE TABLE growth_rfm_wholesale_quarter AS
WITH base_1 AS (
SELECT *
FROM analytics_rfm_quarter)
	, base_2 AS (
	SELECT cliente_id
		,celular
		,purchase_type
		,data_cadastro
	FROM analytics_clientes
	WHERE status_cliente = '1'
	GROUP BY 1)
		SELECT b1.cliente_id, nome, email, celular, purchase_type
			,data_cadastro
			,data_ultima_compra
			,ultima_compra_meses
			,R
-- 			,deviation_R
			,F
-- 			,deviation_F
			,M
-- 			,deviation_M
			,CASE
				WHEN deviation_R <= '-1.1' THEN 5
				WHEN deviation_R BETWEEN '-1.1' AND '-0.9' THEN 4
				WHEN deviation_R BETWEEN '-0.9' AND '-0.2' THEN 3
				WHEN deviation_R BETWEEN '-0.2' AND '0.71' THEN 2
				WHEN deviation_R > '0.7' THEN 1
			ELSE 0 END score_R
			,CASE
				WHEN deviation_F < '-0.4' THEN 1
				WHEN deviation_F BETWEEN '-0.4' AND '-0.3' THEN 2
				WHEN deviation_F BETWEEN '-0.3' AND '-0.2' THEN 3
				WHEN deviation_F BETWEEN '-0.2' AND '0.2' THEN 4
				WHEN deviation_F > '0.2' THEN 5
			ELSE 0 END score_F
			,CASE
				WHEN deviation_M <= '-0.043' THEN 1
				WHEN deviation_M BETWEEN '-0.044' AND '-0.040' THEN 2
				WHEN deviation_M BETWEEN '-0.041' AND '-0.034' THEN 3
				WHEN deviation_M BETWEEN '-0.035' AND '0.000' THEN 4
				WHEN deviation_M > '0.000' THEN 5
			ELSE 0 END score_M	
		FROM base_1 b1
			LEFT JOIN base_2 b2 ON b2.cliente_id = b1.cliente_id
			WHERE R > '0'
				AND data_cadastro BETWEEN '20221001' AND NOW()
-- 				AND purchase_type = 'VAREJO'
				AND purchase_type = 'ATACADO'
		ORDER BY data_cadastro, R ASC;

SELECT *, (score_R * score_F * score_M) score FROM growth_rfm_quarter;
SELECT *, (score_R * score_F * score_M) score FROM growth_rfm_retail_quarter;
SELECT *, (score_R * score_F * score_M) score FROM growth_rfm_wholesale_quarter;

--------------------------------------

-- DROP TABLE IF EXISTS base1_retail_quarter;
-- DROP TABLE IF EXISTS base2_retail_quarter;

CREATE TABLE base1_retail_quarter AS
CREATE TABLE base2_retail_quarter AS
SELECT grq.*
	,(score_R * score_F * score_M) score
	,compra_id
	,nome_fantasia AS seller
	,valor AS valor_compra
	,data_compra
FROM growth_rfm_retail_quarter grq
	LEFT JOIN analytics_compras ac ON grq.cliente_id = ac.cliente_id
-- WHERE F > '5'
	AND status_pedido = '1'
ORDER BY 1 DESC, 19 ASC;
SELECT * FROM base1_retail_quarter;
SELECT * FROM base2_retail_quarter;

-- DROP TABLE IF EXISTS b1_retail_quarter;
-- DROP TABLE IF EXISTS b2_retail_quarter;

CREATE TABLE b1_retail_quarter AS
SELECT brq.cliente_id, brq.nome, brq.email, bairro, cidade, brq.celular, brq.purchase_type, brq.data_cadastro, brq.ultima_compra_meses
	,brq.R, brq.F, brq.M, brq.score_R, brq.score_F, brq.score_M, brq.score, brq.compra_id, brq.seller, brq.valor_compra, brq.data_compra
	,concat(WEEKOFYEAR(data_compra),"-", YEAR (data_compra)) semana, DAY(data_compra) dia,  MONTH (data_compra) mes, YEAR (data_compra) ano
FROM base1_retail_quarter brq
	LEFT JOIN analytics_clientes ac ON brq.cliente_id = ac.cliente_id;
SELECT * FROM b1_retail_quarter;
SELECT * FROM b2_retail_quarter;

SELECT b2.cliente_id
	,c.produto_id
FROM b2_retail_quarter b2
	LEFT JOIN carrinho c ON b2.cliente_id = c.cliente_id
ORDER BY 1 DESC;

SELECT b2.cliente_id
	,resumo
	,SUBSTRING_INDEX(resumo, '\n', 1)
-- 	,SUBSTRING_INDEX(resumo, '\n', 2)
FROM b2_retail_quarter b2
	LEFT JOIN pedidos p ON b2.cliente_id = p.id
ORDER BY 1 DESC;

--------------------------------------

SELECT cliente_id
	,M total_compras
	,F qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
FROM b2_retail_quarter
GROUP BY cliente_id
ORDER BY 1 DESC;

SELECT
	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b2_retail_quarter
-- WHERE F BETWEEN '6' AND '10'
-- WHERE F BETWEEN '11' AND '20'
-- WHERE F > '20'
GROUP BY mes_ano
ORDER BY 1 ASC;

SELECT cliente_id
	,count(compra_id) qtd_compra
	,count(DISTINCT semana) qtd_semanas
	,count(DISTINCT mes) qtd_meses
	,truncate(F / count(DISTINCT semana),2) recorrencia_semanal
	,truncate(F / count(DISTINCT mes),2) recorrencia_mensal
FROM b2_retail_quarter
GROUP BY cliente_id
ORDER BY 1 DESC;

-------------------------

SELECT
	bairro
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b2_retail_quarter
GROUP BY bairro
ORDER BY 2 DESC;

SELECT bairro
	,data_compra
-- 	,CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
FROM b2_retail_quarter
-- GROUP BY mes_ano
ORDER BY 1,2 ASC;

-------------------------

SELECT
	cidade
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b2_retail_quarter
GROUP BY cidade
ORDER BY 2 DESC;

SELECT
	cidade
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b2_retail_quarter
GROUP BY mes_ano
ORDER BY 2 DESC;

-------------------------

SELECT
	seller
-- 	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,sum(valor_compra) total_compras
	,COUNT(compra_id) qtd_compras
	,TRUNCATE(sum(valor_compra) / count(compra_id),2) valor_medio_pedido
	,COUNT(DISTINCT cliente_id) qtd_clientes
	,TRUNCATE(sum(valor_compra) / count(DISTINCT cliente_id),2)	ticket_medio
FROM b2_retail_quarter
GROUP BY seller
-- GROUP BY mes_ano
ORDER BY 2 DESC;

-------------------------

WITH base_1 AS (
SELECT cadastros_2022, cadastros_ativos_2022, qtd_clientes_compraram_quarter
FROM growth_funil)
, base_2 AS (
	SELECT count(DISTINCT cliente_id) compras_2022
	FROM analytics_compras
	WHERE status_pedido = '1'
		AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022'))
		, base_3 AS (
		SELECT count(cliente_id) qtd_clientes_compraram_old
		FROM growth_rfm gr
		WHERE data_ultima_compra >= '2022-01-01'
			AND data_ultima_compra <= '2022-09-30'
			AND qtd_compra > '3')
			SELECT cadastros_2022, cadastros_ativos_2022, compras_2022, qtd_clientes_compraram_quarter
				,TRUNCATE((qtd_clientes_compraram_quarter / cadastros_ativos_2022 * 100),2) AS "%_compras_quarter"
				, qtd_clientes_compraram_old
				,TRUNCATE((qtd_clientes_compraram_old / cadastros_ativos_2022 * 100),2) AS "%_compras_old"			
			FROM base_1 b1 CROSS JOIN base_2 b2 CROSS JOIN base_3 b3;
		
-------------------------

WITH base_1 AS (
SELECT COUNT(cliente_id) compras_total
FROM growth_rfm)
,base_2 AS (
SELECT COUNT(cliente_id) compras_quarter
FROM growth_rfm
WHERE ultima_compra_meses <= '2')
	,base_2a AS (
	SELECT COUNT(cliente_id) compras_maior_quarter
	FROM growth_rfm
	WHERE ultima_compra_meses > '2')
,base_3 AS (
SELECT COUNT(cliente_id) compras_1_vez
FROM growth_rfm
WHERE qtd_compra = '1')
	,base_3a AS (
	SELECT COUNT(cliente_id) compras_1_vez_M1
	FROM growth_rfm
	WHERE qtd_compra = '1'
		AND ultima_compra_meses > '0')
			,base_3b AS (
			SELECT COUNT(cliente_id) compras_1_vez_M2
			FROM growth_rfm
			WHERE qtd_compra = '1'
				AND ultima_compra_meses > '1')
				,base_3c AS (
				SELECT COUNT(cliente_id) compras_1_vez_M3
				FROM growth_rfm
				WHERE qtd_compra = '1'
					AND ultima_compra_meses > '2')
,base_4 AS (
SELECT COUNT(cliente_id) compras_2_vezes
FROM growth_rfm
WHERE qtd_compra = '2')
	,base_4a AS (
	SELECT COUNT(cliente_id) compras_2_vezes_M1
	FROM growth_rfm
	WHERE qtd_compra = '2'
		AND ultima_compra_meses > '0')
		,base_4b AS (
		SELECT COUNT(cliente_id) compras_2_vezes_M2
		FROM growth_rfm
		WHERE qtd_compra = '2'
			AND ultima_compra_meses > '1')
			,base_4c AS (
			SELECT COUNT(cliente_id) compras_2_vezes_M3
			FROM growth_rfm
			WHERE qtd_compra = '2'
				AND ultima_compra_meses > '2')
,base_5 AS (
SELECT COUNT(cliente_id) compras_3_vezes
FROM growth_rfm
WHERE qtd_compra = '3')
	,base_5a AS (
	SELECT COUNT(cliente_id) compras_3_vezes_M1
	FROM growth_rfm
	WHERE qtd_compra = '3'
		AND ultima_compra_meses > '0')
		,base_5b AS (
		SELECT COUNT(cliente_id) compras_3_vezes_M2
		FROM growth_rfm
		WHERE qtd_compra = '3'
			AND ultima_compra_meses > '1')
			,base_5c AS (
			SELECT COUNT(cliente_id) compras_3_vezes_M3
			FROM growth_rfm
			WHERE qtd_compra = '3'
				AND ultima_compra_meses > '2')
SELECT *
	,TRUNCATE((compras_maior_quarter / compras_total * 100),2) AS "%_compras_maior_quarter"
	,TRUNCATE((compras_1_vez / compras_total * 100),2) AS "%_compras_1_vez"
	,TRUNCATE((compras_1_vez_M1 / compras_1_vez * 100),2) AS "%_compras_1_vez_M1"
	,TRUNCATE((compras_1_vez_M2 / compras_1_vez * 100),2) AS "%_compras_1_vez_M2"	
	,TRUNCATE((compras_1_vez_M3 / compras_1_vez * 100),2) AS "%_compras_1_vez_M3"		
	,TRUNCATE((compras_2_vezes / compras_total * 100),2) AS "%_compras_2_vezes"
	,TRUNCATE((compras_2_vezes_M1 / compras_2_vezes * 100),2) AS "%_compras_2_vezes_M1"
	,TRUNCATE((compras_2_vezes_M2 / compras_2_vezes * 100),2) AS "%_compras_2_vezes_M2"
	,TRUNCATE((compras_2_vezes_M3 / compras_2_vezes * 100),2) AS "%_compras_2_vezes_M3"	
	,TRUNCATE((compras_3_vezes / compras_total * 100),2) AS "%_compras_3_vezes"	
	,TRUNCATE((compras_3_vezes_M1 / compras_3_vezes * 100),2) AS "%_compras_3_vezes_M1"
	,TRUNCATE((compras_3_vezes_M2 / compras_3_vezes * 100),2) AS "%_compras_3_vezes_M2"
	,TRUNCATE((compras_3_vezes_M3 / compras_3_vezes * 100),2) AS "%_compras_3_vezes_M3"	
FROM base_1
CROSS JOIN base_2
	CROSS JOIN base_2a
CROSS JOIN base_3
	CROSS JOIN base_3a
	CROSS JOIN base_3b
	CROSS JOIN base_3c
CROSS JOIN base_4
	CROSS JOIN base_4a
	CROSS JOIN base_4b
	CROSS JOIN base_4c
CROSS JOIN base_5
	CROSS JOIN base_5a
	CROSS JOIN base_5b
	CROSS JOIN base_5c;

-------------------------

SELECT 1;

WITH base_1 AS (
SELECT *
	,CASE
		WHEN ultima_compra_dias <= '90' THEN 1
	ELSE 0 END "quarter"
FROM growth_rfm_old
ORDER BY 4 DESC)
	, base_2 AS (
	SELECT COUNT(cliente_id) compras_quarter
		,MAX(qtd_compra) qtd_max_quarter
		,AVG(qtd_compra) avg_quarter
	FROM base_1
	WHERE quarter = '1')
		, base_3 AS (
		SELECT COUNT(cliente_id) compras_old
			,MAX(qtd_compra) qtd_max_old
			,AVG(qtd_compra) avg_old
		FROM base_1
		WHERE quarter = '0')
			, base_4 AS (	
			SELECT COUNT(cliente_id) compras_2022
				,MAX(qtd_compra) qtd_max_2022
				,AVG(qtd_compra) avg_2022
			FROM base_1)
SELECT * FROM base_4 CROSS JOIN base_2 b2 CROSS JOIN base_3 b3;	
		
-------------------------

SELECT
	CASE WHEN data_cadastro IS NOT NULL THEN date_format(data_cadastro, '%m-%Y') END mes_ano
	,COUNT(DISTINCT cliente_id) qtd_clientes
FROM analytics_clientes
WHERE purchase_type = 'VAREJO'
GROUP BY mes_ano
ORDER BY 1 ASC;

-------------------------

SELECT
	bairro
-- 	,CASE WHEN data_cadastro IS NOT NULL THEN date_format(data_cadastro, '%m-%Y') END mes_ano
	,COUNT(DISTINCT cliente_id) qtd_clientes
FROM analytics_clientes
WHERE bairro IN (
'Jardim Presidente Dutra',
'Centro',
'Jardim Paulista',
'Pinheiros',
'Consolação',
'Vila Olímpia',
'Itaim Bibi',
'Vila Mariana',
'Jardim Ema',
'Bela Vista',
'Cerqueira César',
'Vila Matilde',
'Vila Nova Conceição',
'Indianópolis',
'Mirandópolis',
'Gopouva',
'Jardim Taboão',
'Vila Augusta',
'Vila Suzana'
	)
	AND purchase_type = 'VAREJO'	
GROUP BY bairro
ORDER BY 2 DESC;

-------------------------

SELECT
	cidade
-- 	,CASE WHEN data_cadastro IS NOT NULL THEN date_format(data_cadastro, '%m-%Y') END mes_ano
	,COUNT(DISTINCT cliente_id) qtd_clientes
FROM analytics_clientes
WHERE cidade IN (
'São Paulo',
'Guarulhos',
'Rio de Janeiro',
'Barueri',
'Osasco',
'Itapevi',
'Jandira',
'São Bernardo do Campo',
'Belo Horizonte',
'Santo André',	
'Sorocaba',
'Carapicuíba'
)
	AND purchase_type = 'VAREJO'
GROUP BY cidade
ORDER BY 2 DESC;