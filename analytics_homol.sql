SELECT 1;

---------------------------------  
-- 	PEDIDOS (DB_SALE) / COMPRA --
---------------------------------

DROP TABLE IF EXISTS analytics_compras;
CREATE TABLE analytics_compras AS
WITH analytics_compras AS (
SELECT pd.id AS compra_id
	,pd.cliente_id
	,pd.nome
	,lower(pd.email) AS email
	,pd.valor
	,pd.frete
	,pd.comercio_id
	,cm.nome_fantasia
	,pd.status
	,CASE
		WHEN pd.status IN ('Cancelado', 'Pagamento recusado') THEN 0
		WHEN cm.nome_fantasia LIKE '%- Test%' THEN 0
	ELSE 1 END status_pedido
	,DATE_FORMAT(pd.time, '%Y-%m-%d') data_compra
	,concat(quarter(pd.time), "-", year(pd.time)) quarter
FROM pedidos pd
LEFT JOIN comercios cm ON pd.comercio_id = cm.id)
	SELECT *
	FROM analytics_compras acp
	ORDER BY compra_id DESC;
SELECT * FROM analytics_compras;

-------------------------------------
-- 	CLIENTES (DB_USERS) / CADASTRO --
------------------------------------- 

DROP TABLE IF EXISTS analytics_clientes;
CREATE TABLE analytics_clientes AS

WITH analytics_clientes AS (
SELECT cl.id AS cliente_id
	,concat(cl.nome, " ", cl.sobrenome) AS nome_completo
	,lower(cl.email) AS email
	,REPLACE(celular, '-', '' ) AS celular
	,cl.cpf
	,cl.cnpj
-- 	,FLOOR(DATEDIFF(CURRENT_DATE, cl.data_nascimento) / 365.25) AS idade
	,cl.tipo_pessoa
	,CASE
		WHEN (cl.email IN ('angelo.bodelon@yahoo.com.br', 'loretorestaurante@gmail.com', 'marcos.gica@uol.com.br', 'Compras@purana.co',
			'juliana.castanheira94@gmail.com', 'renataeuze123@gmail.com', 'financeiro@afornadapadaria.com.br', 'georgios.loukopoulos94@gmail.com',
			'edusalg@gmail.com', 'lumoscardo@hotmail.com', 'Cantinagigio@uol.com.br', 'dpilikian@hotmail.com', 'zettedias9@gmail.com', 
			'joyce@redrestaurante.com.br', 'beatriz_ap_arantes@hotmail.com', 'santochopprh@gmail.com', 'saborpremium@saborpremium.com.br', 
			'compras@purana.com', 'Brunozonta89@hotmail.com', 'ian.lyrio.costa@gmail.com', 'franklin@confs.com.br', 'gui_pribeiro@hotmail.com',
			'pri@bananaverde.com.br', 'Juniorgreat2003@yahoo.com.br', 'arthurluigi13@gmail.com', 'dza1966@gmail.com', 'paulofelipefreitas@gmail.com',
			'Jane_katia17@hotmail.com', 'mahamantravegetariano@gmail.com', 'beatrizpachecofisioterapia@yahoo.com.br', 'compras@primadonnapizzaria.com.br',
			'compras@lugar166.com.br', 'purana.zonaoestesp@gmail.com', 'theodoramessora.ibe@gmail.com', 'danifuracaodobras@hotmail.com',
			'nkamargo@terra.com.br', 'jef1967@yahoo.com') OR cl.cpf IS NULL) THEN "ATACADO"
		WHEN cl.email IN ('marioprimarano@yahoo.com.br', 'ulissesdantas333@gmail.com', 'rrdoces@gmail.com', 'mreidasofertas@gmail.com',
			'lojapullmansm@gmail.com', 'atendimento@phsustentabilidade.com.br', 'beto_lima1977@hotmail.com', 'canato01@hotmail.com',
			'angsouza72@hotmail.com') THEN "MINIMARKET"
	ELSE "VAREJO" END purchase_type
	,e.cep
	,e.bairro
	,e.cidade
	,e.estado
	,cl.status
	,CASE WHEN cl.status IN ('Ativo') AND email <> 'deleted' THEN 1 ELSE 0 END status_cliente
	,DATE_FORMAT(cl.time, '%Y-%m-%d') data_cadastro
	,concat(quarter(cl.time), "-", year(cl.time)) quarter
	,ROW_NUMBER() OVER(PARTITION BY e.cliente_id, nome ORDER BY e.id DESC) AS row_num
FROM clientes cl
	LEFT JOIN enderecos e ON cl.id = e.cliente_id)
		SELECT *
		FROM analytics_clientes acl
		WHERE row_num = '1'
			AND email NOT IN ('deleted')
		ORDER BY cliente_id DESC;
SELECT * FROM analytics_clientes;

---------------------------------
-- 	DOWNLOADS (DB_APP) / LOJAS --
---------------------------------

SELECT * FROM db_app;

----------------------------------
-- CLIENTES X COMPRAS ANALYTICS --
----------------------------------

SELECT dbcl.id AS cliente_id
	,dbcl.nome_completo
	,dbcl.email
	,dbcl.cpf 
	,dbcl.cnpj
	,dbcl.cep
	,dbcl.bairro
	,dbcl.cidade
	,dbcl.estado
	,dbcl.purchase_type
	,dbcl.data_cadastro
	,dbcp.id AS compra_id
	,dbcp.nome_fantasia
	,dbcp.valor
	,dbcp.frete
	,dbcp.status
	,dbcp.data_compra
FROM analytics_clientes dbcl
	JOIN analytics_compras dbcp ON dbcl.id = dbcp.cliente_id;

-------------------------

-- DADOS APP (VISITAS, DOWNLOADS)

SELECT * FROM analytics_app;

SELECT sum(total_visitas) AS visitas_2022
	,sum(total_downloads) AS downloads_2022
FROM analytics_app;

SELECT
	CASE WHEN data IS NOT NULL THEN date_format(data, '%m-%Y') END mes_ano
	,sum(total_visitas) visitas_mes
	,sum(total_downloads) downloads_mes	
FROM analytics_app
GROUP BY mes_ano;

-------------------------

-- DADOS CLIENTES (CADASTROS, TIPO DE CADASTRO)

SELECT * FROM analytics_clientes;

SELECT COUNT(cliente_id) AS cadastros_2022
FROM analytics_clientes
WHERE quarter IN ('1/2022', '2/2022', '3/2022', '4/2022');

SELECT COUNT(cliente_id) AS cadastros_ativos_2022
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022');

WITH base AS(
SELECT str_to_date(data_cadastro, '%d/%m/%Y') data_cadastro
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022'))
	SELECT
	CASE WHEN data_cadastro IS NOT NULL THEN date_format(data_cadastro, '%m-%Y') END mes_ano
	,COUNT(data_cadastro) cadastro_ativos_mes
	FROM base
	GROUP BY mes_ano
	ORDER BY 1 ASC;

SELECT cliente_id, nome_completo, purchase_type
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022')
GROUP BY cliente_id
ORDER BY 3,1 ASC;

SELECT purchase_type
	,COUNT(purchase_type) AS tipos_clientes_2022
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022')
GROUP BY purchase_type;

SELECT cep
	,COUNT(cep) AS cadastros_cep
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022')
GROUP BY cep
ORDER BY 2 DESC;

SELECT bairro
	,COUNT(bairro) AS cadastros_bairro
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022')
GROUP BY bairro
ORDER BY 2 DESC;

SELECT cidade
	,COUNT(cidade) AS cadastros_cidade
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022')
	AND cidade IS NOT NULL
GROUP BY cidade
ORDER BY 2 DESC;

SELECT estado
	,COUNT(estado) AS cadastros_estado
FROM analytics_clientes
WHERE status_cliente = '1'
	AND email NOT IN ('deleted')
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022')
	AND estado <> '1'
GROUP BY estado
ORDER BY 2 DESC;

-------------------------

-- DADOS COMPRAS (PEDIDOS, VALORES, SELLERS)

SELECT * FROM analytics_compras;

WITH base AS(
SELECT str_to_date(data_compra, '%d/%m/%Y') data_compra
	,valor
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023'))
	SELECT
	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,SUM(valor) gmv
	FROM base
	GROUP BY mes_ano;

WITH base AS(
SELECT str_to_date(data_compra, '%d/%m/%Y') data_compra
	,compra_id 
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023'))
	SELECT
	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,count(compra_id) pedidos
	FROM base
	GROUP BY mes_ano;

SELECT COUNT(compra_id) AS pedidos_2022
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023');

WITH base AS(
SELECT str_to_date(data_compra, '%d/%m/%Y') data_compra
	,compra_id
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023'))
	SELECT
	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,COUNT(compra_id) pedidos_mes
	FROM base
	GROUP BY mes_ano;

SELECT COUNT(DISTINCT(cliente_id)) AS qtd_clientes_compraram
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023');

SELECT cliente_id, nome, email
	,COUNT(compra_id) AS qtd_compra
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023')
GROUP BY cliente_id
ORDER BY 4 DESC;

SELECT cliente_id, nome, email
	,SUM(valor) AS valor_total_compras 
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023')
GROUP BY cliente_id
ORDER BY 4 DESC;

WITH base AS(
SELECT compra_id, cliente_id, nome, email, valor, str_to_date(data_compra, '%d/%m/%Y') data_ultima_compra
	,ROW_NUMBER() OVER(PARTITION BY cliente_id, nome ORDER BY data_ultima_compra DESC) AS row_num
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023')
ORDER BY cliente_id, row_num ASC)
	SELECT cliente_id, nome, email, data_ultima_compra
		,DATEDIFF(CURRENT_DATE(), data_ultima_compra) diff_compra_dias
	FROM base
	WHERE row_num = '1'
	GROUP BY cliente_id
	ORDER BY 5 ASC;

SELECT nome_fantasia AS seller
	,COUNT(nome_fantasia) AS qtd_vendas
	,SUM(valor) AS valor_vendas 
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023')
GROUP BY nome_fantasia 
ORDER BY 3 DESC;

WITH base AS(
SELECT nome_fantasia AS seller
	,str_to_date(data_compra, '%d/%m/%Y') data_compra
FROM analytics_compras
WHERE status_pedido = '1'
	AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022', '1/2023'))
	SELECT
	CASE WHEN data_compra IS NOT NULL THEN date_format(data_compra, '%m-%Y') END mes_ano
	,seller
	,COUNT(data_compra) venda_seller_mes
	FROM base
	GROUP BY seller
	ORDER BY 1 ASC, 3 DESC;

-------------------------

DROP TABLE IF EXISTS growth_funil;

CREATE TABLE growth_funil AS
WITH base_1 AS (
SELECT sum(total_visitas) AS visitas_2022
	,sum(total_downloads) AS downloads_2022
FROM analytics_app)
	, base_2 AS (
	SELECT COUNT(cliente_id) AS cadastros_2022
	FROM analytics_clientes
	WHERE quarter IN ('1/2022', '2/2022', '3/2022', '4/2022'))
		, base_3 AS (
		SELECT COUNT(cliente_id) AS cadastros_ativos_2022
		FROM analytics_clientes
		WHERE status_cliente = '1'
			AND email NOT IN ('deleted')
			AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022'))
			, base_4 AS (
			SELECT COUNT(DISTINCT(cliente_id)) AS qtd_clientes_compraram
			FROM analytics_compras
			WHERE status_pedido = '1'
				AND quarter IN ('1/2022', '2/2022', '3/2022', '4/2022'))
				, base_5 AS (
				SELECT COUNT(DISTINCT(cliente_id)) AS qtd_clientes_compraram_quarter
				FROM analytics_compras
				WHERE status_pedido = '1'
					AND quarter IN ('4/2022'))
				SELECT * FROM base_1 CROSS JOIN base_2 CROSS JOIN base_3 CROSS JOIN base_4 CROSS JOIN base_5;

SELECT * FROM growth_funil;