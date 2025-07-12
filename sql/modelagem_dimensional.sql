-- ==========================================
-- MODELAGEM DIMENSIONAL - DATA WAREHOUSE
-- Estrutura Star Schema para Análise de Vendas
-- ==========================================

-- ==========================================
-- TABELAS DE DIMENSÃO (DIMENSION TABLES)
-- ==========================================

-- Dimensão Produto
CREATE TABLE IF NOT EXISTS dim_produto (
    produto_key SERIAL PRIMARY KEY,
    produto_id VARCHAR(50) UNIQUE NOT NULL,
    nome_produto VARCHAR(100) NOT NULL,
    categoria VARCHAR(50),
    subcategoria VARCHAR(50),
    marca VARCHAR(50),
    faixa_preco VARCHAR(20),
    ativo BOOLEAN DEFAULT TRUE,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensão Cliente
CREATE TABLE IF NOT EXISTS dim_cliente (
    cliente_key SERIAL PRIMARY KEY,
    cliente_id VARCHAR(50) UNIQUE NOT NULL,
    nome_cliente VARCHAR(100),
    email VARCHAR(100),
    telefone VARCHAR(20),
    segmento_cliente VARCHAR(30),
    data_primeiro_pedido DATE,
    status_cliente VARCHAR(20) DEFAULT 'Ativo',
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensão Região/Geografia
CREATE TABLE IF NOT EXISTS dim_regiao (
    regiao_key SERIAL PRIMARY KEY,
    regiao_id VARCHAR(10) UNIQUE NOT NULL,
    nome_regiao VARCHAR(50) NOT NULL,
    pais VARCHAR(50) DEFAULT 'Brasil',
    estado VARCHAR(50),
    zona VARCHAR(20), -- Norte, Sul, Leste, Oeste, Central
    populacao INTEGER,
    ativo BOOLEAN DEFAULT TRUE,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimensão Tempo
CREATE TABLE IF NOT EXISTS dim_tempo (
    tempo_key SERIAL PRIMARY KEY,
    data_completa DATE UNIQUE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    nome_mes VARCHAR(20) NOT NULL,
    nome_dia_semana VARCHAR(20) NOT NULL,
    numero_dia_semana INTEGER NOT NULL, -- 1=Segunda, 7=Domingo
    semana_ano INTEGER NOT NULL,
    eh_fim_semana BOOLEAN NOT NULL,
    eh_feriado BOOLEAN DEFAULT FALSE,
    nome_feriado VARCHAR(50),
    estacao VARCHAR(20) -- Primavera, Verão, Outono, Inverno
);

-- ==========================================
-- TABELA FATO (FACT TABLE)
-- ==========================================

-- Fato Vendas
CREATE TABLE IF NOT EXISTS fato_vendas (
    venda_key SERIAL PRIMARY KEY,
    
    -- Chaves estrangeiras para dimensões
    produto_key INTEGER REFERENCES dim_produto(produto_key),
    cliente_key INTEGER REFERENCES dim_cliente(cliente_key),
    regiao_key INTEGER REFERENCES dim_regiao(regiao_key),
    tempo_key INTEGER REFERENCES dim_tempo(tempo_key),
    
    -- Identificadores de negócio
    venda_id VARCHAR(50),
    numero_pedido VARCHAR(50),
    
    -- Métricas (medidas)
    quantidade INTEGER NOT NULL,
    valor_unitario DECIMAL(10, 2) NOT NULL,
    valor_total DECIMAL(10, 2) NOT NULL,
    desconto_aplicado DECIMAL(10, 2) DEFAULT 0,
    valor_liquido DECIMAL(10, 2) NOT NULL,
    custo_produto DECIMAL(10, 2),
    margem_bruta DECIMAL(10, 2),
    
    -- Metadados
    data_venda TIMESTAMP NOT NULL,
    data_criacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    data_atualizacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ==========================================
-- ÍNDICES PARA PERFORMANCE
-- ==========================================

-- Índices nas chaves estrangeiras da tabela fato
CREATE INDEX IF NOT EXISTS idx_fato_vendas_produto ON fato_vendas(produto_key);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_cliente ON fato_vendas(cliente_key);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_regiao ON fato_vendas(regiao_key);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_tempo ON fato_vendas(tempo_key);

-- Índices nas dimensões
CREATE INDEX IF NOT EXISTS idx_dim_produto_categoria ON dim_produto(categoria);
CREATE INDEX IF NOT EXISTS idx_dim_cliente_segmento ON dim_cliente(segmento_cliente);
CREATE INDEX IF NOT EXISTS idx_dim_regiao_zona ON dim_regiao(zona);
CREATE INDEX IF NOT EXISTS idx_dim_tempo_ano_mes ON dim_tempo(ano, mes);

-- Índice composto para consultas temporais frequentes
CREATE INDEX IF NOT EXISTS idx_fato_vendas_data ON fato_vendas(data_venda);
CREATE INDEX IF NOT EXISTS idx_fato_vendas_valor ON fato_vendas(valor_total);

-- ==========================================
-- VIEWS PARA ANÁLISES FREQUENTES
-- ==========================================

-- View: Vendas Detalhadas
CREATE OR REPLACE VIEW vw_vendas_detalhadas AS
SELECT 
    fv.venda_key,
    fv.venda_id,
    fv.numero_pedido,
    
    -- Produto
    dp.nome_produto,
    dp.categoria,
    dp.subcategoria,
    dp.marca,
    
    -- Cliente
    dc.cliente_id,
    dc.nome_cliente,
    dc.segmento_cliente,
    
    -- Região
    dr.nome_regiao,
    dr.zona,
    dr.estado,
    
    -- Tempo
    dt.data_completa,
    dt.ano,
    dt.mes,
    dt.nome_mes,
    dt.trimestre,
    dt.nome_dia_semana,
    dt.eh_fim_semana,
    
    -- Métricas
    fv.quantidade,
    fv.valor_unitario,
    fv.valor_total,
    fv.desconto_aplicado,
    fv.valor_liquido,
    fv.margem_bruta,
    fv.data_venda
    
FROM fato_vendas fv
JOIN dim_produto dp ON fv.produto_key = dp.produto_key
JOIN dim_cliente dc ON fv.cliente_key = dc.cliente_key
JOIN dim_regiao dr ON fv.regiao_key = dr.regiao_key
JOIN dim_tempo dt ON fv.tempo_key = dt.tempo_key;

-- View: Resumo Mensal de Vendas
CREATE OR REPLACE VIEW vw_vendas_mensais AS
SELECT 
    dt.ano,
    dt.mes,
    dt.nome_mes,
    dr.zona,
    dp.categoria,
    
    COUNT(*) as total_vendas,
    SUM(fv.quantidade) as total_quantidade,
    SUM(fv.valor_total) as receita_bruta,
    SUM(fv.valor_liquido) as receita_liquida,
    SUM(fv.margem_bruta) as margem_total,
    AVG(fv.valor_total) as ticket_medio,
    
    COUNT(DISTINCT fv.cliente_key) as clientes_unicos,
    COUNT(DISTINCT fv.produto_key) as produtos_vendidos
    
FROM fato_vendas fv
JOIN dim_produto dp ON fv.produto_key = dp.produto_key
JOIN dim_cliente dc ON fv.cliente_key = dc.cliente_key
JOIN dim_regiao dr ON fv.regiao_key = dr.regiao_key
JOIN dim_tempo dt ON fv.tempo_key = dt.tempo_key
GROUP BY dt.ano, dt.mes, dt.nome_mes, dr.zona, dp.categoria;

-- View: Top Produtos por Região
CREATE OR REPLACE VIEW vw_top_produtos_regiao AS
WITH ranking_produtos AS (
    SELECT 
        dr.nome_regiao,
        dp.nome_produto,
        dp.categoria,
        SUM(fv.valor_liquido) as receita_total,
        SUM(fv.quantidade) as quantidade_total,
        COUNT(*) as vendas_count,
        ROW_NUMBER() OVER (PARTITION BY dr.nome_regiao ORDER BY SUM(fv.valor_liquido) DESC) as ranking
    FROM fato_vendas fv
    JOIN dim_produto dp ON fv.produto_key = dp.produto_key
    JOIN dim_regiao dr ON fv.regiao_key = dr.regiao_key
    GROUP BY dr.nome_regiao, dp.nome_produto, dp.categoria
)
SELECT *
FROM ranking_produtos
WHERE ranking <= 10;

-- ==========================================
-- STORED PROCEDURES PARA ETL
-- ==========================================

-- Procedure para popular dimensão tempo
CREATE OR REPLACE FUNCTION popular_dim_tempo(
    data_inicio DATE,
    data_fim DATE
) RETURNS INTEGER AS $$
DECLARE
    data_atual DATE;
    contador INTEGER := 0;
BEGIN
    data_atual := data_inicio;
    
    WHILE data_atual <= data_fim LOOP
        INSERT INTO dim_tempo (
            data_completa,
            ano,
            mes,
            dia,
            trimestre,
            nome_mes,
            nome_dia_semana,
            numero_dia_semana,
            semana_ano,
            eh_fim_semana,
            estacao
        ) VALUES (
            data_atual,
            EXTRACT(YEAR FROM data_atual),
            EXTRACT(MONTH FROM data_atual),
            EXTRACT(DAY FROM data_atual),
            EXTRACT(QUARTER FROM data_atual),
            TO_CHAR(data_atual, 'Month'),
            TO_CHAR(data_atual, 'Day'),
            EXTRACT(DOW FROM data_atual),
            EXTRACT(WEEK FROM data_atual),
            EXTRACT(DOW FROM data_atual) IN (0, 6), -- Domingo = 0, Sábado = 6
            CASE 
                WHEN EXTRACT(MONTH FROM data_atual) IN (12, 1, 2) THEN 'Verão'
                WHEN EXTRACT(MONTH FROM data_atual) IN (3, 4, 5) THEN 'Outono'
                WHEN EXTRACT(MONTH FROM data_atual) IN (6, 7, 8) THEN 'Inverno'
                ELSE 'Primavera'
            END
        ) ON CONFLICT (data_completa) DO NOTHING;
        
        data_atual := data_atual + INTERVAL '1 day';
        contador := contador + 1;
    END LOOP;
    
    RETURN contador;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- PROCEDURE PARA MIGRAR DADOS EXISTENTES
-- ==========================================

-- Migrar dados da tabela sales_data para o modelo dimensional
CREATE OR REPLACE FUNCTION migrar_dados_para_dimensional() 
RETURNS TEXT AS $$
DECLARE
    resultado TEXT := '';
    contador_produtos INTEGER := 0;
    contador_clientes INTEGER := 0;
    contador_regioes INTEGER := 0;
    contador_vendas INTEGER := 0;
BEGIN
    -- Popular dimensão produto
    INSERT INTO dim_produto (produto_id, nome_produto, categoria, faixa_preco)
    SELECT DISTINCT 
        LOWER(REPLACE(product_name, ' ', '_')) as produto_id,
        product_name,
        CASE 
            WHEN product_name ILIKE '%laptop%' OR product_name ILIKE '%desktop%' THEN 'Computadores'
            WHEN product_name ILIKE '%monitor%' THEN 'Monitores'
            WHEN product_name ILIKE '%mouse%' OR product_name ILIKE '%keyboard%' THEN 'Periféricos'
            WHEN product_name ILIKE '%printer%' THEN 'Impressoras'
            WHEN product_name ILIKE '%tablet%' OR product_name ILIKE '%smartphone%' THEN 'Dispositivos Móveis'
            ELSE 'Acessórios'
        END as categoria,
        CASE 
            WHEN sales_amount < 100 THEN 'Baixo'
            WHEN sales_amount BETWEEN 100 AND 500 THEN 'Médio'
            WHEN sales_amount BETWEEN 500 AND 1000 THEN 'Alto'
            ELSE 'Premium'
        END as faixa_preco
    FROM sales_data
    ON CONFLICT (produto_id) DO NOTHING;
    
    GET DIAGNOSTICS contador_produtos = ROW_COUNT;
    
    -- Popular dimensão cliente
    INSERT INTO dim_cliente (cliente_id, segmento_cliente)
    SELECT DISTINCT 
        customer_id,
        CASE 
            WHEN SUM(sales_amount) > 2000 THEN 'Premium'
            WHEN SUM(sales_amount) > 1000 THEN 'Alto Valor'
            WHEN SUM(sales_amount) > 500 THEN 'Médio Valor'
            ELSE 'Baixo Valor'
        END as segmento_cliente
    FROM sales_data
    WHERE customer_id IS NOT NULL AND customer_id != ''
    GROUP BY customer_id
    ON CONFLICT (cliente_id) DO NOTHING;
    
    GET DIAGNOSTICS contador_clientes = ROW_COUNT;
    
    -- Popular dimensão região
    INSERT INTO dim_regiao (regiao_id, nome_regiao, zona)
    SELECT DISTINCT 
        LOWER(region) as regiao_id,
        region as nome_regiao,
        region as zona
    FROM sales_data
    WHERE region IS NOT NULL
    ON CONFLICT (regiao_id) DO NOTHING;
    
    GET DIAGNOSTICS contador_regioes = ROW_COUNT;
    
    -- Popular dimensão tempo (para os últimos 2 anos)
    PERFORM popular_dim_tempo(CURRENT_DATE - INTERVAL '2 years', CURRENT_DATE + INTERVAL '1 year');
    
    -- Popular fato vendas
    INSERT INTO fato_vendas (
        produto_key, cliente_key, regiao_key, tempo_key,
        venda_id, quantidade, valor_unitario, valor_total, valor_liquido, data_venda
    )
    SELECT 
        dp.produto_key,
        dc.cliente_key,
        dr.regiao_key,
        dt.tempo_key,
        sd.id::TEXT as venda_id,
        sd.quantity as quantidade,
        sd.sales_amount as valor_unitario,
        sd.sales_amount * sd.quantity as valor_total,
        sd.sales_amount * sd.quantity as valor_liquido,
        sd.sale_date::TIMESTAMP as data_venda
    FROM sales_data sd
    JOIN dim_produto dp ON dp.produto_id = LOWER(REPLACE(sd.product_name, ' ', '_'))
    LEFT JOIN dim_cliente dc ON dc.cliente_id = sd.customer_id
    JOIN dim_regiao dr ON dr.regiao_id = LOWER(sd.region)
    JOIN dim_tempo dt ON dt.data_completa = sd.sale_date::DATE;
    
    GET DIAGNOSTICS contador_vendas = ROW_COUNT;
    
    resultado := FORMAT('Migração concluída: %s produtos, %s clientes, %s regiões, %s vendas',
                       contador_produtos, contador_clientes, contador_regioes, contador_vendas);
    
    RETURN resultado;
END;
$$ LANGUAGE plpgsql;

-- ==========================================
-- CONSULTAS DE EXEMPLO PARA ANÁLISE
-- ==========================================

-- Para executar a migração:
-- SELECT migrar_dados_para_dimensional();

-- Análise de vendas por trimestre e categoria:
/*
SELECT 
    ano,
    trimestre,
    categoria,
    SUM(receita_liquida) as receita_total,
    COUNT(*) as total_vendas
FROM vw_vendas_mensais
GROUP BY ano, trimestre, categoria
ORDER BY ano, trimestre, receita_total DESC;
*/

-- Top 5 produtos por região:
/*
SELECT * FROM vw_top_produtos_regiao 
WHERE ranking <= 5
ORDER BY nome_regiao, ranking;
*/