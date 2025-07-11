-- ==========================================
-- CONSULTAS ANALÍTICAS - SUPABASE POSTGRESQL
-- ==========================================

-- 1. ANÁLISE DE VENDAS POR REGIÃO
-- Top regiões por receita total
SELECT 
    region,
    total_sales,
    total_revenue,
    avg_sale_amount,
    ROUND((total_revenue / (SELECT SUM(total_revenue) FROM region_aggregates) * 100), 2) as revenue_percentage
FROM region_aggregates 
ORDER BY total_revenue DESC;

-- 2. ANÁLISE DE PRODUTOS
-- Top 10 produtos mais vendidos por receita
SELECT 
    product_name,
    total_sales,
    total_revenue,
    avg_sale_amount,
    region_count as regions_sold
FROM product_aggregates 
ORDER BY total_revenue DESC 
LIMIT 10;

-- Produtos com melhor ticket médio
SELECT 
    product_name,
    avg_sale_amount,
    total_sales,
    total_revenue
FROM product_aggregates 
WHERE total_sales >= 5  -- Mínimo 5 vendas
ORDER BY avg_sale_amount DESC 
LIMIT 10;

-- 3. ANÁLISE TEMPORAL
-- Vendas por mês
SELECT 
    sale_month,
    COUNT(*) as total_transactions,
    SUM(sales_amount) as monthly_revenue,
    AVG(sales_amount) as avg_transaction_value,
    SUM(quantity) as total_quantity_sold
FROM sales_data 
WHERE sale_month IS NOT NULL
GROUP BY sale_month 
ORDER BY sale_month;

-- Vendas por trimestre
SELECT 
    sale_quarter,
    sale_year,
    COUNT(*) as total_transactions,
    SUM(sales_amount) as quarterly_revenue,
    AVG(sales_amount) as avg_transaction_value
FROM sales_data 
WHERE sale_quarter IS NOT NULL
GROUP BY sale_quarter, sale_year 
ORDER BY sale_year, sale_quarter;

-- 4. ANÁLISE DE PERFORMANCE POR REGIÃO E PRODUTO
-- Combinação região x produto mais lucrativa
SELECT 
    region,
    product_name,
    COUNT(*) as sales_count,
    SUM(sales_amount) as total_revenue,
    AVG(sales_amount) as avg_sale_amount,
    SUM(quantity) as total_quantity
FROM sales_data
GROUP BY region, product_name
HAVING COUNT(*) >= 2  -- Mínimo 2 vendas
ORDER BY total_revenue DESC
LIMIT 20;

-- 5. ANÁLISE DE CLIENTES
-- Top clientes por valor de compra
SELECT 
    customer_id,
    COUNT(*) as purchase_count,
    SUM(sales_amount) as total_spent,
    AVG(sales_amount) as avg_purchase_value,
    SUM(quantity) as total_items_purchased,
    MIN(sale_date) as first_purchase,
    MAX(sale_date) as last_purchase
FROM sales_data
WHERE customer_id IS NOT NULL AND customer_id != ''
GROUP BY customer_id
ORDER BY total_spent DESC
LIMIT 20;

-- 6. ANÁLISE DE DISTRIBUIÇÃO DE PREÇOS
-- Distribuição de vendas por faixa de preço
SELECT 
    CASE 
        WHEN sales_amount < 50 THEN 'Baixo (< $50)'
        WHEN sales_amount BETWEEN 50 AND 200 THEN 'Médio ($50-$200)'
        WHEN sales_amount BETWEEN 200 AND 500 THEN 'Alto ($200-$500)'
        WHEN sales_amount BETWEEN 500 AND 1000 THEN 'Premium ($500-$1000)'
        ELSE 'Luxo (> $1000)'
    END as price_range,
    COUNT(*) as transaction_count,
    SUM(sales_amount) as total_revenue,
    ROUND(AVG(sales_amount), 2) as avg_amount
FROM sales_data
GROUP BY 
    CASE 
        WHEN sales_amount < 50 THEN 'Baixo (< $50)'
        WHEN sales_amount BETWEEN 50 AND 200 THEN 'Médio ($50-$200)'
        WHEN sales_amount BETWEEN 200 AND 500 THEN 'Alto ($200-$500)'
        WHEN sales_amount BETWEEN 500 AND 1000 THEN 'Premium ($500-$1000)'
        ELSE 'Luxo (> $1000)'
    END
ORDER BY 
    MIN(CASE 
        WHEN sales_amount < 50 THEN 1
        WHEN sales_amount BETWEEN 50 AND 200 THEN 2
        WHEN sales_amount BETWEEN 200 AND 500 THEN 3
        WHEN sales_amount BETWEEN 500 AND 1000 THEN 4
        ELSE 5
    END);

-- 7. MÉTRICAS GERAIS DO NEGÓCIO
-- Dashboard executivo
SELECT 
    COUNT(*) as total_transactions,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT product_name) as products_sold,
    COUNT(DISTINCT region) as regions_active,
    SUM(sales_amount) as total_revenue,
    AVG(sales_amount) as avg_transaction_value,
    SUM(quantity) as total_units_sold,
    MIN(sale_date) as first_sale_date,
    MAX(sale_date) as last_sale_date
FROM sales_data;

-- Receita média por cliente
WITH customer_revenue AS (
    SELECT 
        customer_id,
        SUM(sales_amount) as customer_total
    FROM sales_data
    WHERE customer_id IS NOT NULL AND customer_id != ''
    GROUP BY customer_id
)
SELECT 
    COUNT(*) as total_customers,
    ROUND(AVG(customer_total), 2) as avg_revenue_per_customer,
    ROUND(MIN(customer_total), 2) as min_customer_revenue,
    ROUND(MAX(customer_total), 2) as max_customer_revenue
FROM customer_revenue;

-- 8. ANÁLISE DE CRESCIMENTO
-- Crescimento mês a mês (se tivermos dados de múltiplos meses)
WITH monthly_sales AS (
    SELECT 
        sale_month,
        SUM(sales_amount) as monthly_revenue,
        COUNT(*) as monthly_transactions
    FROM sales_data 
    WHERE sale_month IS NOT NULL
    GROUP BY sale_month
    ORDER BY sale_month
),
growth_analysis AS (
    SELECT 
        sale_month,
        monthly_revenue,
        monthly_transactions,
        LAG(monthly_revenue) OVER (ORDER BY sale_month) as prev_month_revenue,
        LAG(monthly_transactions) OVER (ORDER BY sale_month) as prev_month_transactions
    FROM monthly_sales
)
SELECT 
    sale_month,
    monthly_revenue,
    monthly_transactions,
    prev_month_revenue,
    CASE 
        WHEN prev_month_revenue IS NOT NULL AND prev_month_revenue > 0 
        THEN ROUND(((monthly_revenue - prev_month_revenue) / prev_month_revenue * 100), 2)
        ELSE NULL 
    END as revenue_growth_percent,
    CASE 
        WHEN prev_month_transactions IS NOT NULL AND prev_month_transactions > 0 
        THEN ROUND(((monthly_transactions - prev_month_transactions) / prev_month_transactions::NUMERIC * 100), 2)
        ELSE NULL 
    END as transaction_growth_percent
FROM growth_analysis
ORDER BY sale_month;

-- 9. ANÁLISE DE SAZONALIDADE
-- Performance por dia da semana (se sale_date estiver disponível)
SELECT 
    EXTRACT(DOW FROM sale_date::DATE) as day_of_week,
    CASE EXTRACT(DOW FROM sale_date::DATE)
        WHEN 0 THEN 'Domingo'
        WHEN 1 THEN 'Segunda'
        WHEN 2 THEN 'Terça'
        WHEN 3 THEN 'Quarta'
        WHEN 4 THEN 'Quinta'
        WHEN 5 THEN 'Sexta'
        WHEN 6 THEN 'Sábado'
    END as day_name,
    COUNT(*) as transaction_count,
    SUM(sales_amount) as daily_revenue,
    AVG(sales_amount) as avg_transaction_value
FROM sales_data
GROUP BY EXTRACT(DOW FROM sale_date::DATE)
ORDER BY day_of_week;

-- 10. PRODUTOS QUE PRECISAM DE ATENÇÃO
-- Produtos com baixa performance
SELECT 
    product_name,
    total_sales,
    total_revenue,
    avg_sale_amount,
    region_count
FROM product_aggregates 
WHERE total_sales < 3 OR avg_sale_amount < 100
ORDER BY total_revenue ASC;

-- Regiões com baixa performance
SELECT 
    region,
    total_sales,
    total_revenue,
    avg_sale_amount,
    product_count
FROM region_aggregates 
WHERE total_revenue < (SELECT AVG(total_revenue) FROM region_aggregates)
ORDER BY total_revenue ASC;