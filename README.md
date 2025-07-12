# Data Engineering Pipeline – Personal Project

Este projeto simula um pipeline completo de dados para extrair, transformar, orquestrar e carregar dados em um ambiente moderno de data warehouse usando **Supabase PostgreSQL**.

## 🚀 Tech Stack

- **Python** (pandas, requests, SQLAlchemy, psycopg2)
- **Supabase PostgreSQL** (banco de dados na nuvem)
- **Apache Airflow** (orquestração de DAGs)
- **Jupyter Notebook** (exploração inicial)
- **GitHub Actions** (CI/CD opcional)

## 📊 Pipeline Flow

1. Extração de dados de APIs públicas ou dados simulados
2. Transformação e limpeza usando Pandas
3. Armazenamento no Supabase PostgreSQL
4. Orquestração via Apache Airflow

![Architecture](docs/arquitetura_pipeline.png)

## �️ Estrutura do Projeto

| Pasta       | Descrição                                    |
|-------------|----------------------------------------------|
| `src/`      | Scripts Python para ETL (extract, transform, load) |
| `src/config/` | Configurações de banco de dados            |
| `src/database/` | Módulos de conexão com banco              |
| `src/etl/`  | Módulos ETL (extract, transform, load)     |
| `dags/`     | Definições de DAGs do Apache Airflow       |
| `sql/`      | Consultas SQL (modelagem dimensional)      |
| `notebooks/`| Exploração e validação de dados            |
| `data/`     | Arquivos CSV brutos e processados          |
| `docs/`     | Diagramas e documentação técnica           |

## 🏗️ Configuração do Supabase

### 1. Criar conta no Supabase

1. Acesse: [https://app.supabase.com/](https://app.supabase.com/)
2. Faça login com GitHub ou email
3. Clique em **"New Project"**

### 2. Configurar o projeto

Preencha os campos:

| Campo                 | Valor sugerido                   |
| --------------------- | -------------------------------- |
| **Project name**      | `data-engineering-pipeline`      |
| **Database password** | Use uma senha forte (ex: `MinhaSenh@2025!`) |
| **Region**            | Mantenha a sugerida              |

⚠️ **Importante:** Guarde a senha do banco! Você precisará para conectar.

### 3. Obter credenciais de conexão

1. Vá até: **Project Settings → Database**
2. Copie as informações de conexão:

| Dado              | Exemplo                     |
| ----------------- | --------------------------- |
| **Host**          | `db.xxxxxxx.supabase.co`    |
| **Port**          | `5432`                      |
| **Database name** | `postgres`                  |
| **User**          | `postgres`                  |
| **Password**      | A senha que você criou      |

## ⚙️ Configuração do Ambiente Local

### 1. Instalar dependências

```bash
pip install -r requirements.txt
```

### 2. Configurar variáveis de ambiente

1. Copie o arquivo de exemplo:
```bash
cp .env.example .env
```

2. Edite o arquivo `.env` com suas credenciais do Supabase:
```env
SUPABASE_HOST=db.xxxxxxx.supabase.co
SUPABASE_PORT=5432
SUPABASE_DBNAME=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=sua_senha_aqui
```

### 3. Testar conexão

Execute o script principal para testar a conexão:
```bash
python src/main.py
```

## 🔄 Executando o Pipeline ETL

### Pipeline Completo

Execute o pipeline ETL completo:
```bash
python src/etl_pipeline.py
```

Este pipeline irá:
1. **Extrair** dados simulados de vendas
2. **Transformar** e limpar os dados
3. **Carregar** no Supabase PostgreSQL
4. Criar agregações por região e produto

### Componentes Individuais

Você também pode executar componentes ETL individualmente:

```python
from src.etl import get_extractor, get_transformer, get_loader

# Extração
extractor = get_extractor()
data = extractor.extract_sample_sales_data()

# Transformação
transformer = get_transformer()
cleaned_data = transformer.clean_sales_data(data)

# Carregamento
loader = get_loader()
loader.create_tables()
loader.load_sales_data(cleaned_data)
```

## 📊 Explorando os Dados

### Via Supabase Dashboard

1. Acesse seu projeto no Supabase
2. Vá para **Table Editor**
3. Explore as tabelas criadas:
   - `sales_data`: Dados de vendas individuais
   - `region_aggregates`: Agregações por região
   - `product_aggregates`: Agregações por produto

### Via SQL

Conecte-se usando qualquer cliente PostgreSQL com as credenciais:

```sql
-- Top produtos por receita
SELECT product_name, total_revenue, total_sales 
FROM product_aggregates 
ORDER BY total_revenue DESC 
LIMIT 10;

-- Vendas por região
SELECT region, total_revenue, total_sales 
FROM region_aggregates 
ORDER BY total_revenue DESC;

-- Análise temporal
SELECT sale_month, SUM(sales_amount) as monthly_revenue
FROM sales_data 
GROUP BY sale_month 
ORDER BY sale_month;
```

## 🔧 Conectando Ferramentas de BI

### Mode Analytics

1. No Mode, clique em "Connect a database"
2. Selecione **PostgreSQL**
3. Use as credenciais do Supabase:
   - Host: `db.xxxxxxx.supabase.co`
   - Port: `5432`
   - Database Name: `postgres`
   - Username: `postgres`
   - Password: sua senha
   - SSL: **Yes**

### Power BI / Tableau

Use o conector PostgreSQL com as mesmas credenciais do Supabase.

## 🚀 Próximos Passos

1. **Airflow**: Configure DAGs para automação
2. **APIs Reais**: Integre com APIs externas (ex: APIs de e-commerce)
3. **Dashboard**: Crie dashboards no Mode, Power BI ou Tableau
4. **Monitoramento**: Implemente logs e alertas
5. **CI/CD**: Configure GitHub Actions para deploys automáticos

## 📈 Estrutura dos Dados

### Tabela: sales_data
- `id`: Chave primária
- `product_name`: Nome do produto
- `sales_amount`: Valor da venda
- `sale_date`: Data da venda
- `region`: Região da venda
- `customer_id`: ID do cliente
- `quantity`: Quantidade vendida

### Tabela: region_aggregates
- Agregações de vendas por região
- Total de vendas, receita, quantidade média

### Tabela: product_aggregates
- Agregações de vendas por produto
- Performance de produtos, regiões atendidas

## 🤝 Contribuindo

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/AmazingFeature`)
3. Commit suas mudanças (`git commit -m 'Add some AmazingFeature'`)
4. Push para a branch (`git push origin feature/AmazingFeature`)
5. Abra um Pull Request

## 📝 Licença

Distribuído sob a licença MIT. Veja `LICENSE` para mais informações.
