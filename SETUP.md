# 🚀 SETUP RÁPIDO - DATA ENGINEERING PIPELINE

## ✅ Pré-requisitos
- Python 3.8+
- Conta no Supabase (gratuita)

## 🔧 Configuração (5 minutos)

### 1. Instalar dependências
```bash
pip install -r requirements.txt
```

### 2. Configurar Supabase

#### 2.1. Criar projeto no Supabase
1. Acesse: https://app.supabase.com/
2. Clique em "Sign in with GitHub"
3. Clique em "New Project"
4. Preencha:
   - **Project name**: `data-engineering-pipeline`
   - **Database password**: Use uma senha forte (ex: `MinhaSenh@2025!`)
   - **Region**: Mantenha a sugerida
5. Aguarde 1-2 minutos para o projeto ser criado

#### 2.2. Obter credenciais
1. No seu projeto, vá em **Settings → Database**
2. Copie as informações da seção "Connection info"

### 3. Configurar variáveis de ambiente
```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar com suas credenciais
nano .env  # ou use seu editor preferido
```

Edite o arquivo `.env`:
```env
SUPABASE_HOST=db.xxxxxxx.supabase.co
SUPABASE_PORT=5432
SUPABASE_DBNAME=postgres
SUPABASE_USER=postgres
SUPABASE_PASSWORD=sua_senha_aqui
```

## 🎯 Executar Demo

### Opção 1: Demo Completa
```bash
python demo.py
```

### Opção 2: Pipeline ETL
```bash
python src/etl_pipeline.py
```

### Opção 3: Teste Básico
```bash
python src/main.py
```

## 📊 Visualizar Dados

### No Supabase
1. Acesse seu projeto no Supabase
2. Vá para **Table Editor**
3. Explore as tabelas:
   - `sales_data`
   - `region_aggregates` 
   - `product_aggregates`

### Consultas SQL
Execute as consultas em `sql/consultas_analiticas.sql` no editor SQL do Supabase.

## 🔗 Conectar BI Tools

### Mode Analytics
1. No Mode: "Connect a database" → PostgreSQL
2. Use as credenciais do Supabase
3. SSL: **Yes**

### Power BI / Tableau
Use o conector PostgreSQL com as credenciais do Supabase.

## 🆘 Solução de Problemas

### Erro de conexão
- Verifique se as credenciais no `.env` estão corretas
- Confirme se o projeto Supabase está ativo
- Teste a conexão: `python src/main.py`

### Erro de importação
- Confirme que instalou as dependências: `pip install -r requirements.txt`
- Verifique se está executando do diretório raiz do projeto

### Problemas com SSL
- No Supabase, vá em **Settings → Database**
- Na seção "Connection pooling", ative SSL se necessário

## 📞 Suporte
- Documentação Supabase: https://supabase.com/docs
- Issues: Abra uma issue no repositório GitHub
