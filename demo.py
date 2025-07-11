#!/usr/bin/env python3
"""
Script de demonstração completo do pipeline de dados.
Execute este script após configurar suas credenciais do Supabase.
"""
import sys
import os
import logging

# Adicionar o diretório src ao path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Imports after path setup
from src.database.connection import get_db_connection
from src.etl_pipeline import run_etl_pipeline


def check_environment():
    """Verifica se o ambiente está configurado corretamente."""
    logger.info("Verificando configuração do ambiente...")
    
    try:
        db = get_db_connection()
        if db.test_connection():
            logger.info("✅ Conexão com Supabase estabelecida com sucesso!")
            return True
        else:
            logger.error("❌ Falha na conexão com Supabase")
            return False
    except Exception as e:
        logger.error(f"❌ Erro na verificação do ambiente: {e}")
        return False


def main():
    """Função principal de demonstração."""
    print("=" * 60)
    print("🚀 DATA ENGINEERING PIPELINE - DEMONSTRAÇÃO")
    print("=" * 60)
    
    # Verificar ambiente
    if not check_environment():
        print("\n❌ Falha na verificação do ambiente!")
        print("Verifique se:")
        print("1. Você criou um arquivo .env com suas credenciais do Supabase")
        print("2. As credenciais estão corretas")
        print("3. O Supabase está acessível")
        return
    
    # Executar pipeline ETL
    try:
        print("\n🔄 Executando Pipeline ETL Completo...")
        summary = run_etl_pipeline()
        
        print("\n" + "=" * 60)
        print("✅ PIPELINE EXECUTADO COM SUCESSO!")
        print("=" * 60)
        print(f"📊 Registros de vendas: {summary['sales_records']}")
        print(f"🌍 Regiões: {summary['regions']}")
        print(f"📦 Produtos: {summary['products']}")
        print(f"💰 Receita total: ${summary['total_revenue']:.2f}")
        
        print("\n🎯 PRÓXIMOS PASSOS:")
        print("1. Acesse seu projeto no Supabase")
        print("2. Vá para 'Table Editor' para visualizar os dados")
        print("3. Execute as consultas em sql/consultas_analiticas.sql")
        print("4. Conecte ferramentas de BI como Mode, Power BI ou Tableau")
        
        print("\n📋 TABELAS CRIADAS:")
        print("- sales_data: Dados detalhados de vendas")
        print("- region_aggregates: Agregações por região")
        print("- product_aggregates: Agregações por produto")
        
        print("\n📈 CONSULTAS SUGERIDAS:")
        print("```sql")
        print("-- Top produtos por receita")
        print("SELECT product_name, total_revenue, total_sales")
        print("FROM product_aggregates")
        print("ORDER BY total_revenue DESC LIMIT 10;")
        print("")
        print("-- Performance por região")
        print("SELECT region, total_revenue, total_sales")
        print("FROM region_aggregates")
        print("ORDER BY total_revenue DESC;")
        print("```")
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução do pipeline: {e}")
        logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()
