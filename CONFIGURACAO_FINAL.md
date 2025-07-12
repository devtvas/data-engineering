# 🔧 CONFIGURAÇÃO FINAL - SUAS CREDENCIAIS SUPABASE

## ✅ Credenciais já configuradas no .env:

```env
SUPABASE_HOST=aws-0-sa-east-1.pooler.supabase.com
SUPABASE_PORT=5432
SUPABASE_DBNAME=postgres
SUPABASE_USER=postgres.fumwicepmtoqsxatftip
```

## 🔑 FALTA APENAS:

### 1. Substituir a senha
No arquivo `.env`, substitua `SUBSTITUA_PELA_SUA_SENHA_AQUI` pela senha real do seu projeto Supabase.

### 2. Obter a senha (se esqueceu)
1. Acesse: https://app.supabase.com/
2. Vá no seu projeto
3. **Settings → Database**
4. Clique em **"Reset database password"** se necessário

### 3. Testar a conexão
```bash
# Instalar dependências
pip install -r requirements.txt

# Testar conexão
python3 src/main.py
```

### 4. Executar pipeline completo
```bash
python3 demo.py
```

## 🎯 VANTAGENS DO SESSION POOLER:

✅ **Melhor performance** - Pool de conexões otimizado
✅ **Mais estável** - Recomendado para aplicações externas
✅ **IPv4 compatible** - Funciona em redes IPv4
✅ **Timeout management** - Gerenciamento automático de timeouts

## 📍 SUAS CONFIGURAÇÕES:

- **Região**: South America East (São Paulo)
- **Pooler**: Session Pooler (Recomendado)
- **Host**: `aws-0-sa-east-1.pooler.supabase.com`
- **Project ID**: `fumwicepmtoqsxatftip`

## 🚀 PRÓXIMOS PASSOS:

1. ✅ Credenciais configuradas
2. 🔑 **VOCÊ ESTÁ AQUI** → Adicionar senha no .env
3. 🧪 Testar conexão
4. 📊 Executar pipeline
5. 🎨 Conectar ferramentas de BI

---
**💡 Dica**: Mantenha sua senha segura e nunca a compartilhe no GitHub!
