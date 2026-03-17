import os
import pymysql
import pandas as pd
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# ---------------------------
# Secured Connections
# ---------------------------
conn = pymysql.connect(
    host=os.getenv('DB1_HOST'),
    port=3306,
    user=os.getenv('DB1_USER'),
    password=os.getenv('DB1_PASS'),
    database=os.getenv('DB1_NAME')
)

conn_qchamado = pymysql.connect(
    host=os.getenv('DB1_HOST'),
    port=int(os.getenv('DB2_PORT')), # Convert port to int
    user=os.getenv('DB1_USER'),
    password=os.getenv('DB1_PASS'),
    database=os.getenv('DB2_NAME')
)

# ---------------------------
# Parâmetros: romaneio 35 e período
# ---------------------------
lista_35_especifica = [
'U8615#1767875950484',
'U8915#1769539145727',
'U11860#1767871705695',
'U12169#1768992591949',
'U3884#1768015548763',
'U9969#1768221277522',
'U9969#1768412475148',
'U9969#1768410469310',
'U9969#1768417216373',
'U6135#1768595903570',
'U2557#1768594193021',
'U12174#1769168752872',
'U12173#1769167802028',
'U12173#1769182720491',
'U12173#1769702374607',
'U12173#1769701549659',
'U3884#1769712598916',
'U12173#1769702042989',
'U9476#1769790271254',
'U12173#1769799248495',
'U3884#1769797106838',
'U12173#1769794352241',
'U12174#1769796481263',
'F12358#1768581202045',
'U9744#1768582377475',
'U5551#1767721230370',
'U12124#1769444753821',
'U12078#1767965563915',
'U8432#1769337714640',
'U12006#1767868999886',
'U5783#1769017766812'

]
data_inicio_35 = '2025-12-01'
data_fim_35 = '2026-02-10'

# ---------------------------
# Helper: monta placeholders e parâmetros para IN
# ---------------------------
def build_in_clause_params(values):
    if not values:
        return "", []
    placeholders = ",".join(["%s"] * len(values))
    return placeholders, values

# ---------------------------
# 1. Consulta lista 35 (parametrizada)
# ---------------------------
placeholders_35, params_listas = build_in_clause_params(lista_35_especifica)

query_35 = f"""
SELECT DISTINCT
    ql.lista as lista_35,
    ql.validada,
    ql.origem as cdd_origem,
    ped.id as id_pedido,
    ql.dh_entrada as dh_inicio_35,
    ql.dh_validacao as dh_fim_35,
    ql.usuario as usuario_35,
    ql.lat AS lat_emis_rom,
    ql.lon AS lon_emis_rom,
    ql.lat_validacao,
    ql.lon_validacao 
FROM qrota_listas ql
JOIN qrota_listas_volumes qlv ON ql.lista = qlv.lista
JOIN volumes vol ON qlv.etiqueta = vol.etiqueta
JOIN volumespedidos vp ON vol.id = vp.idvolume
JOIN pedidos ped ON vp.idpedido = ped.id
WHERE ql.atividade = '35'
    AND qlv.bipado = '1'
    AND ql.validada = '1'
    AND ped.tipo NOT IN ('nfduplicada')
    AND ped.status NOT IN ('cancelado', 'nadacoletado')
    AND ped.idregional != '44'
    AND ql.lista IN ({placeholders_35})
"""

params_35 = params_listas 
df_35 = pd.read_sql(query_35, conn, params=params_35)

# Limpa e converte datas
df_35 = df_35[df_35['id_pedido'].notna()].copy()
df_35['dh_inicio_35'] = pd.to_datetime(df_35['dh_inicio_35'], errors='coerce')
df_35['dh_fim_35'] = pd.to_datetime(df_35['dh_fim_35'], errors='coerce')

if df_35.empty:
    print(f"Nenhum dado para lista_35 {lista_35_especifica} no período. Encerrando.")
    conn.close()
    conn_qchamado.close()
    exit(0)

# ---------------------------
# 2. Pedidos únicos da lista 35
# ---------------------------
ids_35 = df_35['id_pedido'].dropna().astype(str).unique().tolist()
placeholders_pedidos, params_pedidos = build_in_clause_params(ids_35)

# ---------------------------
# 3. Consulta romaneios tipo 2 e 25
# ---------------------------
base_cols = ['romaneio', 'id_pedido', 'dh_entrada', 'usuario', 'lat', 'lon', 'tipo', 'origem']
df_tipo2 = pd.DataFrame(columns=base_cols)
df_tipo25 = pd.DataFrame(columns=base_cols)

if params_pedidos:
    query_tipo2 = f"""
    SELECT DISTINCT
        ql.lista as romaneio,
        ped.id as id_pedido,
        ql.dh_entrada as dh_entrada,
        ql.usuario,
        ql.lat,
        ql.lon,
        '2' as tipo
    FROM qrota_listas ql
    JOIN qrota_listas_volumes qlv ON ql.lista=qlv.lista
    JOIN volumes vol ON qlv.etiqueta=vol.etiqueta
    JOIN volumespedidos vp ON vol.id=vp.idvolume
    JOIN pedidos ped ON vp.idpedido=ped.id
    WHERE ql.atividade='2'
        AND qlv.bipado='1'
        AND ped.tipo NOT IN ('nfduplicada')
        AND ped.status NOT IN ('cancelado','nadacoletado')
        AND ped.idregional !='44'
        AND ped.id IN ({placeholders_pedidos})
        AND ped.dataconfirmacao <> '0000-00-00 00:00:00'
      
    """
    df_tipo2 = pd.read_sql(query_tipo2, conn, params=params_pedidos)
    if not df_tipo2.empty:
        df_tipo2['origem'] = 'entrega'
        df_tipo2['tipo'] = '2'

    query_tipo25 = f"""
    SELECT DISTINCT
        ql.lista as romaneio,
        ped.id as id_pedido,
        ql.usuario,
        ql.dh_entrada as dh_entrada,
        ql.lat AS lat,
        ql.lon AS lon,
        '25' as tipo
    FROM qrota_listas ql
    JOIN qrota_listas_volumes qlv ON ql.lista=qlv.lista
    JOIN volumes vol ON qlv.etiqueta=vol.etiqueta
    JOIN volumespedidos vp ON vol.id=vp.idvolume
    JOIN pedidos ped ON vp.idpedido=ped.id
    WHERE ql.atividade='25'
        AND qlv.bipado='1'
        AND ped.tipo NOT IN ('nfduplicada')
        AND ped.status NOT IN ('cancelado','nadacoletado')
        AND ped.idregional !='44'
        AND ped.id IN ({placeholders_pedidos})
    """
    df_tipo25 = pd.read_sql(query_tipo25, conn, params=params_pedidos)
    if not df_tipo25.empty:
        df_tipo25['origem'] = 'ocorrencia'
        df_tipo25['tipo'] = '25'

# Padroniza colunas mínimas para garantir consistência
for df in (df_tipo2, df_tipo25):
    for col in base_cols:
        if col not in df.columns:
            df[col] = np.nan

# ---------------------------
# 4. Junta e trata rejeições
# ---------------------------
# Concatena tipo2 + tipo25 (pode estar vazio)
df_final = pd.concat([df_tipo2[base_cols], df_tipo25[base_cols]], ignore_index=True)

# Tratamento de rejeição para tipo 25
if not df_tipo25.empty and df_tipo25['id_pedido'].dropna().any():
    ids_25 = df_tipo25['id_pedido'].dropna().astype(str).unique().tolist()
    placeholders_25, params_25 = build_in_clause_params(ids_25)
    query_qchamado = f"""
    SELECT 
        IDPedido,
        DATE(DataHoraCadastro) AS data_cadastro,
        Rejeitado
    FROM QeJS_db.CategoriasListaOCC
    WHERE YEAR(DataHoraCadastro) between  '2025' and '2026'
        AND IDPedido IN ({placeholders_25})
    """
    df_chamado = pd.read_sql(query_qchamado, conn_qchamado, params=params_25)

    df_tipo25['data_dh_entrada'] = pd.to_datetime(df_tipo25['dh_entrada']).dt.date
    df_chamado['data_cadastro'] = pd.to_datetime(df_chamado['data_cadastro']).dt.date

    df_25_merge = pd.merge(
        df_tipo25,
        df_chamado,
        left_on=['id_pedido', 'data_dh_entrada'],
        right_on=['IDPedido', 'data_cadastro'],
        how='left'
    )
    df_tipo2_copy = df_tipo2.copy()
    df_tipo2_copy['Rejeitado'] = None
    cols_to_take = base_cols + ['Rejeitado']
    df_final = pd.concat(
        [df_tipo2_copy[cols_to_take], df_25_merge[[*base_cols, 'Rejeitado']]],
        ignore_index=True
    )
    df_final = df_final[df_final['Rejeitado'] != 1]
else:
    # mantém tipo2 mesmo vazio
    df_final = df_tipo2.copy()
    df_final['origem'] = df_final.get('origem', 'entrega')
    df_final['Rejeitado'] = None

# ---------------------------
# 5. Associação com lista 35 (robusta)
# ---------------------------
df_final['dh_entrada'] = pd.to_datetime(df_final['dh_entrada'], errors='coerce')

# Merge por id_pedido para trazer potenciais correspondências de lista_35
df_merged = pd.merge(
    df_final.reset_index(drop=True),
    df_35[['lista_35', 'usuario_35', 'id_pedido', 'dh_inicio_35', 'dh_fim_35']],
    on='id_pedido',
    how='left'
)

# Filtra pelo intervalo de validade da lista_35
mask_intervalo = (
    (df_merged['dh_entrada'] >= df_merged['dh_inicio_35']) &
    (df_merged['dh_entrada'] <= df_merged['dh_fim_35'])
)
df_final = df_merged[mask_intervalo].copy()

if df_final.empty:
    print("Nenhuma linha casou com lista_35 no intervalo. Saindo sem gerar arquivo.")
    conn.close()
    conn_qchamado.close()
    exit(0)

# Merge único para trazer validada e cdd_origem
extra = df_35[['lista_35', 'validada', 'cdd_origem']].drop_duplicates(subset=['lista_35'])
df_final = pd.merge(
    df_final,
    extra,
    on='lista_35',
    how='left'
)
df_final['data_lista35'] = pd.to_datetime(df_final['dh_inicio_35'], errors='coerce').dt.date
df_final = df_final.drop(columns=['dh_inicio_35', 'dh_fim_35'], errors='ignore')

# ---------------------------
# 6. Cria início_35 e fim_35
# ---------------------------
df_inicio = df_35[['lista_35', 'dh_inicio_35', 'usuario_35', 'lat_emis_rom', 'lon_emis_rom']].drop_duplicates(subset=['lista_35']).copy()
df_inicio['romaneio'] = df_inicio['lista_35']
df_inicio['id_pedido'] = np.nan
df_inicio['tipo'] = 'inicio_35'
df_inicio['origem'] = 'inicio'
df_inicio['usuario'] = df_inicio['usuario_35']
df_inicio.rename(columns={
    'dh_inicio_35': 'dh_entrada',
    'lat_emis_rom': 'lat',
    'lon_emis_rom': 'lon'
}, inplace=True)

df_fim = df_35[['lista_35', 'dh_fim_35', 'usuario_35', 'lat_validacao', 'lon_validacao']].drop_duplicates(subset=['lista_35']).copy()
df_fim['romaneio'] = df_fim['lista_35']
df_fim['id_pedido'] = np.nan
df_fim['tipo'] = 'fim_35'
df_fim['origem'] = 'validacao'
df_fim['usuario'] = df_fim['usuario_35']
df_fim.rename(columns={
    'dh_fim_35': 'dh_entrada',
    'lat_validacao': 'lat',
    'lon_validacao': 'lon'
}, inplace=True)

df_final = pd.concat([df_final, df_inicio, df_fim], ignore_index=True)

# ---------------------------
# 7. Remove listas 35 sem pedidos válidos
# ---------------------------
mask_validos = df_final['tipo'].isin(['2', '25'])
if 'lista_35' in df_final:
    mask_validos &= df_final['lista_35'].notna()
contagem_por_lista = (
    df_final[mask_validos]
    .groupby('lista_35')['id_pedido']
    .count()
    .reset_index()
    .rename(columns={'id_pedido': 'qtde_pedidos'})
)
todas_listas_35 = df_final['lista_35'].dropna().unique()
listas_com_pedidos = contagem_por_lista['lista_35'].unique()
listas_sem_pedidos = list(set(todas_listas_35) - set(listas_com_pedidos))
df_final = df_final[~df_final['lista_35'].isin(listas_sem_pedidos)]

# ---------------------------
# 8. Consulta CDD e monta retorno_cdd
# ---------------------------
ids_pedidos = df_final['id_pedido'].dropna().astype(str).unique().tolist()
placeholders_ped, params_ped = build_in_clause_params(ids_pedidos)
if params_ped:
    query_cdd = f"""
    SELECT DISTINCT
        ped.id AS id_pedido, 
        c.latitude AS lat_unidade,
        c.longitude AS lon_unidade
    FROM pedidos ped 
    JOIN centrosdedistribuicao c ON ped.idregional = c.id
    WHERE ped.id IN ({placeholders_ped})
    """
    df_cdd = pd.read_sql(query_cdd, conn, params=params_ped)
    df_final = pd.merge(df_final, df_cdd, on='id_pedido', how='left')
else:
    df_final['lat_unidade'] = np.nan
    df_final['lon_unidade'] = np.nan

# Retorno CDD
df_retorno = (
    df_final[df_final['tipo'].isin(['2', '25']) & df_final['lista_35'].notna()]
    .drop_duplicates(subset='lista_35')
    [['lista_35', 'usuario_35', 'lat_unidade', 'lon_unidade', 'romaneio']]
    .dropna(subset=['lat_unidade', 'lon_unidade'])
    .copy()
)
df_retorno['tipo'] = 'retorno_cdd'
df_retorno['origem'] = 'retorno_cdd'
df_retorno['dh_entrada'] = None
df_retorno['id_pedido'] = None
df_retorno['usuario'] = df_retorno['usuario_35']
df_retorno.rename(columns={'lat_unidade': 'lat', 'lon_unidade': 'lon'}, inplace=True)

colunas_padrao = df_final.columns.tolist()
colunas_comuns = [col for col in colunas_padrao if col in df_retorno.columns]
df_retorno = df_retorno[colunas_comuns]
df_final = pd.concat([df_final, df_retorno], ignore_index=True)

# ---------------------------
# 9. Ordenação, máscara e limpeza final
# ---------------------------
df_final['dh_entrada'] = pd.to_datetime(df_final['dh_entrada'], errors='coerce')
df_final['ordem'] = df_final['dh_entrada']
df_final.loc[df_final['tipo'] == 'retorno_cdd', 'ordem'] = pd.to_datetime('2100-01-01')

# Máscara de usuário
mascara_tipo_usuario = ~(
    (df_final['tipo'].isin(['2', '25'])) & (df_final['usuario'] != df_final.get('usuario_35'))
)
df_final = df_final[mascara_tipo_usuario].copy()

# Remove colunas temporárias se existirem
df_final = df_final.drop(columns=['lat_unidade', 'lon_unidade'], errors='ignore')

# Reordena colunas finais
ordem_colunas = [
    'data_lista35',
    'cdd_origem',
    'lista_35',
    'usuario_35',
    'validada',
    'romaneio',
    'id_pedido',
    'dh_entrada',
    'usuario',
    'lat',
    'lon',
    'tipo',
    'origem',
    'Rejeitado',
    'ordem'
]
colunas_existentes = [c for c in ordem_colunas if c in df_final.columns]
df_final = df_final[colunas_existentes]
df_final = df_final.sort_values(by=['lista_35', 'ordem'], ascending=[True, True]).drop(columns='ordem', errors='ignore')

# ---------------------------
# 10. Salva arquivo com nome legível
# ---------------------------
hoje = datetime.now().date()
data_inicial = hoje - timedelta(days=3)
data_final = hoje - timedelta(days=1)
dia_inicio = data_inicial.strftime('%d')
dia_fim = data_final.strftime('%d')
nome_base = "_".join(lista_35_especifica)
nome_arquivo = f"Roman_romaneios_zerados.xlsx"

df_final.to_excel(r'C:/Users/Paulo Fanti/Documents/Arquivos/teste.xlsx')
print(f"Arquivo salvo como: {nome_arquivo}")

# ---------------------------
# 11. Fecha conexões
# ---------------------------
conn.close()
conn_qchamado.close()
