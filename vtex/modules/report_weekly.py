from modules.dbpgconn import *

from modules.save_to_blob import *

import subprocess
import sys

import os
import datetime

# Função para instalar um pacote via pip
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

# Instalar matplotlib se não estiver instalado
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("matplotlib")
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches

# Instalar matplotlib se não estiver instalado
try:
    from fpdf import FPDF
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("fpdf2")
    from fpdf import FPDF


# import pandas as pd

import uuid
import shutil

# Instalar matplotlib se não estiver instalado
try:
    import numpy as np
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("numpy")
    import numpy as np

# Instalar matplotlib se não estiver instalado
try:
    import geopandas as gpd
except ImportError:
    print("matplotlib não está instalado. Instalando agora...")
    install("geopandas")
    import geopandas as gpd

data_conection_info = None
idintegration = None
celular = None
semana = None
logo = None



def formatar_numerocasas_br(valor,casas,ismoeda):
    valor_arred = round(valor,casas)
    if(ismoeda == 1):
        valor_str = f"R$ {valor_arred:,.{casas}f}"
    else:    
        valor_str = f"{valor_arred:,.{casas}f}"

    valor_str = valor_str.replace('.', 'x')
    valor_str = valor_str.replace(',', '.')
    valor_str = valor_str.replace('x', ',')
    return valor_str


def abreviar_valor_br(valor,ismoeda,tipoabreviacao):
        if tipoabreviacao == 3:
            return f'{formatar_numerocasas_br(valor/1000000000,1,ismoeda)}B'
        elif tipoabreviacao == 2:
            return f'{formatar_numerocasas_br(valor/1000000,1,ismoeda)}M'
        elif tipoabreviacao == 1:
            return f'{formatar_numerocasas_br(valor/1000,1,ismoeda)}K'
        return f'{formatar_numerocasas_br(valor,0,ismoeda)}'


def abreviar_eixoy_moeda(valor,casas,ismoeda,tipoabreviacao):
        if tipoabreviacao == 3:
            return f'{formatar_numerocasas_br(valor/1000000000,casas,ismoeda)}B'
        elif tipoabreviacao == 2:
            return f'{formatar_numerocasas_br(valor/1000000,casas,ismoeda)}M'
        elif tipoabreviacao == 1:
            return f'{formatar_numerocasas_br(valor/1000,casas,ismoeda)}K'
        return f'{formatar_numerocasas_br(valor,casas,ismoeda)}'


def quebrarlinha(text, qtdletra, maxletraquebra):
    if len(text) <= maxletraquebra :
        return text+'  '  # Se o texto for menor ou igual ao dobro da quantidade, retorna o texto todo
    else:
        # Pega os primeiros 'qtdletra' caracteres e evita cortar no meio da palavra
        if text[qtdletra] != ' ':
            space_index = text.rfind(' ', 0, qtdletra)  # Encontra o último espaço antes do limite
            if space_index != -1:
                qtdletra=space_index
                inicio = text[:qtdletra]
            else:
                inicio = text[:qtdletra]   # Se não houver espaço, corta normalmente
        else:
            inicio = text[:qtdletra]
        
         
        # Pega os últimos 'qtdletra' caracteres sem cortar palavras no meio
        qtdresto=len(text) -qtdletra
        if( qtdresto >maxletraquebra):
            qtdresto= maxletraquebra

        fim = text[-qtdresto:]

        return inicio + ' ...  ' + '\n' + fim + '  '

def grafico_card(nm_imagem,nm_card,vlatual_card,vlanterior_card,porc_card):

    # Criar uma figura e uma subplot
    fig, ax = plt.subplots(figsize=(5, 2))  # Tamanho da figura
 

    # Adicionar um retângulo com bordas arredondadas
    bbox = patches.FancyBboxPatch((0.05, 0.05), 0.6, 0.5, boxstyle="round,pad=0.02,rounding_size=0.1", color='white', ec="lightgrey",lw=0.3)
    ax.add_patch(bbox)

    # # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/OpenSans-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()
    
    
    

    # (len(vlatual_card)/100)*2.5
    #print(porc_card)
    altura = 0.42 
    horizontal = 0.105
    # Adicionar texto ao cartão
    ax.text(0.09, altura, nm_card, fontsize=20, color='#676270')
    font_valor_atual = {'fontsize': 22, 'color': '#15131B', 'fontweight':'bold'}
    text_valor_atual=ax.text(horizontal, altura -0.14,  vlatual_card , fontdict=font_valor_atual)

    
    # Essa parte serve para calcular DPI para colocar o % sempre a frente do numero principal
    fig.canvas.draw()
    # Calcular a largura do texto 'valor_atual' em pixels
    bbox_valor_atual = text_valor_atual.get_window_extent(renderer=fig.canvas.get_renderer())
    largura_valor_atual_px = bbox_valor_atual.width
    # Convertendo pixels para uma unidade de coordenadas relativa, ajustando com base na largura total
    fig_width_in_pixels = fig.bbox.width  # Largura total da figura em pixels
    largura_valor_atual_coord = (largura_valor_atual_px / fig_width_in_pixels)*0.9 # Ajuste para coordenadas relativas

    #fazer o card de % e a cor
    if(porc_card[0]== '-'):
        ax.text(horizontal+largura_valor_atual_coord + 0.01 , altura - 0.14, '↘ '+ porc_card.replace('-','') + '%' , fontsize=18, color='#B2003B',bbox=dict(boxstyle="round,pad=0.3", facecolor='#FFEAF3', edgecolor='#FFEAF3'))
    else:
        ax.text(horizontal+largura_valor_atual_coord + 0.01 , altura - 0.14, '↗ '+ porc_card + '%' , fontsize=18, color='#0D9446',bbox=dict(boxstyle="round,pad=0.3", facecolor='#E2FCED', edgecolor='#E2FCED'))

    #colocar a informação do mes anterior     
    ax.text(horizontal, altura -0.28, vlanterior_card + ' na semana anterior', fontsize=12, color='#938F9A')

    # Esconder os eixos
    # ax.set_xlim([0.026, 0.68])
    # ax.set_ylim([0.021, 0.58])
    ax.set_xlim([0.026, 0.68])
    ax.set_ylim([0.021, 0.58])
    
    ax.axis('off')

    

    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=100,bbox_inches='tight', pad_inches=0)
    plt.close()
    #plt.show()


def grafico_linha(nm_imagem,titulografico,datas,valores,abreviacao):

    
    # Convertendo os valores e datas para plotagem
    dates = np.arange(len(datas))
    # Obtendo o dia da semana em inglês
    dias_da_semana_eng = datas.dt.day_name()

    # Mapeando para português
    dias_da_semana_pt = dias_da_semana_eng.map({
    'Monday': 'Seg',
    'Tuesday': 'Ter',
    'Wednesday': 'Qua',
    'Thursday': 'Qui',
    'Friday': 'Sex',
    'Saturday': 'Sab',
    'Sunday': 'Dom'
    })
    
   
    dias = datas.dt.strftime('%d')+'('+ dias_da_semana_pt +')'

  

    # Criando a figura e o eixo
    fig, ax = plt.subplots(figsize=(8, 6))  # Tamanho definido para a figura
    # Plotando os dados
    ax.plot(dates, valores, marker='o', linestyle='-', color='#006DCC')

    # # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/OpenSans-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()



    # Adicionando valores em cada ponto
    for i, txt in enumerate(valores):
        ax.annotate(f'{abreviar_valor_br(valores[i],0,abreviacao)}', (dates[i], valores[i]), textcoords="offset points", xytext=(0,10), ha='center',fontsize=12,color ='white',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='#006DCC', edgecolor='#006DCC')
                        ) 

        # Configurações dos ticks
    ax.set_xticks(dates)
    ax.set_xticklabels(dias,color ='#676270',fontsize=12 )
    plt.yticks(color='#676270', fontsize=12)
    ticks = ax.get_yticks()
    ax.set_yticks(ticks)  # Definir explicitamente os ticks do eixo Y
    valores_ajustado = [ abreviar_eixoy_moeda(x,0,0,abreviacao) for x in ticks]
    ax.set_yticklabels(valores_ajustado)
   

    # Removendo os traços dos ticks dos eixos
    ax.tick_params(axis='x', length=0)  # Remove os ticks do eixo X
    ax.tick_params(axis='y', length=0)  # Remove os ticks do eixo Y
    
    # Removendo as cores das bordas dos eixos x e y
    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    ax.grid(True, linestyle='-', which='both', color='#676270', alpha=0.05)
        # Ajustar os limites dos eixos
    min_faturamento = min(valores)
    max_faturamento = max(valores)
    ax.set_ylim(min_faturamento * 0.85, max_faturamento * 1.15) 


    # Ajustes finais
    fig.suptitle(titulografico, fontsize=18, fontweight='bold', x=0.5, ha='center',color="#15131B", y=0.92 )


    fig.subplots_adjust(top=0.8,left=0.12,right=0.95  )

    fancy_box = patches.FancyBboxPatch((0.03, 0.03), width=0.94, height=0.94,
                      boxstyle="round,pad=0.02,rounding_size=0.02", ec="lightgrey", lw=0.3, facecolor="none",
                      transform=fig.transFigure, clip_on=False)

    fig.patches.append(fancy_box)
    # Exibindo o gráfico
   

    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=300,bbox_inches='tight', pad_inches=0)
    plt.close()
   # plt.show()



def grafico_horizontal(nm_imagem,titulografico,texto,valores,porc,abreviacao,istipocor):

    if istipocor == 1:
        cor='#006DCC'
    elif istipocor == 2:
        cor='#6f359f'
    else:
        cor='#1a494f'
        

    # Criação do gráfico
    fig, ax = plt.subplots(figsize=(8, 6))

    textoajustado = [quebrarlinha(label, 27,32) for label in texto]
   
    ax.barh(textoajustado, valores,color=cor,capstyle='round' )

    # # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/OpenSans-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()


    
    #   # Adicionando valores em cada ponto
    for i, txt in enumerate(valores):

        #textovalor=f'  {abreviar_valor_br(txt,0,abreviacao)}'
        text_valor_atual=ax.annotate(f'  {abreviar_valor_br(txt,0,abreviacao)}',xy=(txt+5, i),color='#15131B', va='center',fontsize=14)
        
                # Essa parte serve para calcular DPI para colocar o % sempre a frente do numero principal
        fig.canvas.draw()
        # Calcular a largura do texto 'valor_atual' em pixels
        bbox_valor_atual = text_valor_atual.get_window_extent(renderer=fig.canvas.get_renderer())
        largura_valor_atual_px = bbox_valor_atual.width
        #print(largura_valor_atual_px)
        # Convertendo pixels para uma unidade de coordenadas relativa, ajustando com base na largura total
        #fig_width_in_pixels = fig.bbox.width  # Largura total da figura em pixels
        #largura_valor_atual_coord = (largura_valor_atual_px / fig_width_in_pixels)*0.9 # Ajuste para coordenadas relativas
        #print(fig_width_in_pixels)

        if(porc.iloc[i]<0):
            porc_card = '↘ '+ formatar_numerocasas_br(porc.iloc[i],1,0).replace('-','') + '%'  
            ax.annotate(f'{porc_card}', (txt, i),xytext=(largura_valor_atual_px, 0),textcoords="offset points",ha='left',color='#B2003B', va='center',fontsize=12,bbox=dict(boxstyle="round,pad=0.3", facecolor='#FFEAF3', edgecolor='#FFEAF3'))
        else:
            porc_card = '↗ '+ formatar_numerocasas_br(porc.iloc[i],1,0).replace('-','') + '%' 
            ax.annotate(f'{porc_card}', (txt , i),xytext=(largura_valor_atual_px, 0),textcoords="offset points",ha='left',color='#0D9446', va='center',fontsize=12,bbox=dict(boxstyle="round,pad=0.3", facecolor='#e2fced', edgecolor='#e2fced'))
  
    
    #ax.set_xticklabels(color ='gray',fontsize=8 )
    plt.yticks(color='#676270', fontsize=12)
    plt.xticks(color='white', fontsize=8)

    # Removendo os traços dos ticks dos eixos
    ax.tick_params(axis='x', length=0)  # Remove os ticks do eixo X
    ax.tick_params(axis='y', length=0)  # Remove os ticks do eixo Y

    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')

    #title=plt.title(titulografico, fontsize=18, fontweight='bold', loc='left', pad=40, color= '#15131B')
    
    fig.suptitle(titulografico, fontsize=18, fontweight='bold', x=0.5, ha='center',color="#15131B", y=0.92 )


    fancy_box = patches.FancyBboxPatch((0.03, 0.03), width=0.94, height=0.94,
                      boxstyle="round,pad=0.02,rounding_size=0.02", ec="lightgrey", lw=0.3, facecolor="none",
                      transform=fig.transFigure, clip_on=False)

    fig.patches.append(fancy_box)
   

    istextolongo = [1 if len(label2) >= 20 else 0 for label2 in texto]
    # Exibindo o gráfico
    if(sum(istextolongo) > 0):
      #  title.set_position((-0.80, 1)) 
        fig.subplots_adjust(top=0.8,left=0.45,right= 0.70,bottom=0)
       
    else:
       # title.set_position((-0.35, 1)) 
        fig.subplots_adjust(top=0.8,left=0.35,right= 0.70,bottom=0)

    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=300)#,bbox_inches='tight', pad_inches=0
    #plt.show()
    plt.close()




def grafico_mapa(diretorio,nm_imagem,titulografico,dataframe,abreviacao):


    # # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/OpenSans-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()

    # Carregar os dados geográficos
    try:
        ExecuteBlob().get_file("appgemdata","arquivos-sistemicos/BR_UF_2022.shp",f"{diretorio}/BR_UF_2022.shp") 
        ExecuteBlob().get_file("appgemdata","arquivos-sistemicos/BR_UF_2022.shx",f"{diretorio}/BR_UF_2022.shx") 
        ExecuteBlob().get_file("appgemdata","arquivos-sistemicos/BR_UF_2022.cpg",f"{diretorio}/BR_UF_2022.cpg") 
        ExecuteBlob().get_file("appgemdata","arquivos-sistemicos/BR_UF_2022.dbf",f"{diretorio}/BR_UF_2022.dbf") 
        ExecuteBlob().get_file("appgemdata","arquivos-sistemicos/BR_UF_2022.prj",f"{diretorio}/BR_UF_2022.prj") 
    except Exception as e:
        print(f"Erro ao puxar o mapa{e}")
          
    # Carregar os dados geográficos
    mapa_br = gpd.read_file(f"{diretorio}/BR_UF_2022.shp")

 
    # Combinar os dados geográficos com os dados de faturamento
    mapa_br = mapa_br.set_index('SIGLA_UF').join(dataframe.set_index('selectedaddresses_0_state'))
    mapa_br['faturamento']=mapa_br['faturamento'].fillna(0)
    

    # Ordena o DataFrame pelo faturamento em ordem decrescente
    mapa_br.sort_values('faturamento', ascending=False, inplace=True)

    # Mantém os 5 primeiros valores como estão e zera os demais
    mapa_br.loc[mapa_br.index[5:], 'faturamento'] = 0

    mapa_br_top = mapa_br.nlargest(5, 'faturamento')

    # Criar o mapa
    fig, ax = plt.subplots(figsize=(8, 6))
    
    mapa_br.plot(column='faturamento', ax=ax, legend=False,cmap='Blues',vmin=0, vmax=mapa_br['faturamento'].max()*1.1, edgecolor='silver', linewidth=0.3)
    ax.set_aspect('equal')
    # Adicionar rótulos de faturamento em cada estado
    for idx, row in mapa_br_top.iterrows():
        plt.annotate(text=f"{abreviar_valor_br(row['faturamento'],0,abreviacao)}", xy=(row['geometry'].centroid.x, row['geometry'].centroid.y),
                 horizontalalignment='center', color='black',fontsize=8)


    # Criar a barra de cores
    sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=0, vmax=mapa_br['faturamento'].max()*1.1))
    sm._A = []  # Array vazio para o ScalarMappable
    plt.colorbar(sm, ax=ax, orientation='horizontal', pad=0.05)

    # Remover rótulos e marcas dos eixos
    ax.set_xlabel('')
    ax.set_ylabel('')
    ax.set_xticks([])
    ax.set_yticks([])


    # Removendo os traços dos ticks dos eixos
    ax.tick_params(axis='x', length=0)  # Remove os ticks do eixo X
    ax.tick_params(axis='y', length=0)  # Remove os ticks do eixo Y


    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    ax.set_axis_off()

    #title=plt.title(titulografico, fontsize=18, fontweight='bold', loc='left', pad=40,color= '#15131B')
    fig.suptitle(titulografico, fontsize=18, fontweight='bold', x=0.5, ha='center',color="#15131B", y=0.92 )


    fancy_box = patches.FancyBboxPatch((0.03, 0.03), width=0.94, height=0.94,
                      boxstyle="round,pad=0.02,rounding_size=0.02", ec="lightgrey", lw=0.3, facecolor="none",
                      transform=fig.transFigure, clip_on=False)

    fig.patches.append(fancy_box)
   
    # Exibindo o gráfico
    fig.subplots_adjust(top=0.8 )
    # title.set_position((-0.35, 1)) 
    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=300,bbox_inches='tight', pad_inches=0)
    #plt.show()
    plt.close()




def criar_relatorio_semanal (celular,integration,data_inicio,data_fim,data_inicio_ant,data_fim_ant,diretorio): 


    consulta_order=f"""
        select 
        'atual' as comparacao,
        date_trunc('day',oi.creationdate) as dataprincipal ,
        paymentnames,
        userprofileid,
        sum(revenue) as Faturamento,
        sum(quantityorder) as Pedidos

        from "{integration}".orders_ia oi 
        where 
        date_trunc('day',oi.creationdate)  >= '{str(data_inicio)}'
        and 
        date_trunc('day',oi.creationdate) <=  '{str(data_fim)}'
         group by 
        date_trunc('day',oi.creationdate)  ,
        oi.paymentnames,
         userprofileid

        union all 

        select 
          'anterior' as comparacao,
        date_trunc('day',oi.creationdate) as dataprincipal ,
        paymentnames,
        userprofileid,
        cast(sum(revenue) as float) as Faturamento,
        cast(sum(quantityorder)  as float) as Pedidos
        from "{integration}".orders_ia oi
        where 
        date_trunc('day',oi.creationdate)  >=  '{str(data_inicio_ant)}'
        and 
        date_trunc('day',oi.creationdate) <=  '{str(data_fim_ant)}'
         group by 
        date_trunc('day',oi.creationdate)  ,
        oi.paymentnames,
        userprofileid """

    df=WriteJsonToPostgres(data_conection_info,consulta_order).query_dataframe()

    
   # print(df.info())
    dfatual = df[(df['dataprincipal'] >= data_inicio) & (df['dataprincipal'] <= data_fim)]
    dfanterior = df[(df['dataprincipal'] >= data_inicio_ant) & (df['dataprincipal'] <= data_fim_ant)]

    # card de faturamento 
    vl_faturamento_atual = dfatual['faturamento'].sum()
    vl_faturamento_anterior = dfanterior['faturamento'].sum()
    var_faturamento = ((vl_faturamento_atual/vl_faturamento_anterior)-1)*100
   
    # card de  ticket medio 
    vl_pedidos_atual = dfatual['pedidos'].sum()
    vl_pedidos_anterior = dfanterior['pedidos'].sum()
    vl_tickemedio_atual =vl_faturamento_atual/vl_pedidos_atual
    vl_tickemedio_anterior =vl_faturamento_anterior/vl_pedidos_anterior
    var_tickemedio = ((vl_tickemedio_atual/vl_tickemedio_anterior )-1)*100
   # card de  pedidos
    var_pedidos = ((vl_pedidos_atual/vl_pedidos_anterior )-1)*100
    # card de  compradores
    qtd_compradores_unicos_atual = dfatual['userprofileid'].nunique()
    qtd_compradores_unicos_anterior = dfanterior['userprofileid'].nunique()
    var_compradores_unicos = ((qtd_compradores_unicos_atual/qtd_compradores_unicos_anterior )-1)*100

    # Agrupamento por 'dataprincipal'
    groupby_fat_df = dfatual.groupby('dataprincipal').agg(
    faturamentodia=pd.NamedAgg(column='faturamento', aggfunc='sum')
    ).reset_index()

   
 
   
    # Criando uma tabela pivot
    groupby_pagamento_df = df.pivot_table(
        index='paymentnames', 
        columns='comparacao', 
        values='faturamento', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    groupby_pagamento_df['varfaturamento'] = (((groupby_pagamento_df['atual'])/ groupby_pagamento_df['anterior'])-1)*100
    groupby_pagamento_df= groupby_pagamento_df.sort_values(by='atual', ascending=False ).reset_index(drop=True)
    groupby_pagamento_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)
    
    groupby_pagamento_df=groupby_pagamento_df.query('atual>= 8000 ')
    groupby_pagamento_df= groupby_pagamento_df.sort_values(by='atual', ascending=True )


    consulta_item=f"""                
                                    
            select 
            'atual' as comparacao,
            idprod ,
            namesku,
            idcat,
            namecategory,
            cast(sum(revenue_without_shipping) as float) as Faturamento_item,
            cast(avg(revenue_orders_out_ship) as float) as Faturamento_orders,
            cast(sum(quantityitems)  as float) as quantidade_itens
            from "{integration}".orders_items_ia oi 
            where date_trunc('day',oi.creationdate)  >= '{str(data_inicio)}'
            and 
            date_trunc('day',oi.creationdate) <=   '{str(data_fim)}'
            group by 
            idprod ,
            namesku,
            idcat,
            namecategory

            union all 
                    
            select 
            'anterior' as comparacao,
            idprod ,
            namesku,
            idcat,
            namecategory,
            cast(sum(revenue_without_shipping) as float) as Faturamento_item,
            cast(avg(revenue_orders_out_ship) as float) as Faturamento_orders,
            cast(sum(quantityitems)  as float) as quantidade_itens
            from "{integration}".orders_items_ia oi 
            where date_trunc('day',oi.creationdate)  >= '{str(data_inicio_ant)}'
            and 
            date_trunc('day',oi.creationdate) <=  '{str(data_fim_ant)}'
            group by 
            idprod ,
            namesku,
            idcat,
            namecategory
        """

    df_item=WriteJsonToPostgres(data_conection_info,consulta_item).query_dataframe()

    # Criando uma tabela pivot
    groupby_categoria_df = df_item.pivot_table(
        index='namecategory', 
        columns='comparacao', 
        values='faturamento_item', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()
    groupby_categoria_df = groupby_categoria_df.nlargest(10, 'atual')
    groupby_categoria_df['varfaturamento'] = ((groupby_categoria_df['atual']/ groupby_categoria_df['anterior'])-1)*100
    groupby_categoria_df= groupby_categoria_df.sort_values(by='atual' ).reset_index(drop=True)
    groupby_categoria_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)



  
    # Criando uma tabela pivot
    groupby_sku_ticket_df = df_item.pivot_table(
        index='namesku', 
        columns='comparacao', 
        values='faturamento_orders', 
        aggfunc='mean',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    groupby_sku_ticket_df = groupby_sku_ticket_df.nlargest(10, 'atual').sort_values(by='atual', ascending=False )
    groupby_sku_ticket_df['varfaturamento'] = ((groupby_sku_ticket_df['atual']/ groupby_sku_ticket_df['anterior'])-1)*100
    groupby_sku_ticket_df= groupby_sku_ticket_df.sort_values(by='atual' ).reset_index(drop=True)
    groupby_sku_ticket_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)

   
    # Criando uma tabela pivot
    groupby_sku_df = df_item.pivot_table(
        index='namesku', 
        columns='comparacao', 
        values='faturamento_item', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    groupby_sku_df = groupby_sku_df.nlargest(10, 'atual')
    groupby_sku_df['varfaturamento'] = ((groupby_sku_df['atual']/ groupby_sku_df['anterior'])-1)*100
    groupby_sku_df= groupby_sku_df.sort_values(by='atual' ).reset_index(drop=True)
    groupby_sku_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)

    
    
    consulta_mapa=f"""                
              select 
                'atual' as comparacao,
                ltrim(rtrim(selectedaddresses_0_state)) as selectedaddresses_0_state,
                cast(sum(revenue) as float) as Faturamento,
                cast(sum(quantityorder) as float) as Pedidos
                from "{integration}".orders_ia oi 
                 where date_trunc('day',oi.creationdate)  >= '{str(data_inicio)}'
                and 
                date_trunc('day',oi.creationdate) <=  '{str(data_fim)}'
                group by 
                ltrim(rtrim(selectedaddresses_0_state))                       
            
        """

    df_mapa=WriteJsonToPostgres(data_conection_info,consulta_mapa).query_dataframe()

   
    milhao = 0
    mil = 0
    absoluto = 0 
    for index, row in groupby_sku_df.iterrows():
        if(row['atual'] >=1000000):
            milhao = milhao + 1
        elif row['atual'] >=1000:
            mil = mil+ 1
        else:
            absoluto = absoluto +1     

    abreviacao = 0
    if(milhao >=5 ):
        abreviacao =2
    elif mil+milhao >= 5:
        abreviacao =1
    else:
        abreviacao =0  

            

    grafico_card(f"{diretorio}/cardfaturamento{celular}.png",'Faturamento', formatar_numerocasas_br(vl_faturamento_atual,0,1),formatar_numerocasas_br(vl_faturamento_anterior,0,1),formatar_numerocasas_br(var_faturamento,1,0))
    grafico_card(f"{diretorio}/cardticketmedio{celular}.png",'Ticket Médio',formatar_numerocasas_br(vl_tickemedio_atual,0,1),formatar_numerocasas_br(vl_tickemedio_anterior,0,1),formatar_numerocasas_br(var_tickemedio,1,0))
    grafico_card(f"{diretorio}/cardpedidos{celular}.png",'Pedidos','Pedidos',formatar_numerocasas_br (vl_pedidos_atual,0,0),formatar_numerocasas_br(vl_pedidos_anterior,0,0),formatar_numerocasas_br(var_pedidos,1,0))
    grafico_card(f"{diretorio}/cardcompradores{celular}.png",'Compradores',formatar_numerocasas_br (qtd_compradores_unicos_atual,0,0),formatar_numerocasas_br(qtd_compradores_unicos_anterior,0,0),formatar_numerocasas_br(var_compradores_unicos,1,0))
    grafico_linha(f"{diretorio}/grafico_linha_faturamentopordia{celular}.png","Faturamento por dia - em R$",groupby_fat_df['dataprincipal'],groupby_fat_df['faturamentodia'],abreviacao)
    grafico_horizontal(f"{diretorio}/grafico_h_fatcategoria{celular}.png","Top 10 faturamento por categoria  - em R$",groupby_categoria_df['namecategory'],groupby_categoria_df['atual'],groupby_categoria_df['varfaturamento'],abreviacao,2)
    grafico_horizontal(f"{diretorio}/grafico_h_tipopagmento{celular}.png","Forma de pagamento mais utilizadas - em R$",groupby_pagamento_df['paymentnames'],groupby_pagamento_df['atual'],groupby_pagamento_df['varfaturamento'],abreviacao,1)
    grafico_horizontal(f"{diretorio}/grafico_h_ticketmedio{celular}.png","Ticket médio por produto - em R$",groupby_sku_ticket_df['namesku'],groupby_sku_ticket_df['atual'],groupby_sku_ticket_df['varfaturamento'],abreviacao,3)
    grafico_horizontal(f"{diretorio}/grafico_h_fatproduto{celular}.png","Top 10 produtos mais vendidos - em R$",groupby_sku_df['namesku'],groupby_sku_df['atual'],groupby_sku_df['varfaturamento'],abreviacao,1)
    grafico_mapa(diretorio,f"{diretorio}/grafico_mapa_mapa{celular}.png",'Top 5 faturamento por estado - em R$',  df_mapa,abreviacao)



def gerar_pdf(semana,celular,integration,extensao,diretorio):

          
    tempo=datetime.datetime.today()
    if(tempo.month ==1):
        ano = tempo.year-1 #datetime.now()
    else:
        ano = tempo.year

    semana_data = datetime.datetime.strptime(f'{ano}-W{semana}-0', "%Y-W%W-%w")
    # Definindo o intervalo de datas

    data_inicio = semana_data -  pd.DateOffset(days=7)
    data_fim = semana_data - pd.DateOffset(days=1)
    
    # Definindo o intervalo de datas
    data_inicio_ant = semana_data -pd.DateOffset(days=14)
    data_fim_ant = semana_data -pd.DateOffset(days=8)

   


    criar_relatorio_semanal(celular,integration,data_inicio,data_fim,data_inicio_ant,data_fim_ant,diretorio)


    pdf = FPDF(format='A4')
    pdf.add_page()
    altura = 20

    data_inicio_formatado = str(data_inicio).replace(' 00:00:00','').split('-')
    data_fim_formatado = str(data_fim).replace(' 00:00:00','').split('-')

    data_inicio_ant_formatado = str(data_inicio_ant).replace(' 00:00:00','').split('-')
    data_fim_ant_formatado = str(data_fim_ant).replace(' 00:00:00','').split('-')
    
    #pdf.add_font('OpenSans', '', 'Fonte/Open_Sans/static/OpenSans-Regular.ttf')
    pdf.set_font('Arial', size=18)
 
    pdf.cell(0, 5, text="Relatório Mensal", align='C')

    pdf.ln(5)

    mesatualtexto = 'Semana '+ str(semana) + ': '+ data_inicio_formatado[2]+'/'+data_inicio_formatado[1]+'/'+data_inicio_formatado[0]+' até ' + data_fim_formatado[2]+'/'+data_fim_formatado[1]+'/'+data_fim_formatado[0]
    mescompativotexto = "Período comparativo : "+ data_inicio_ant_formatado[2]+'/'+data_inicio_ant_formatado[1]+'/'+data_inicio_ant_formatado[0]+' até ' + data_fim_ant_formatado[2]+'/'+data_fim_ant_formatado[1]+'/'+data_fim_ant_formatado[0]+ ' (semana anterior)'
    pdf.set_font('Arial', size=8)
    pdf.cell(0, 5, text=mesatualtexto, align='C')
    pdf.ln(4)
    pdf.cell(0, 5,text=mescompativotexto, align='C')


    #colocando logo do lado esquerdo
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 15, y = 15 , w = 20)
    #colocando logo do lado direito
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 180, y = 15 , w = 20)

   

    # # cards
    pdf.image(f"{diretorio}/cardfaturamento{celular}.png", x = 10, y = altura + 10, w = 47)
    pdf.image(f"{diretorio}/cardpedidos{celular}.png", x = 58, y =altura +  10, w = 47)
    pdf.image(f"{diretorio}/cardticketmedio{celular}.png", x = 106, y = altura + 10, w = 47)
    pdf.image(f"{diretorio}/cardcompradores{celular}.png", x = 154, y = altura + 10, w = 47)

    pdf.image(f"{diretorio}/grafico_linha_faturamentopordia{celular}.png", x = 10.5, y =altura + 33,w=94 ,h = 72)   
    pdf.image(f"{diretorio}/grafico_h_fatcategoria{celular}.png", x = 106, y = altura +32.5 ,w=96 ,h = 73 ) 
 
    
    pdf.image(f"{diretorio}/grafico_h_fatproduto{celular}.png", x = 9.5, y = altura +108 ,w=96 ,h = 73 ) 
    pdf.image(f"{diretorio}/grafico_h_ticketmedio{celular}.png", x = 106, y =altura + 108 ,w=96 ,h = 73 ) 

    pdf.image(f"{diretorio}/grafico_mapa_mapa{celular}.png", x = 10, y = altura + 184 ,w=95,h = 72 ) 
    pdf.image(f"{diretorio}/grafico_h_tipopagmento{celular}.png", x = 106.5, y =altura + 183.5 ,w=95 ,h = 73 ) 
  


    current_datetime = datetime.datetime.now() 
    numeric_datetime = current_datetime.strftime('%Y%m%d%H%M%S')
    pdf.output(f"{diretorio}/relatorio_semanal_{celular}_{semana}_{numeric_datetime}.pdf")
    
    filename=f"relatorio_semanal_{celular}_{semana}_{numeric_datetime}.pdf"

    return filename




def get_logo(logo,celular, diretorio):

    if(logo == ""):
        extensao = '.png'
        ExecuteBlob().get_file("appgemdata","teams-pictures/Logo_GD_preto.png",f"{diretorio}/logo_{celular}{extensao}") 
       
    else:   
        start_index = logo.rfind(".")
        extensao = logo[start_index:] 
        ExecuteBlob().get_file("appgemdata",logo,f"{diretorio}/logo_{celular}{extensao}") 
       
    return extensao


def salvar_pdf_blob(idintegration,diretorio,filename):
        try:
            ExecuteBlob().upload_file("reportclient",f"{idintegration}/{filename}",f"{diretorio}/{filename}") 
            print ("PDF gravado no blob com sucesso") 
        except Exception as e:
            print (f"erro ao gravar PDF mensal {e}") 


def criar_pasta_temp(celular):
    #Gera um UUID para criar um diretório único
    unique_id = uuid.uuid4().hex
    temp_dir = f"/opt/airflow/temp/{unique_id}_{celular}"

    # Cria o diretório
    os.makedirs(temp_dir, exist_ok=True)

    return temp_dir

    # Exemplo de caminho para salvar arquivos dentro do diretório temporário
    #temp_file_path = os.path.join(temp_dir, 'imagem.png')




def set_globals(data_conection,api_info,celphone,weekly,cami_logo,**kwargs):
    global  data_conection_info,idintegration ,celular,semana,logo
    data_conection_info = data_conection
    idintegration = api_info
    celular= celphone
    semana= weekly
    logo= cami_logo

    if not all([idintegration,celular,semana]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    diretorio=criar_pasta_temp(celular)
    print(diretorio)

    extensao=get_logo(logo,celular,diretorio)
    filename=gerar_pdf(int(semana), celular,idintegration,extensao,diretorio)

    salvar_pdf_blob(idintegration,diretorio,filename)
    
    # Remover o diretório após o uso
    shutil.rmtree(diretorio, ignore_errors=True)
    print(f"Diretório temporário {diretorio} removido.")


