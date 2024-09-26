
from modules.dbpgconn import *

from modules.save_to_blobstorageteste import *

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
mes = None
logo = None
isemail = None


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
    # font_path = 'Fonte/Open_Sans/static/Arial-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    #plt.rcParams['font.family'] = open_sans.get_family()
    
    
    

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
    ax.text(horizontal, altura -0.28, vlanterior_card + ' no mês anterior', fontsize=15, color='#938F9A')

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

   # print(datas)
    # # Convertendo os valores e datas para plotagem
    dates = np.arange(len(datas))
    
    dias = datas.dt.strftime('%d') 

    # Identificar os 5 maiores e 5 menores valores
    indices = np.argsort(valores)  
    top5 = indices[-1:]  # Índices dos 1 maiores
    bottom5 = indices[:1]  # Índices dos 1 menores

    # Criando a figura e o eixo
    fig, ax = plt.subplots(figsize=(12, 6))  # Tamanho definido para a figura
    # Plotando os dados
    ax.plot(dates, valores, marker='o', linestyle='-', color='#006DCC')

    # # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/Arial-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()


    for i in top5:
        ax.annotate(f'{abreviar_valor_br(valores[i],0,abreviacao)}', (dates[i], valores[i]), textcoords="offset points", xytext=(0,10), ha='center',fontsize=9,color ='white',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='#006DCC', edgecolor='#006DCC')
                        ) 
        
    for i in bottom5:
        ax.annotate(f'{abreviar_valor_br(valores[i],0,abreviacao)}', (dates[i], valores[i]), textcoords="offset points", xytext=(0,10), ha='center',fontsize=9,color ='white',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='#006DCC', edgecolor='#006DCC')
                        ) 

    a= 1
    b= 0
    for i in dates:
        if(b ==a ):
            ax.annotate(f'{abreviar_valor_br(valores[b],0,abreviacao)}', (dates[b], valores[b]), textcoords="offset points", xytext=(0,10), ha='center',fontsize=9,color ='white',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor='#006DCC', edgecolor='#006DCC')
                        ) 
            a=b+2
           
        b = b + 1

                    

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
    # font_path = 'Fonte/Open_Sans/static/Arial-Regular.ttf'
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
    # font_path = 'Fonte/Open_Sans/static/Arial-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)
    # plt.rcParams['font.family'] = open_sans.get_family()
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

##############################################################################
#esse aqui é novo


def grafico_abc(nm_imagem,titulografico,texto,valores,acumporc,abreviacao):

    
    # # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/Arial-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()


    textoajustado = [quebrarlinha(label, 25,25) for label in texto]

    # Passo 3: Criar o gráfico
    fig, ax = plt.subplots(figsize=(12, 6))
    bars=ax.bar(textoajustado, valores, label='Faturamento',color='#006DCC', width=0.5)
   
    ax.set_ylim(0, max(valores) * 1.5)


    ax2 = ax.twinx()
    line, =ax2.plot(textoajustado, acumporc, label='% acumulado', marker='o',color='#283747')

    ax2.set_ylim(-100, max(acumporc))
    

    for bar in bars:
        yval = bar.get_height()
        ax.annotate(f'{abreviar_valor_br(yval,0,abreviacao)}', xy=(bar.get_x() + bar.get_width()/2, yval), 
                    xytext=(0, 3), textcoords="offset points", 
                    ha='center', va='bottom',color='#15131B', fontsize=12)

    for i, txt in enumerate(acumporc):
        ax2.annotate(f'{formatar_numerocasas_br(txt,1,0)}%', xy=(i, txt), 
                    xytext=(0, 3), textcoords="offset points", 
                    ha='center', va='bottom',color='white', fontsize=10, bbox=dict(boxstyle="round,pad=0.3", facecolor='#283747', edgecolor='#283747'))
    

    # Ajustar os rótulos do eixo x para ficarem diagonais
    ax.set_xticks(range(len(textoajustado)))
    ax.set_xticklabels(textoajustado, rotation=45, ha='right',fontsize=10, color='#676270' )  # Ajuste aqui
    
    # Removendo os traços dos ticks dos eixos
    ax.tick_params(axis='x', length=0)  # Remove os ticks do eixo X
    ax.tick_params(axis='y', length=0)  # Remove os ticks do eixo Y
    ax2.tick_params(axis='y', length=0)  # Remove os ticks do eixo Y

    ax.set_xlabel('')  # Remove o título do eixo x
    ax.set_ylabel('')  # Remove o título do eixo y
    ax2.set_ylabel('') # Remove o título do eixo y secundário

    # Remover os números dos eixos
 #   ax.set_xticks([])  # Remove os ticks do eixo x
    ax.set_yticks([])  # Remove os ticks do eixo y
    ax2.set_yticks([]) # Remove os ticks do eixo y secundário

    ax.spines['bottom'].set_color('none')
    ax.spines['left'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')

    ax2.spines['bottom'].set_color('none')
    ax2.spines['left'].set_color('none')
    ax2.spines['right'].set_color('none')
    ax2.spines['top'].set_color('none')
 
    
    ax2.legend(loc=(0.03, -0.90),facecolor='#676270',edgecolor = '#676270', fontsize=10, frameon=False)
    
   # title=plt.title(titulografico, fontsize=18, fontweight='bold', loc='left', pad=40, color= '#15131B')
   # title.set_position((0.05, 1)) 

    fig.suptitle(titulografico, fontsize=18, fontweight='bold', x=0.5, ha='center',color="#15131B", y=0.92 )

    fancy_box = patches.FancyBboxPatch((0.03, 0.03), width=0.94, height=0.94,
                      boxstyle="round,pad=0.02,rounding_size=0.02", ec="lightgrey", lw=0.3, facecolor="none",
                      transform=fig.transFigure, clip_on=False)

    fig.patches.append(fancy_box)
    # Exibindo o gráfico
  
    fig.subplots_adjust(top=0.8 ,bottom=0.4,left=0.10 ,right=0.90)
    
    # Exibindo o gráfico
    #plt.tight_layout()
    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=300,bbox_inches='tight', pad_inches=0)
    plt.close()
   # plt.show()





def grafico_pizza(nm_imagem,titulografico,texto,valores,abreviacao,isgraficosexo):

    if(isgraficosexo == 1):
        colors = ['#ff007f', '#006DCC']
    else:
        colors = ['#154360', '#3498db']
        

    # Caminho para a fonte 'Open Sans'
    # font_path = 'Fonte/Open_Sans/static/Arial-Regular.ttf'
    # open_sans = FontProperties(fname=font_path)

    # plt.rcParams['font.family'] = open_sans.get_family()


    # Dados para o gráfico de pizza (donut) com todos os percentuais fora do gráfico
    fig, ax = plt.subplots(figsize=(8, 6))
    wedges, texts, autotexts = ax.pie(valores, colors=colors, autopct='',
                                    shadow=True, startangle=90, wedgeprops=dict(width=0.4, edgecolor='#676270', linewidth=1))

    texts[0].set_text('    '+abreviar_valor_br(valores[0],0,abreviacao)+' ('+formatar_numerocasas_br((valores[0]/valores.sum())*100,1,0)+'%)    ' )  # Percentual para Frete Grátis
   # texts[0].set_position((-1.5, 0))  # Move o percentual de Frete Grátis para fora do gráfico
    texts[0].set_fontsize(14)  # Aumentando o tamanho da fonte para o "60%"
    texts[0].set_color('#15131B')

    #texts[1].set_position((0.8, 0))  # Move o percentual de Frete Pago para fora do gráfico
    texts[1].set_text('    '+abreviar_valor_br(valores[1],0,abreviacao)+' ('+formatar_numerocasas_br((valores[1]/valores.sum())*100,1,0)+'%)    ' )  # Percentual para Frete Pago
    texts[1].set_fontsize(14)  # Aumentando o tamanho da fonte para o "60%"
    texts[1].set_color('#15131B')
    # Iguala a razão de aspecto para que o pie seja desenhado como um círculo.
    ax.axis('equal')

    # Adicionando legenda externa no canto inferior esquerdo
    legenda=ax.legend(wedges, texto, loc='lower center', ncol=2,bbox_to_anchor=(0.2, -0.12),fontsize=15)
    legenda.get_frame().set_edgecolor('none') 


    # title=plt.title(titulografico, fontsize=18, fontweight='bold', loc='left', pad=30, color= '#15131B')
    # title.set_position((-0.35, -0.35)) 
    fig.suptitle(titulografico, fontsize=18, fontweight='bold', x=0.5, ha='center',color="#15131B", y=0.92 )


    fancy_box = patches.FancyBboxPatch((0.03, 0.03), width=0.94, height=0.94,
                      boxstyle="round,pad=0.02,rounding_size=0.02", ec="lightgrey", lw=0.3, facecolor="none",
                      transform=fig.transFigure, clip_on=False)

    fig.patches.append(fancy_box)
    
    fig.subplots_adjust(top=0.8,left=0.3,right= 0.7)
    #fig.subplots_adjust(top=0.8 )

    # Exibindo o gráfico
    #plt.tight_layout()
    # Salvar a imagem
    plt.savefig(nm_imagem, dpi=300,bbox_inches='tight', pad_inches=0)
#    plt.show()
    plt.close()




def criar_relatorio_mensal (mes,celular,integration,data_inicio,data_fim,data_inicio_ant,data_fim_ant,diretorio): 
   
   
    consulta_order=f"""
        select 
        'atual' as comparacao,
        date_trunc('day',oi.creationdate)  as dataprincipal ,
        selectedaddresses_0_state,
        paymentnames,
        userprofileid,
        saleschannel,
        FreeShipping,
        Sexo,
        selectedaddresses_0_city,
        sum(revenue) as Faturamento,
        sum(quantityorder) as Pedidos

        from "{integration}".orders_ia oi 
        where 
        date_trunc('month',oi.creationdate)  = '{str(data_inicio)}'
        group by 
        date_trunc('day',oi.creationdate)  ,
        oi.selectedaddresses_0_state,
        oi.paymentnames,
        userprofileid,
        saleschannel,
        FreeShipping,
        Sexo,
        selectedaddresses_0_city

        union all 

        select 
        'anterior' as comparacao,
        date_trunc('day',oi.creationdate)  as dataprincipal ,
        selectedaddresses_0_state,
        paymentnames,
        userprofileid,
        saleschannel,
        FreeShipping,
        Sexo,
        selectedaddresses_0_city,
        cast(sum(revenue) as float) as Faturamento,
        cast(sum(quantityorder)  as float) as Pedidos
        from  "{integration}".orders_ia oi
        where 
        date_trunc('month',oi.creationdate)  = '{str(data_inicio_ant)}'
        group by 
        date_trunc('day',oi.creationdate)  ,
        oi.selectedaddresses_0_state,
        oi.paymentnames,
        userprofileid,
        saleschannel,
        FreeShipping,
        Sexo,
        selectedaddresses_0_city
          
          """
    
    df=WriteJsonToPostgres(data_conection_info,consulta_order).query_dataframe()

    
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

   
#   # Criando uma tabela pivot
#     groupby_canal_df = df.pivot_table(
#         index='saleschannel', 
#         columns='comparacao', 
#         values='faturamento', 
#         aggfunc='sum',
#         fill_value=0  # Preenche com zero onde não há dados
#     ).reset_index()
# #    print(groupby_canal_df)
    
#     groupby_canal_df['varfaturamento'] = ((groupby_canal_df['atual']/ groupby_canal_df['anterior'])-1)*100
#     groupby_canal_df= groupby_canal_df.sort_values(by='atual' ).reset_index(drop=True)
#     groupby_canal_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)

   
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



    # Criando uma tabela pivot
    groupby_frete_df = df.pivot_table(
        index='freeshipping', 
        columns='comparacao', 
        values='faturamento', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    groupby_frete_df= groupby_frete_df.sort_values(by='atual', ascending=False ).reset_index(drop=True)
    groupby_frete_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)




    # Criando uma tabela pivot
    groupby_sexo_df = df.pivot_table(
        index='sexo', 
        columns='comparacao', 
        values='faturamento', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    groupby_sexo_df= groupby_sexo_df.sort_values(by='atual', ascending=False ).reset_index(drop=True)
    groupby_sexo_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)



    # Criando uma tabela pivot
    groupby_cidade_df = df.pivot_table(
        index='selectedaddresses_0_city', 
        columns='comparacao', 
        values='faturamento', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    groupby_cidade_df = groupby_cidade_df.nlargest(10, 'atual').sort_values(by='atual' , ascending=False).reset_index(drop=True)
    groupby_cidade_df['varfaturamento'] = ((groupby_cidade_df['atual']/ groupby_cidade_df['anterior'])-1)*100
    groupby_cidade_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)
    groupby_cidade_df= groupby_cidade_df.sort_values(by='atual', ascending=True )






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
            where  date_trunc('month',oi.creationdate)  = '{str(data_inicio)}'
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
            where  date_trunc('month',oi.creationdate)  = '{str(data_inicio_ant)}'
            group by 
            idprod ,
            namesku,
            idcat,
            namecategory
        """
  
    df_item=WriteJsonToPostgres(data_conection_info,consulta_item).query_dataframe()
    
  
    # Criando uma tabela pivot
    groupby_sku_df = df_item.pivot_table(
        index='namesku', 
        columns='comparacao', 
        values='faturamento_item', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()

    
    groupby_sku_df['varfaturamento'] = ((groupby_sku_df['atual']/ groupby_sku_df['anterior'])-1)*100
    groupby_sku_df= groupby_sku_df.sort_values(by='atual', ascending=False).reset_index(drop=True)
    groupby_sku_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)

    groupby_sku_df['varfaturamento'] = ((groupby_sku_df['atual']/ groupby_sku_df['anterior'])-1)*100
    groupby_sku_df['percentual'] = groupby_sku_df['atual'] / groupby_sku_df['atual'].sum()
    groupby_sku_df['percacumulado'] = (groupby_sku_df['percentual'].cumsum())*100
    groupby_sku_df = groupby_sku_df.nlargest(10, 'atual').sort_values(by='atual' , ascending=False)
   

  
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

  
    df_item['preco_medio'] = df_item['faturamento_item']/df_item['quantidade_itens']
   
    # Criando uma tabela pivot
    groupby_sku_preco_df = df_item.pivot_table(
        index='namesku', 
        columns='comparacao', 
        values='preco_medio', 
        aggfunc='sum',
        fill_value=0  # Preenche com zero onde não há dados
    ).reset_index()
       
    groupby_sku_preco_df = groupby_sku_preco_df.nlargest(10, 'atual').sort_values(by='atual' , ascending=False)
    groupby_sku_preco_df['varfaturamento'] = ((groupby_sku_preco_df['atual']/ groupby_sku_preco_df['anterior'])-1)*100
    groupby_sku_preco_df= groupby_sku_preco_df.sort_values(by='atual' ).reset_index(drop=True)
    groupby_sku_preco_df.replace(to_replace=[np.inf, -np.inf],value = 0, inplace=True)

    



    
    consulta_mapa=f"""                
              select 
                'atual' as comparacao,
                ltrim(rtrim(selectedaddresses_0_state)) as selectedaddresses_0_state,
                cast(sum(revenue) as float) as Faturamento,
                cast(sum(quantityorder) as float) as Pedidos
                from "{integration}".orders_ia oi 
                 where  date_trunc('month',oi.creationdate)  = '{str(data_inicio)}'
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
    grafico_card(f"{diretorio}/cardpedidos{celular}.png",'Pedidos',formatar_numerocasas_br (vl_pedidos_atual,0,0),formatar_numerocasas_br(vl_pedidos_anterior,0,0),formatar_numerocasas_br(var_pedidos,1,0))
    grafico_card(f"{diretorio}/cardcompradores{celular}.png",'Compradores',formatar_numerocasas_br (qtd_compradores_unicos_atual,0,0),formatar_numerocasas_br(qtd_compradores_unicos_anterior,0,0),formatar_numerocasas_br(var_compradores_unicos,1,0))
    
    grafico_linha(f"{diretorio}/grafico_linha_faturamentopordia{celular}.png","Faturamento por dia - em R$",groupby_fat_df['dataprincipal'],groupby_fat_df['faturamentodia'],abreviacao)

    #grafico_horizontal("canal"+celular,"Top 10 faturamentos por canais - em R$",groupby_canal_df['saleschannel'],groupby_canal_df['atual'],groupby_canal_df['varfaturamento'],abreviacao)
  

    grafico_horizontal(f"{diretorio}/grafico_h_precomedio{celular}.png","Preço médio por produto - em R$",groupby_sku_preco_df['namesku'],groupby_sku_preco_df['atual'],groupby_sku_preco_df['varfaturamento'],abreviacao,2)
  
    grafico_horizontal(f"{diretorio}/grafico_h_ticketmedio{celular}.png","Ticket médio por produto - em R$",groupby_sku_ticket_df['namesku'],groupby_sku_ticket_df['atual'],groupby_sku_ticket_df['varfaturamento'],abreviacao,3)

    grafico_horizontal(f"{diretorio}/grafico_h_tipopagmento{celular}.png","Formas de pagamento mais utilizadas - em R$",groupby_pagamento_df['paymentnames'],groupby_pagamento_df['atual'],groupby_pagamento_df['varfaturamento'],abreviacao,1)

    grafico_abc(f"{diretorio}/grafico_abc_fatprodutoabc{celular}.png","Top 10 produtos mais vendidos - em R$",groupby_sku_df['namesku'],groupby_sku_df['atual'],groupby_sku_df['percacumulado'],abreviacao)
   
    grafico_mapa(diretorio,f"{diretorio}/grafico_mapa_mapa{celular}.png",'Top 5 faturamento por estado - em R$',  df_mapa,abreviacao)

  
    grafico_pizza(f"{diretorio}/grafico_pizza_frete{celular}.png","Representatividade por tipo de frete - em R$",groupby_frete_df['freeshipping'],groupby_frete_df['atual'],1,0)
        
    #grafico_pizza("sexo"+celular,"Faturamento por sexo - em R$",groupby_sexo_df['sexo'],groupby_sexo_df['atual'],1,1)

    grafico_horizontal(f"{diretorio}/grafico_h_fatcidade{celular}.png","Top 10 faturamento por cidade - em R$",groupby_cidade_df['selectedaddresses_0_city'],groupby_cidade_df['atual'],groupby_cidade_df['varfaturamento'],abreviacao,1)
 





def gerar_pdf(mes,celular,integration,extensao,diretorio):
    
    tempo=datetime.datetime.today()
    if(tempo.month ==1):
        ano = tempo.year-1 #datetime.now()
    else:
        ano = tempo.year
        

    # Definindo o intervalo de datas

    data_inicio = datetime.datetime(ano, mes, 1)


    if mes == 12:
        data_fim = datetime.datetime(ano + 1, 1, 1) - datetime.timedelta(days=1)
    else:
        data_fim = datetime.datetime(ano, mes + 1, 1) - datetime.timedelta(days=1)


    data_fim_ant = data_inicio -pd.DateOffset(days=1)
    
    # Definindo o intervalo de datas
    data_inicio_ant = datetime.datetime(data_fim_ant.year,data_fim_ant.month,1)


    criar_relatorio_mensal(mes,celular,integration,data_inicio,data_fim,data_inicio_ant,data_fim_ant,diretorio)



    pdf = FPDF(format='A4')
    pdf.add_page()
   ##print(largura_pagina)
    # # Você pode adicionar algum conteúdo na primeira página
    # pdf.set_font("Arial", size=12)
    # pdf.cell(200, 10, txt="Bem-vindo à primeira página!", ln=True, align='C')
     
    altura = 20
    
 
    #colocando titulo da pagina : 
    data_inicio_formatado = str(data_inicio).replace(' 00:00:00','').split('-')
    data_fim_formatado = str(data_fim).replace(' 00:00:00','').split('-')

    data_inicio_ant_formatado = str(data_inicio_ant).replace(' 00:00:00','').split('-')
    data_fim_ant_formatado = str(data_fim_ant).replace(' 00:00:00','').split('-')

    # pdf.add_font('Arial', '', 'Fonte/Open_Sans/static/Arial-Regular.ttf')
    pdf.set_font('Arial', size=18)
 
    pdf.cell(0, 5, txt="Relatório Mensal", align='C')

    pdf.ln(5)

    mesatualtexto = 'Mês '+ str(mes) + ': '+ data_inicio_formatado[2]+'/'+data_inicio_formatado[1]+'/'+data_inicio_formatado[0]+' até ' + data_fim_formatado[2]+'/'+data_fim_formatado[1]+'/'+data_fim_formatado[0]
    mescompativotexto = "Período comparativo: "+ data_inicio_ant_formatado[2]+'/'+data_inicio_ant_formatado[1]+'/'+data_inicio_ant_formatado[0]+' até ' + data_fim_ant_formatado[2]+'/'+data_fim_ant_formatado[1]+'/'+data_fim_ant_formatado[0]+ ' (mês anterior)'
    pdf.set_font('Arial', size=8)
    pdf.cell(0, 5, txt=mesatualtexto, align='C')
    pdf.ln(4)
    pdf.cell(0, 5,txt=mescompativotexto, align='C')
    

    #colocando logo do lado esquerdo
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 15, y = 15 , w = 20)

    #colocando logo do lado direito
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 180, y = 15 , w = 20)

    # # cards
    pdf.image(f"{diretorio}/cardfaturamento{celular}.png", x = 10, y = altura + 10, w = 47)
    pdf.image(f"{diretorio}/cardpedidos{celular}.png", x = 58, y =altura +  10, w = 47)
    pdf.image(f"{diretorio}/cardticketmedio{celular}.png", x = 106, y = altura + 10, w = 47)
    pdf.image(f"{diretorio}/cardcompradores{celular}.png", x = 154, y = altura + 10, w = 47)
    
    #grafico de linha faixa inteira (parte a1 e a2 ) 
    pdf.image(f"{diretorio}/grafico_linha_faturamentopordia{celular}.png", x = 10, y =altura + 32, w = 190,h=90 )   
    #grafico de pizza (parte b1)
    pdf.image(f"{diretorio}/grafico_pizza_frete{celular}.png",  x = 10, y = altura +126 ,w=95,h = 72  ) 
    #grafico de tipo de pagamento (parte b2)
    pdf.image(f"{diretorio}/grafico_h_tipopagmento{celular}.png", x = 106, y =altura + 125.5 ,w=95 ,h = 73) 
    #grafico de mapa (parte c1)
    pdf.image(f"{diretorio}/grafico_mapa_mapa{celular}.png", x = 10, y = altura +202 ,w=95,h = 72 ) 
    #grafico de faturamento por cidade (parte c2) 
    pdf.image(f"{diretorio}/grafico_h_fatcidade{celular}.png", x = 106, y =altura +201.5 ,w=95,h = 73 ) 
     
    #pagina nova (segunda pagina )
    pdf.add_page()
    pdf.set_font('Arial', size=18)
    pdf.cell(0, 5, txt="Relatório Mensal", align='C')
    pdf.ln(5)
    pdf.set_font('Arial', size=8)
    pdf.cell(0, 5, txt=mesatualtexto, align='C')
    pdf.ln(4)
    pdf.cell(0, 5,txt=mescompativotexto, align='C')
    #colocando logo do lado esquerdo
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 15, y = 15 , w = 20)
    #colocando logo do lado direito
    pdf.image(f"{diretorio}/logo_{celular}{extensao}", x = 180, y = 15 , w = 20)

    pdf.image(f"{diretorio}/grafico_abc_fatprodutoabc{celular}.png",x = 10,  y = altura + 12 ,w = 190 ,h=90) 
    pdf.image(f"{diretorio}/grafico_h_precomedio{celular}.png",x = 9, y = altura + 106 ,w=95 ) 
    pdf.image(f"{diretorio}/grafico_h_ticketmedio{celular}.png",x = 106, y = altura + 106 ,w=95 ) 


    pdf.output(f"{diretorio}/relatorio_mensal_{celular}.pdf")
    
    diretoriopdf=f"{diretorio}/relatorio_mensal_{celular}.pdf"

    return diretoriopdf
    


def get_logo(logo,celular, diretorio):

    if(logo == ""):
        extensao = '.png'
        ExecuteBlob().get_file("appgemdata","teams-pictures/Logo_GD_preto.png",f"{diretorio}/logo_{celular}{extensao}") 
        return extensao
    else:   
        try:
            start_index = logo.rfind(".")
            extensao = logo[start_index:] 
            ExecuteBlob().get_file("appgemdata",logo,f"{diretorio}/logo_{celular}{extensao}") 
            return extensao 
        except Exception :
            extensao = '.png'
            ExecuteBlob().get_file("appgemdata","teams-pictures/Logo_GD_preto.png",f"{diretorio}/logo_{celular}{extensao}") 
            return extensao




def salvar_pdf_blob(diretoriopdf):
        try:
            ExecuteBlob().upload_file("reportclient",f"{celular}",diretoriopdf) 
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




def set_globals(data_conection,api_info,celphone,month,cami_logo,issendemail,**kwargs):
    global  data_conection_info,idintegration ,celular,mes,logo,isemail
    data_conection_info = data_conection
    idintegration = api_info
    celular= celphone
    mes= month
    logo= cami_logo
    isemail= issendemail
    if not all([idintegration,celular,mes]):
        logging.error("Global connection information is incomplete.")
        raise ValueError("All global connection information must be provided.")

    diretorio=criar_pasta_temp(celular)
    print(diretorio)

    extensao=get_logo(logo,celular,diretorio)
    gerar_pdf(int(mes), celular,idintegration,extensao,diretorio)
    
    
    # Remover o diretório após o uso
    shutil.rmtree(diretorio, ignore_errors=True)
    print(f"Diretório temporário {diretorio} removido.")


