
from dbpgconn import *

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
    ax.text(horizontal, altura -0.28, vlanterior_card + ' no mês anterior', fontsize=15, color='#938F9A')

    # Esconder os eixos
    # ax.set_xlim([0.026, 0.68])
    # ax.set_ylim([0.021, 0.58])
    ax.set_xlim([0.026, 0.68])
    ax.set_ylim([0.021, 0.58])
    
    ax.axis('off')

    

    # Salvar a imagem
    plt.savefig('relatorio_mensal/card'+nm_imagem+'.png', dpi=100,bbox_inches='tight', pad_inches=0)
    plt.close()
    #plt.show()


def ok():
    grafico_card('ticketmedio'+'97777','Ticket Médio',formatar_numerocasas_br(22552,0,1),formatar_numerocasas_br(2255,0,1),formatar_numerocasas_br(2222,1,0))

