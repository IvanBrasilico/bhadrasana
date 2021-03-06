"""Classes para reunir tarefas repetitivas com arquivos csv e planilhas.

Padrão do arquivo csv usado é o mais simples possível:
- Nomes de campo na primeira linha;
- Valores nas restantes;
- Único separador é sempre a vírgula;
- Fim de linha significa nova linha;
- Para comparações, retira espaços antes e depois do conteúdo das colunas.
"""
import csv
import glob
import io
import os
from copy import deepcopy
from zipfile import ZipFile

from ajna_commons.utils.sanitiza import ascii_sanitizar, sanitizar
from bhadrasana.conf import ENCODE, tmpdir


def muda_titulos_csv(csv_file, de_para_dict):
    """Apenas abre o arquivo e repassa para muda_titulos_lista."""
    with open(csv_file, 'r', encoding=ENCODE, newline='') as csvfile:
        reader = csv.reader(csvfile)
        result = [linha for linha in reader]
    # print(result)
    result = muda_titulos_lista(result, de_para_dict)
    # print(result)
    return result


def muda_titulos_lista(lista, de_para_dict, make_copy=True):
    """Muda titulos.

    Recebe um dicionário na forma titulo_old:titulo_new
    e muda a linha de titulo da lista.

    Passar copy=False para listas grandes faz a modificação in-line
    na lista original (muito mais rápido) modificando a lista original
    e retornando ela mesma. O padrão é copiar a lista e deixar
    intocada a lista original

    Args:
        plista: lista de listas representando a planilha a ter
        títulos modificados

        de_para_dict: dicionário titulo_antigo: titulo_novo

        copy: Se False, modifica original
    """
    if make_copy:
        lista = deepcopy(lista)
    for r in range(len(lista[0])):
        # Se título não está no de_para, retorna ele mesmo
        titulo = sanitizar(lista[0][r], norm_function=ascii_sanitizar)
        novo_titulo = de_para_dict.get(titulo, titulo)
        lista[0][r] = novo_titulo
    return lista


def retificar_linhas(lista, cabecalhos):
    """Retifica as linhas de arquivos com falhas."""
    # RETIFICAR LINHAS!!!!
    # Foram detectados arquivos com falha
    # (TABs a mais, ver notebook ExploraCarga)
    width_header = len(cabecalhos)
    for ind, linha in enumerate(lista):
        width_linha = len(linha)
        while width_linha > width_header:
            # print('Detectado problema linha: ', ind)
            # print('Largura linha:header', width_linha, ':', width_header)
            # Caso haja colunas "sobrando" na linha, retirar
            # uma coluna nula
            for index, col in enumerate(linha):
                if isinstance(col, str) and not col:
                    # print('Eliminando coluna: ', index)
                    linha.pop(index)
                    break
            width_linha -= 1


def sch_tocsv(sch, txt, dest_path=tmpdir):
    """Processa padrão sch (CARGA).

    Pega um arquivo txt, aplica os cabecalhos e a informação de um sch,
    e o transforma em um csv padrão.
    """
    cabecalhos = []
    for ind in range(len(sch)):
        if not isinstance(sch[ind], str):
            sch[ind] = str(sch[ind], ENCODE)
        linha = sch[ind]
        position_equal = linha.find('="')
        position_quote = linha.find('" ')
        position_col = linha.find('Col')
        if position_equal != -1 and position_col == 0:
            cabecalhos.append(linha[position_equal + 2:position_quote])
    campo = str(sch[0])[2:-3]
    filename = os.path.join(dest_path, campo + '.csv')
    with open(filename, 'w', encoding=ENCODE, newline='') as out:
        writer = csv.writer(out, quotechar='"', quoting=csv.QUOTE_ALL)
        # print('txt', txt)
        del txt[0]
        # print('txt', txt)
        writer.writerow(cabecalhos)
        # RETIFICAR LINHAS!!!!
        retificar_linhas(txt, cabecalhos)
        for row in txt:
            if row:
                writer.writerow(row)

    return filename
    # print(sch, txt)


def sch_processing(path, mask_txt='0.txt', dest_path=tmpdir):
    """Processa arquivos sch (CARGA).

    Processa lotes de extração que gerem arquivos txt csv e arquivos sch
    (txt contém os dados e sch descreve o schema), transformando-os em arquivos
    csv estilo "planilha", isto é, primeira linha de cabecalhos.

    Args:
        path: diretório ou arquivo .zip onde estão os arquivos .sch

    Obs:
        Não há procura recursiva, apenas no raiz do diretório

    """
    filenames = []
    if path.find('.zip') == -1:
        for sch in glob.glob(os.path.join(path, '*.sch')):
            sch_name = sch
            # print('****', sch_name)
            txt_name = glob.glob(os.path.join(
                path, '*' + os.path.basename(sch_name)[3:-4] + mask_txt))[0]
            with open(sch_name, encoding=ENCODE,
                      newline='') as sch_file, \
                    open(txt_name, encoding=ENCODE,
                         newline='') as txt_file:
                sch_content = sch_file.readlines()
                reader = csv.reader(txt_file, delimiter='\t')
                txt_content = [linha for linha in reader]
            csv_name = sch_tocsv(sch_content, txt_content, dest_path)
            filenames.append((csv_name, txt_name))
    else:
        with ZipFile(path) as myzip:
            info_list = myzip.infolist()
            # print('info_list ',info_list)
            for info in info_list:
                if info.filename.find('.sch') != -1:
                    sch_name = info.filename
                    # print('****', sch_name)
                    txt_search = sch_name[3:-4] + mask_txt
                    # print('****', txt_search)
                    for txtinfo in info_list:
                        if txtinfo.filename.find(txt_search) != -1:
                            txt_name = txtinfo.filename
                            with myzip.open(sch_name) as sch_file:
                                sch_content = io.TextIOWrapper(
                                    sch_file,
                                    encoding=ENCODE, newline=''
                                ).readlines()
                            with myzip.open(txt_name) as txt_file:
                                txt_io = io.TextIOWrapper(
                                    txt_file,
                                    encoding=ENCODE, newline=''
                                )
                                reader = csv.reader(txt_io, delimiter='\t')
                                txt_content = [linha for linha in reader]
                                # print('txt_content', txt_content)
                    csv_name = sch_tocsv(sch_content, txt_content, dest_path)
                    filenames.append((csv_name, txt_name))
    return filenames
