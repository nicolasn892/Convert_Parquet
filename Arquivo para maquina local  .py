import urllib3
from urllib3.util.retry import Retry
from zeep import Client
import requests
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from zeep.transports import Transport
from xml.etree import ElementTree as ET
import json
import re
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import itertools
import os
import pandas as pd


# Suprimir avisos de insegurança do urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# FUNÇÃO - IMPORTA ARQUIVO JSON
def importa_json_parametros():
    # Carrega o conteúdo do arquivo JSON
    with open(caminho_parametros, 'r', encoding='utf-8') as arquivo:
        dados = json.load(arquivo)
    return dados


# FUNÇÃO - REMOVE CARACTERES NÃO PERMITIDOS NO XML
def remover_caracteres_invalidos(xml_str):
    xml_str = re.sub(r'[^\x09\x0A\x0D\x20-\x7F\xA0-\xFF]', '', xml_str)
    return xml_str


# FUNÇÃO - INICIA SESSÃO NO SERVIÇO SOAP
def iniciar_sessao(client, usuario, senha):
    response = client.service.IniciarSessao(usuario=usuario, senha=senha)
    return response


# FUNÇÃO - ENCERRA SESSÃO NO SERVIÇO SOAP
def encerrar_sessao(client, session_id):
    client.service.EncerrarSessao(session=session_id)


# FUNÇÃO - REMOVE QUEBRAS DE LINHA E ESPAÇOS EXCESSIVOS NO XML
def remover_quebras_de_linha_dos_textos(element):
    # Normaliza o texto do elemento (se existir)
    if element.text:
        # Remove quebras de linha, espaços iniciais e finais
        element.text = re.sub(r'\s+', ' ', element.text).strip()
    # Percorre os filhos do elemento recursivamente
    for child in element:
        remover_quebras_de_linha_dos_textos(child)


# FUNÇÃO - FAZ REQUISIÇÃO PAGINADA E EXTRAI PROCESSOS
def fazer_requisicao_paginada(client, urls, session_id, pagina, escreve_arquivo, contextos, no_dos_registros, nome_arquivos):

    # Filtro Web Service utilizado na requisição - Obrigatório!
    filtro = '{gA:[{c:{ctx:"' + contextos + '",f:"ULTIMA_MODIFICACAO",o:"nnl"}}]}'

    # Envelope SOAP para a requisição
    envelope_soap = f"""
    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:web="http://webservices.projurisweb.virtuem.com.br">
        <soapenv:Header/>
        <soapenv:Body>
            <web:session>{session_id}</web:session>
            <web:LazyListaRequest>
                <relatorio-customizado>
                    <filtro>
                        {filtro}
                    </filtro>
                    <start>{pagina}</start>
                </relatorio-customizado>
            </web:LazyListaRequest>
        </soapenv:Body>
    </soapenv:Envelope>
    """

    # Cabeçalhos da requisição
    headers = {
        'Content-Type': 'text/xml',
        'SOAPAction': 'Lista'
    }
    # Envia a requisição SOAP
    response = requests.post(urls, data=envelope_soap, headers=headers, verify=False)

    # Processa a resposta SOAP
    if response.status_code == 200:

        # Extrai o conteúdo da resposta
        resposta_xml = response.content.decode('ISO-8859-1')
        
        # Remove caracteres inválidos
        resposta_xml = remover_caracteres_invalidos(resposta_xml)
        
        try:
            root = ET.fromstring(resposta_xml)
            remover_quebras_de_linha_dos_textos(root)  # Remove quebras de linha dos textos sem alterar a estrutura
            
            if escreve_arquivo:
                with open(f'{caminho_arquivos}dados_{nome_arquivos}_processo.xml', 'a', encoding='utf-8') as arquivo:
                    for processo in root.iter(no_dos_registros):  
                        # Serializa o elemento de forma legível
                        xml_str = ET.tostring(processo, encoding='unicode', method='xml')
                        
                        # Garantir que o fechamento seja em uma nova linha
                        if not xml_str.endswith('\n'):
                            xml_str += '\n'
                        
                        arquivo.write(xml_str)
                        
        # Mostra o erro e salva os registros com caracteres inválidos em um arquivo de erro
        except ET.ParseError as e:
            print(f"\n Erro ao analisar o XML: {e}")
            with open(f'{caminho_arquivos}erro_xml_response_{nome_arquivos}.xml', 'a', encoding='ISO-8859-1') as arquivo:
                arquivo.write(resposta_xml)
            return None
                        
        return resposta_xml
    else:
        print(f"Erro ao fazer requisição: {response.status_code}")
        print(response.content.decode('ISO-8859-1'))


# FUNÇÃO - ADICIONA PROCESSOS E FAZ A PAGINAÇÃO
def adiciona_processos(client, urls, session_id, contextos, no_dos_registros, nome_arquivos):
    # Faz a primeira requisição para obter a quantidade total de registros
    resposta_xml = fazer_requisicao_paginada(client, urls, session_id, 1, False, contextos, no_dos_registros, nome_arquivos)
    
    # Verifica se a resposta foi recebida corretamente
    if resposta_xml is None:
        print("Erro ao receber a resposta da requisição inicial.")
        return

    # Converte a resposta para uma árvore XML
    try:
        root = ET.fromstring(resposta_xml)
    except ET.ParseError as e:
        print(f"Erro ao parsear a resposta XML: {e}")
        return
    
    # Tenta encontrar o elemento 'total'
    primeiro_elemento = next(root.iter('total'), None)
    
    if primeiro_elemento is None:
        print(f'\n Arquivo ' + nome_arquivos + ' não há registros.')
        return

    # Se o elemento foi encontrado, acessa o valor de 'total'
    try:
        total = int(primeiro_elemento.text)
        print("\n Total de registros em " + nome_arquivos + ": " + str(total))
    except AttributeError:
        print("Erro ao acessar o texto do elemento 'total'. O elemento está vazio ou inválido.")
        return
    
    # Define o número de páginas com base no total de registros
    n_paginas = range(0, int(total / 50) + 1)
    paginacoes = [idx * 50 for idx in n_paginas]
    
    # Imprime o número de CPUs disponíveis
    #print(f"Número de CPUs disponíveis: {os.cpu_count()}")
    
    # Processamento paralelo usando ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        # Passa todos os parâmetros corretos usando itertools.starmap ou itertools.zip
        responses = list(tqdm(executor.map(
            fazer_requisicao_paginada,
            itertools.repeat(client),
            itertools.repeat(urls),
            itertools.repeat(session_id),
            paginacoes,
            itertools.repeat(True),
            itertools.repeat(contextos),
            itertools.repeat(no_dos_registros),
            itertools.repeat(nome_arquivos)
        ), total=len(paginacoes)))
    
    print(f"Total de paginações processadas em {nome_arquivos}: {len(responses)}")


# FUNÇÃO - PARSEA ARQUIVO XML
def parse_multiple_xml(xml_file, no_dos_registros):
    with open(xml_file, 'r', encoding='utf-8') as file:
        content = file.read()
    
    xml_documents = content.split(f'</{no_dos_registros}>')
    
    records = []
    for xml_document in tqdm(xml_documents, desc="Conversão dos XML's em CSV's"):
        if xml_document.strip():
            xml_document = xml_document + f'</{no_dos_registros}>'
            try:
                root = ET.fromstring(xml_document)
                record = {child.tag: child.text for child in root}
                records.append(record)
            except ET.ParseError:
                continue
    
    return pd.DataFrame(records)


# FUNÇÃO - CONVERTE ARQUIVO XML PARA CSV
def processar_arquivos(nomes_arquivos, no_dos_registros, caminho_arquivos):
    for nome_arquivo in nomes_arquivos:
        xml_file = os.path.join(caminho_arquivos, f'dados_{nome_arquivo}_processo.xml')
        csv_file = os.path.join(caminho_arquivos, f'dados_{nome_arquivo}_processo.csv')

        if os.path.exists(xml_file):
            df = parse_multiple_xml(xml_file, no_dos_registros)
            df.to_csv(csv_file, sep=';', index=False, encoding='utf-8')
            print(f"Arquivo CSV '{nome_arquivo}' gerado com sucesso.")
        else:
            print(f"Arquivo XML '{nome_arquivo}' não encontrado.")


if __name__ == '__main__':
    caminho_parametros = 'C:/Jurimetria/Parametros.json'
    caminho_arquivos = 'C:/Jurimetria/Arquivos_Gerados/'

    # ATRIBUIÇÃO DOS DADOS DO JSON ÀS VARIÁVEIS
    dados = importa_json_parametros()
    usuario = dados['usuario']
    senha = dados['senha']
    WSDL = dados['WSDL']
    urls = dados['URL_SOAP'] # Pega as URLs que são criados no SOAP UI
    no_dos_registros = dados['no_dos_registros'] # Nó do XML que separa cada registro
    nomes_arquivos = dados['nome_arquivo'] 
    contextos = dados['contexto'] # Dados do Filtro Web Service

    # CRIAÇÃO DE SESSÃO QUE IGNORA A VERIFICAÇÃO DO CERTIFICADO SSL
    sessao = Session()
    sessao.verify = False  # Ignora verificação do certificado
    # Configuração de retry para falhas de conexão
    retry = Retry(
        total=15,  # Número total de tentativas
        backoff_factor=0.3,  # Fator de espera exponencial
        status_forcelist=(500, 502, 503, 504),  # Códigos de status que acionam a retentativa
    )
    timeout_seconds = 120  # Tempo de conexão desejado em segundos

    # Criação do transporte utilizando a sessão personalizada
    transporte = Transport(session=sessao)

    # CRIA UM ÍNDICE PARA PASSAR EM CADA WSDL CORRETAMENTE
    for i in range(len(WSDL)):

        # Criação do cliente SOAP com o transporte personalizado
        client = Client(WSDL[i], transport=transporte)

        # Iniciando a sessão no serviço SOAP
        session_id = iniciar_sessao(client, usuario, senha)
        

        # Incrementa dados das novas paginações
        with open(f'{caminho_arquivos}dados_{nomes_arquivos[i]}_processo.xml', 'a', encoding='utf-8') as arquivo:
            arquivo.write('')

        adiciona_processos(client, urls[i], session_id, contextos[i], no_dos_registros[i], nomes_arquivos[i])
        
        # Encerra a sessão no serviço SOAP
        encerrar_sessao(client, session_id)
        
         # Converte o XML gerado para CSV logo após o processamento
        processar_arquivos([nomes_arquivos[i]], no_dos_registros[i], caminho_arquivos)
