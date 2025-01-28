import os
from cryptography.fernet import Fernet
import pandas as pd
import re
import time
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv
# import pyodbc // conecta com o banco de dados, porém incompatível com a versão instalada do pandas
from sqlalchemy import create_engine # Recomendada pela comunidade para consultas SQL+Pandas
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

count = 0
# arquivo de black_list 
black_file = r"C:\projetos\RPA_COBRANCA\Black_list\list.txt"
# Carrega o arquivo .env
load_dotenv()
# Carregar chave da variável de ambiente
key = os.getenv("CHAVE_RPA").encode()
cipher_suite = Fernet(key)
#carregar variaveis de conexão
smptp_server = os.getenv("SERVER_EMAIL")
smtp_port =  os.getenv("PORTA")
address_mail = os.getenv("EMAIL")
password_mail = os.getenv("PASSWORD_MAIL")
# caminho para o logo
logo_path = r"C:\projetos\RPA_COBRANCA\logo\logoRobustec.png"
# Configurações do servidor de e-mail
SMTP_SERVER = cipher_suite.decrypt(smptp_server.encode()).decode() 
SMTP_PORT = int(cipher_suite.decrypt(smtp_port.encode()).decode()) 
EMAIL_ADDRESS = cipher_suite.decrypt(address_mail.encode()).decode()  # Substitua pelo seu e-mail
EMAIL_PASSWORD = cipher_suite.decrypt(password_mail.encode()).decode()   # Substitua pela sua senha

##função para enviar emails de forma simples 
def send_basic(mensagem, assunto, destino):
    time.sleep(15)
    try:
        # Configuração do e-mail
        msg = MIMEMultipart()
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = destino
        msg["Subject"] = assunto
        msg.attach(MIMEText(mensagem, "plain"))

        # Envio do e-mail
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
            print(f"E-mail enviado para {destino}")
    except Exception as e:
        print(f"Erro ao enviar e-mail para {destino}: {e}")

def send_advance(destino, assunto, mensagem, periodo):
    time.sleep(15)
    try:
        # Configuração do e-mail
        msg = MIMEMultipart()
        msg["From"] = EMAIL_ADDRESS
        msg["To"] = destino
        msg["Subject"] = assunto
        msg.attach(MIMEText(mensagem, "html"))
       
        # Anexar imagem como inline (CID)
        with open(logo_path, "rb") as img_file:
            logo = MIMEImage(img_file.read())
            logo.add_header("Content-ID", "<logo>")
            logo.add_header("Content-Disposition", "inline", filename="logoRobustec.png")
            msg.attach(logo)

        # Envio do e-mail
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
            server.send_message(msg)
            print(f"E-mail enviado para {{destino}}")
        arquivo = rf'C:\projetos\RPA_COBRANCA\logs\ENVIADOS\LOG_Periodo_{periodo}.txt'
        with open(arquivo, 'a') as arquivo:
            arquivo.write(f'Email enviado com sucesso!\n')

            # Retornar True em caso de sucesso
        return True
    except Exception as e:
        arquivo = rf'C:\projetos\RPA_COBRANCA\logs\ERROS\LOG_Periodo_{periodo}.txt'
        with open(arquivo, 'a') as arquivo:
            arquivo.write(f'Erro {e} ao tentar realizar envio de e-mail.\n')
            arquivo.write(f'Para o destino {destino}\n')
        # Retornar False em caso de erro
        return False
    

    
# Função para minificar HTML
def minify_html(html):
    html = re.sub(r'\n+', '', html)  # Remove quebras de linha
    html = re.sub(r'\s{2,}', ' ', html)  # Remove múltiplos espaços
    html = re.sub(r'<!--.*?-->', '', html)  # Remove comentários
    return html.strip()

#CREDENCIAIS PARA CONEXÃO COM DATABASE
srv =  os.getenv("SERVER")
db =  os.getenv("DATABASE")
usr =  os.getenv("USUARIO")
pss =  os.getenv("SENHA_CONN")

credenciais = {
   'server': f'{srv}',
    'database': f'{db}', 
    'usuario': f'{usr}', 
    'senha': f'{pss}'
}
# Descriptografar credenciais
server = cipher_suite.decrypt(credenciais["server"].encode()).decode()
database = cipher_suite.decrypt(credenciais["database"].encode()).decode()
user = cipher_suite.decrypt(credenciais["usuario"].encode()).decode()
password = cipher_suite.decrypt(credenciais["senha"].encode()).decode()
#String de conexão com banco
conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={user};PWD={password}'
engine = create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver=SQL+Server")
#Tenta consultar
try:
# Consulta SQL
    consulta_sql = "SELECT * FROM VW_BI_RPA_COBRANCA "
    df_inadimplentes = pd.read_sql_query(consulta_sql, engine)
    
    #print(df_inadimplentes)
    df_inadimplentes['EMAILS'] = df_inadimplentes['EMAILS'].str.replace(';', '', regex=False)  # Remove todos os ';'
    df_inadimplentes['EMAILS'] = df_inadimplentes['EMAILS'].str.strip()  # Remove espaços extras no início e no final

    #  Substituir múltiplos espaços por uma vírgula
    df_inadimplentes['EMAILS'] = df_inadimplentes['EMAILS'].str.replace(r'\s+', ', ', regex=True)
    
    # Variaveis auxiliares
    hoje = datetime.now()
    intervalo_5 = timedelta(days=5)
    intervalo_1 = timedelta(days=1)
    inadimplentes_sem_email = pd.DataFrame()
    #Verificar datas e destinos
    grupo_envio_5 = {}
    grupo_envio_1 = {}
    grupo_envio_today = {}
    grupo_passou_tres = {}
    for index, row in df_inadimplentes.iterrows():
        try:
            # 1. Ler o arquivo TXT e criar a black_lista
            with open(black_file, "r") as file:
                content = file.read()
                # Remover espaços em branco e criar uma lista de códigos
            black_lista = [code.strip() for code in content.split(",")]
            data_vencimento = pd.to_datetime(row["DATA_VENCIMENTO"])
            cd_empresa = row["Cd_empresa"]
            if cd_empresa not in black_lista:
                if  data_vencimento.date() == (hoje + intervalo_5).date():
                    # print(data_vencimento).date()
                    # print(hoje + intervalo_5).date()
                    # 
                    if not row["EMAILS"] or pd.isna(row["EMAILS"]) or str(row["EMAILS"]).strip() == "":
                        # Adiciona a linha ao DataFrame de inadimplentes sem e-mail
                        inadimplentes_sem_email = pd.concat([inadimplentes_sem_email, pd.DataFrame([row])], ignore_index=True)
                    else:
                        # print()
                        # print(
                        #     'Nome: ',str(row["EMPRESA"]).strip(),
                        #     'CPF/CNPJ: ',str(row["Cnpj_cpf"]).strip(),
                        #     'Fone: ',str(row["Fone"]).strip(),
                        #     'Email:',str(row["EMAILS"]).strip(),
                        #     'NF: ',str(row["NF"]).strip(),
                        #     'Fatura: ',str(row["FATURA"]).strip(),
                        #     'Boleto: ',str(row["BOLETO"]).strip(),)
                        if row["Cnpj_cpf"] in grupo_envio_5:
                            # Se o Cnpj_cpf já estiver no dicionário, verifica as NFs
                            if row["NF"] not in grupo_envio_5[row["Cnpj_cpf"]]['NF']:
                                # Se a NF ainda não estiver no dicionário, adiciona com FATURA e BOLETO
                                grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]] = {
                                    "FATURA": [str(row["FATURA"]).strip()],
                                    "SALDO": [str(row["SALDO"]).strip()],
                                    "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                    "BOLETO": [str(row["BOLETO"]).strip()]

                                }
                            else:
                                # Se a NF já existir, adiciona FATURA e BOLETO (evitando duplicatas)
                                if str(row["FATURA"]).strip() not in grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"]:
                                    grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"].append(str(row["FATURA"]).strip())
                                if str(row["SALDO"]) not in grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"]:
                                    grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"].append(str(row["SALDO"]).strip())
                                if str(row["CD_LANCAMENTO"]) not in grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"]:
                                    grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"].append(str(row["CD_LANCAMENTO"]).strip())
                                if str(row["BOLETO"]).strip() not in grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"]:
                                    grupo_envio_5[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"].append(str(row["BOLETO"]).strip())
                        else:
                            # Se o Cnpj_cpf não estiver no dicionário, cria uma nova entrada
                            grupo_envio_5[row["Cnpj_cpf"]] = {
                                "NOME": str(row["EMPRESA"]).strip(),
                                "EMAILS": [str(row["EMAILS"]).strip()],
                                "FONE": str(row["Fone"]).strip(),
                                "NF": {
                                    row["NF"]: {
                                        "FATURA": [str(row["FATURA"]).strip()],
                                        "SALDO": [str(row["SALDO"]).strip()],
                                        "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                        "BOLETO": [str(row["BOLETO"]).strip()]
                                    }
                                }
                            }
                        #print()
                elif  data_vencimento.date() == (hoje + intervalo_1).date():
                    if not row["EMAILS"] or pd.isna(row["EMAILS"]) or str(row["EMAILS"]).strip() == "":
                        # Adiciona a linha ao DataFrame de inadimplentes sem e-mail
                        inadimplentes_sem_email = pd.concat([inadimplentes_sem_email, pd.DataFrame([row])], ignore_index=True)
                    else:
                        # print()
                        # print(
                        #     'Nome: ',str(row["EMPRESA"]).strip(),
                        #     'CPF/CNPJ: ',str(row["Cnpj_cpf"]).strip(),
                        #     'Fone: ',str(row["Fone"]).strip(),
                        #     'Email:',str(row["EMAILS"]).strip(),
                        #     'NF: ',str(row["NF"]).strip(),
                        #     'Fatura: ',str(row["FATURA"]).strip(),
                        #     'Boleto: ',str(row["BOLETO"]).strip(),)
                        if row["Cnpj_cpf"] in grupo_envio_1:
                            # Se o Cnpj_cpf já estiver no dicionário, verifica as NFs
                            if row["NF"] not in grupo_envio_1[row["Cnpj_cpf"]]['NF']:
                                # Se a NF ainda não estiver no dicionário, adiciona com FATURA e BOLETO
                                grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]] = {
                                    "FATURA": [str(row["FATURA"]).strip()],
                                    "SALDO": [str(row["SALDO"]).strip()],  
                                    "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],  
                                    "BOLETO": [str(row["BOLETO"]).strip()]
                                }
                            else:
                                # Se a NF já existir, adiciona FATURA e BOLETO (evitando duplicatas)
                                if str(row["FATURA"]).strip() not in grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"]:
                                    grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"].append(str(row["FATURA"]).strip())
                                if str(row["SALDO"]) not in grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"]:
                                    grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"].append(str(row["SALDO"]).strip())
                                if str(row["CD_LANCAMENTO"]) not in grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"]:
                                    grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"].append(str(row["CD_LANCAMENTO"]).strip())
                                if str(row["BOLETO"]).strip() not in grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"]:
                                    grupo_envio_1[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"].append(str(row["BOLETO"]).strip())
                        else:
                            # Se o Cnpj_cpf não estiver no dicionário, cria uma nova entrada
                            grupo_envio_1[row["Cnpj_cpf"]] = {
                                "NOME": str(row["EMPRESA"]).strip(),
                                "EMAILS": [str(row["EMAILS"]).strip()],
                                "FONE": str(row["Fone"]).strip(),
                                "NF": {
                                    row["NF"]: {
                                        "FATURA": [str(row["FATURA"]).strip()],
                                        "SALDO": [str(row["SALDO"]).strip()],
                                        "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                        "BOLETO": [str(row["BOLETO"]).strip()]
                                    }
                                }
                            }
                        #print()
                elif  data_vencimento.date() == hoje.date():
                    if not row["EMAILS"] or pd.isna(row["EMAILS"]) or str(row["EMAILS"]).strip() == "":
                        # Adiciona a linha ao DataFrame de inadimplentes sem e-mail
                        inadimplentes_sem_email = pd.concat([inadimplentes_sem_email, pd.DataFrame([row])], ignore_index=True)
                    else:
                        # print()
                        # print(
                        #     'Nome: ',str(row["EMPRESA"]).strip(),
                        #     'CPF/CNPJ: ',str(row["Cnpj_cpf"]).strip(),
                        #     'Fone: ',str(row["Fone"]).strip(),
                        #     'Email:',str(row["EMAILS"]).strip(),
                        #     'NF: ',str(row["NF"]).strip(),
                        #     'Fatura: ',str(row["FATURA"]).strip(),
                        #     'Boleto: ',str(row["BOLETO"]).strip(),)
                        if row["Cnpj_cpf"] in grupo_envio_today:
                            # Se o Cnpj_cpf já estiver no dicionário, verifica as NFs
                            if row["NF"] not in grupo_envio_today[row["Cnpj_cpf"]]['NF']:
                                # Se a NF ainda não estiver no dicionário, adiciona com FATURA e BOLETO
                                grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]] = {
                                "FATURA": [str(row["FATURA"]).strip()],
                                    "SALDO": [str(row["SALDO"]).strip()],
                                    "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                    "BOLETO": [str(row["BOLETO"]).strip()]
                                }
                            else:
                                # Se a NF já existir, adiciona FATURA e BOLETO (evitando duplicatas)
                                if str(row["FATURA"]).strip() not in grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"]:
                                    grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"].append(str(row["FATURA"]).strip())
                                if str(row["SALDO"]) not in grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"]:
                                    grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"].append(str(row["SALDO"]).strip())
                                if str(row["CD_LANCAMENTO"]) not in grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"]:
                                    grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"].append(str(row["CD_LANCAMENTO"]).strip())
                                if str(row["BOLETO"]).strip() not in grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"]:
                                    grupo_envio_today[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"].append(str(row["BOLETO"]).strip())
                        else:
                            # Se o Cnpj_cpf não estiver no dicionário, cria uma nova entrada
                            grupo_envio_today[row["Cnpj_cpf"]] = {
                                "NOME": str(row["EMPRESA"]).strip(),
                                "EMAILS": [str(row["EMAILS"]).strip()],
                                "FONE": str(row["Fone"]).strip(),
                                "NF": {
                                    row["NF"]: {
                                        "FATURA": [str(row["FATURA"]).strip()],
                                        "SALDO": [str(row["SALDO"]).strip()],
                                        "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                        "BOLETO": [str(row["BOLETO"]).strip()]
                                    }
                                }
                            }
                elif  (hoje - data_vencimento).days >= 3:
                    if not row["EMAILS"] or pd.isna(row["EMAILS"]) or str(row["EMAILS"]).strip() == "":
                        # Adiciona a linha ao DataFrame de inadimplentes sem e-mail
                        inadimplentes_sem_email = pd.concat([inadimplentes_sem_email, pd.DataFrame([row])], ignore_index=True)
                    else:
                        val = (hoje - data_vencimento).days
                        if val % 2 == 0:
                            continue
                        else:
                            # print()
                            # print(
                            #     'Nome: ',str(row["EMPRESA"]).strip(),
                            #     'CPF/CNPJ: ',str(row["Cnpj_cpf"]).strip(),
                            #     'Fone: ',str(row["Fone"]).strip(),
                            #     'Email:',str(row["EMAILS"]).strip(),
                            #     'NF: ',str(row["NF"]).strip(),
                            #     'Fatura: ',str(row["FATURA"]).strip(),
                            #     'Boleto: ',str(row["BOLETO"]).strip(),)
                            if row["Cnpj_cpf"] in grupo_passou_tres:
                                # Se o Cnpj_cpf já estiver no dicionário, verifica as NFs
                                if row["NF"] not in grupo_passou_tres[row["Cnpj_cpf"]]['NF']:
                                    # Se a NF ainda não estiver no dicionário, adiciona com FATURA e BOLETO
                                    grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]] = {
                                        "FATURA": [str(row["FATURA"]).strip()],
                                        "SALDO": [str(row["SALDO"]).strip()],
                                        "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                        "BOLETO": [str(row["BOLETO"]).strip()]
                                    }
                                else:
                                    # Se a NF já existir, adiciona FATURA e BOLETO (evitando duplicatas)
                                    if str(row["FATURA"]).strip() not in grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"]:
                                        grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["FATURA"].append(str(row["FATURA"]).strip())
                                    if str(row["SALDO"]).strip() not in grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"]:
                                        grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["SALDO"].append(str(row["SALDO"]).strip())
                                    if str(row["CD_LANCAMENTO"]).strip() not in grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"]:
                                        grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["CD_LANCAMENTO"].append(str(row["CD_LANCAMENTO"]).strip())
                                    if str(row["BOLETO"]).strip() not in grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"]:
                                        grupo_passou_tres[row["Cnpj_cpf"]]['NF'][row["NF"]]["BOLETO"].append(str(row["BOLETO"]).strip())
                            else:
                                # Se o Cnpj_cpf não estiver no dicionário, cria uma nova entrada
                                grupo_passou_tres[row["Cnpj_cpf"]] = {
                                    "NOME": str(row["EMPRESA"]).strip(),
                                    "EMAILS": [str(row["EMAILS"]).strip()],
                                    "FONE": str(row["Fone"]).strip(),
                                    "NF": {
                                        row["NF"]: {
                                        "FATURA": [str(row["FATURA"]).strip()],
                                        "SALDO": [str(row["SALDO"]).strip()],
                                        "CD_LANCAMENTO": [str(row["CD_LANCAMENTO"]).strip()],
                                        "BOLETO": [str(row["BOLETO"]).strip()]
                                        }
                                    }
                                }
            else:
                print(cd_empresa)
        except Exception as e:
            import traceback
            print("Ocorreu um erro ao processar a linha:")
            print(f"Dados atuais da linha: {row}")  # Mostra os dados que estavam sendo processados
            print(f"Erro encontrado: {e}")          # Mostra a mensagem de erro
            traceback.print_exc()
            arquivo = rf'C:\projetos\RPA_COBRANCA\logs\ERROS\LOG_LINHA_362.txt'
            with open(arquivo, 'a') as arquivo:
                arquivo.write(f'Erro {e} ao tentar realizar envio de e-mail.\n')
            
            #print()
    # nessa etapa do código os dicionários já esão criados, com os dados organizados
    # e prontos para serem utilizados para enviar os e-mails
    # print(inadimplentes_sem_email)
    # print(grupo_passou_tres)
    # 
    ################ FALTAM 5 DIAS ###############################################
    print('verificando dicionário de 5 dias')
    try:
        error = pd.DataFrame(columns=["EMPRESA", "CD_LANCAMENTO","FONE"])
        success = pd.DataFrame(columns=["EMPRESA", "CD_LANCAMENTO","FONE"])
        status = True
        for cnpj, info in grupo_envio_5.items():
            df_temp = pd.DataFrame(columns=["EMPRESA", "CD_LANCAMENTO","FONE"])
            nome = info["NOME"]
            fone = info["FONE"]
            # Construção dinâmica da tabela
            html_table = f"""
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-size: 14px; table-layout: fixed;">
                <thead>
                    <tr style="background-color: #fd8e45; color: #ffffff; ">
                        <th colspan="3" style="text-align: center; padding: 8px;">ID: {info['NOME']} </th>
                    </tr>
                    <tr style="background-color: #fd8e45; color: #ffffff; ">
                        <th colspan="3" style="text-align: center; padding: 8px;">DOCUMENTO: {cnpj}</th>
                    </tr>
                    <tr style="background-color: #fd8e45; color: #ffffff;">
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">NF|LANÇAMENTO</th>
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">VALOR PRINCIPAL</th>
                        <th style="border: 1px solid #ddd;text-align: center;"width: 33%;>PARCELA</th>
                    </tr>
                </thead>
                <tbody>
            """
            #print(f"CNPJ_CPF: {cnpj}")
            #print(info['FONE'])
            # Loop para adicionar as linhas dinamicamente
            for nf, detalhes in info['NF'].items():
                faturas = ", ".join(detalhes['FATURA'])
                saldo = ", ".join(detalhes['SALDO'])
                # Primeiro, converta o saldo para float (ou Decimal, se necessário)
                saldo = saldo.split(",")[0].strip()  # Pega o primeiro valor, removendo espaços extras
                saldo = float(saldo.replace(",", "."))    # Se o saldo estiver com vírgulas, converta para ponto
                valor_formatado = f"R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                cd_lancamento = ", ".join(detalhes['CD_LANCAMENTO'])
                boletos = ", ".join(detalhes['BOLETO'])
                #adicionar linha no df
                nova_linha = {"EMPRESA":nome, "CD_LANCAMENTO":cd_lancamento,"FONE": fone}
                df_temp = pd.concat([df_temp, pd.DataFrame([nova_linha])], ignore_index=True)
                if ',' in boletos:
                    boletos = "UNIFICADO"
                if boletos == "UNIFICADO":
                    cd_lancamento = cd_lancamento.split(",")[0].strip()
                    nf = cd_lancamento
                    if nf.endswith('.0'):
                        nf = nf[:-2]

                html_table += f"""
                <tr>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{nf}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{valor_formatado}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center; ">{boletos}° </td>
                </tr>
                """
            # Fechando a tabela
            html_table += """
            <tr style="background-color: #27573d; color: #fff; margin: 0; text-align: center; padding: 8px; border-radius: 0 0 8px 8px; position: relative; bottom: 0;">
                    <th colspan="3" >  <p style="margin: 0; font-size: 12px;">Dúvidas? Entre em contato:</p>
                        <a href="https://wa.me/555433592230" style="color: #fff; text-decoration: none;">WhatsApp 54-3359-2230</a> | 
                        <a href="mailto:cobranca@robustec.com.br" style="color: #fff; text-decoration: none;"> cobranca@robustec.com.br</a>
                </th>
                    </tr>
                <tr>
                    <th colspan="3" >  <p style="color: #f98114; text-align: center; font-size: 13px;">
                        <strong> Caso já tenha sido realizado pagamento, favor desconsiderar </strong>
                        Atenção: Fique atento aos possíveis boletos fraudados. Nossos e-mails sempre terão robustec.com.br após o @
                    </p>
                    </th>
                    </tr>
            </tbody>
            </table>
            """
            # Resultado final
            # print(html_table)
            # Separar e-mails
            emails = info['EMAILS']
            if isinstance(emails, list):  # Verificar se é uma lista
                emails = ", ".join(emails)  # Juntar os e-mails com vírgula
            email_list = emails.split(",")  # Separar os e-mails por vírgula
            # Corpo do e-mail em HTML
            # Corpo do e-mail com CSS inline
            corpo_html = f"""
            <div style="max-width: 600px; margin: 0 auto; font-family: Arial, sans-serif; background-color: #fff; border: 1px solid #ddd; border-radius: 8px;">
                <div style="text-align: center; padding: 5px;">
                    <img src="cid:logo" alt="Logo Robustec" style="max-width: 150px;">
                </div>
                <div style="padding: 10px;">
                    <h1 style="color: #f98114; text-align: center; font-size: 20px;">Aviso Importante!</h1>
                    <p style="font-size: 14px; color: #333;overflow-wrap: break-word;">
                        Prezado cliente,<br>
                        informamos que você possui a(s) seguinte(s) NF(s) com data de vencimento nos próximos 5 dias.\n
                        </p>
                {html_table}
                </div>
                
            </div>
            """
            # Minificar o HTML
            corpo_html_minificado = minify_html(corpo_html)
            #print("E-MAILS:")
            for email in email_list:
                send_to = email.strip()  # Remove espaços antes/depois do e-mail
                send_to = send_to.lower() # garantir que toda a escrita de email esteja em minusculas
                status = send_advance(send_to, 'ROBUSTEC AVISO IMPORTANTE', corpo_html_minificado, hoje.date())
            if status == True:
               success = pd.concat([success, df_temp], ignore_index=True)
            else:
               error = pd.concat([error, df_temp], ignore_index=True)
         
        ############### FALTA 1 DIA ############################################
        print('verificando dicionário de 1 dia')
        for cnpj, info in grupo_envio_1.items():
            df_temp = pd.DataFrame(columns=["EMPRESA", "CD_LANCAMENTO","FONE"])
            nome = info["NOME"]
            fone = info["FONE"]
            
            # Construção dinâmica da tabela
            html_table = f"""
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-size: 14px; table-layout: fixed;">
                <thead>
                    <tr style="background-color: #fd8e45; color: #ffffff; ">
                        <th colspan="3" style="text-align: center; padding: 8px;">ID: {info['NOME']} | DOCUMENTO: {cnpj}</th>
                    </tr>
                    <tr style="background-color: #fd8e45; color: #ffffff;">
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">NF|LANÇAMENTO</th>
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">VALOR PRINCIPAL</th>
                        <th style="border: 1px solid #ddd;text-align: center;"width: 33%;>PARCELA</th>
                    </tr>
                </thead>
                <tbody>
            """
            #print(f"CNPJ_CPF: {cnpj}")
            #print(info['FONE'])
            # Loop para adicionar as linhas dinamicamente
            for nf, detalhes in info['NF'].items():
                faturas = ", ".join(detalhes['FATURA'])
                saldo = ", ".join(detalhes['SALDO'])
                # Primeiro, converta o saldo para float (ou Decimal, se necessário)
                saldo = saldo.split(",")[0].strip()  # Pega o primeiro valor, removendo espaços extras
                saldo = float(saldo.replace(",", "."))    # Se o saldo estiver com vírgulas, converta para ponto
                valor_formatado = f"R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                cd_lancamento = ", ".join(detalhes['CD_LANCAMENTO'])
                boletos = ", ".join(detalhes['BOLETO'])
                #adicionar linha no df
                nova_linha = {"EMPRESA":nome, "CD_LANCAMENTO":cd_lancamento,"FONE": fone}
                df_temp = pd.concat([df_temp, pd.DataFrame([nova_linha])], ignore_index=True)
                if ',' in boletos:
                    boletos = "UNIFICADO"
                if boletos == "UNIFICADO":
                    cd_lancamento = cd_lancamento.split(",")[0].strip()
                    nf = cd_lancamento
                    if nf.endswith('.0'):
                        nf = nf[:-2]
                html_table += f"""
                <tr>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{nf}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{valor_formatado}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center; ">{boletos}° </td>
                </tr>
                """
        # Fechando a tabela
            html_table += """
            <tr style="background-color: #27573d; color: #fff; margin: 0; text-align: center; padding: 8px; border-radius: 0 0 8px 8px; position: relative; bottom: 0;">
                    <th colspan="3" >  <p style="margin: 0; font-size: 12px;">Dúvidas? Entre em contato:</p>
                        <a href="https://wa.me/555433592230" style="color: #fff; text-decoration: none;">WhatsApp 54-3359-2230</a> | 
                        <a href="mailto:cobranca@robustec.com.br" style="color: #fff; text-decoration: none;"> cobranca@robustec.com.br</a>
                </th>
                    </tr>
                <tr>
                    <th colspan="3" >  <p style="color: #f98114; text-align: center; font-size: 13px;">
                        <strong> Caso já tenha sido realizado pagamento, favor desconsiderar </strong>
                        Atenção: Fique atento aos possíveis boletos fraudados. Nossos e-mails sempre terão robustec.com.br após o @
                    </p>
                    </th>
                    </tr>
            </tbody>
            </table>
            """
            # Resultado final
            #print(html_table)
            # Separar e-mails
            emails = info['EMAILS']
            if isinstance(emails, list):  # Verificar se é uma lista
                emails = ", ".join(emails)  # Juntar os e-mails com vírgula
            email_list = emails.split(",")  # Separar os e-mails por vírgula
            # Corpo do e-mail em HTML
            # Corpo do e-mail com CSS inline
            corpo_html = f"""
            <div style="max-width: 600px; margin: 0 auto; font-family: Arial, sans-serif; background-color: #fff; border: 1px solid #ddd; border-radius: 8px;">
                <div style="text-align: center; padding: 5px;">
                    <img src="cid:logo" alt="Logo Robustec" style="max-width: 150px;">
                </div>
                <div style="padding: 10px;">
                    <h1 style="color: #f98114; text-align: center; font-size: 20px;">Aviso Importante!</h1>
                    <p style="font-size: 14px; color: #333;overflow-wrap: break-word;">
                        Prezado cliente,<br>
                        informamos que você possui a(s) seguinte(s) NF(s) com data de vencimento no dia de amanhã.\n
                         </p>
                {html_table}
                </div>
                
            </div>
            """
            # Minificar o HTML
            corpo_html_minificado = minify_html(corpo_html)
            
            #print("E-MAILS:")
            for email in email_list:
                send_to = email.strip()  # Remove espaços antes/depois do e-mail
                send_to = send_to.lower() # garantir que toda a escrita de email esteja em minusculas
                status = send_advance(send_to, 'ROBUSTEC AVISO IMPORTANTE', corpo_html_minificado, hoje.date())
            if status == True:
               success = pd.concat([success, df_temp], ignore_index=True)
            else:
               error = pd.concat([error, df_temp], ignore_index=True)
            
            ############# É HOJEEEE #########################################
        print('verificando dicionário vencimento Hoje')
        for cnpj, info in grupo_envio_today.items():
            df_temp = pd.DataFrame(columns=["EMPRESA", "CD_LANCAMENTO","FONE"])
            nome = info["NOME"]
            fone = info["FONE"]
            # Construção dinâmica da tabela
            html_table = f"""
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-size: 14px; table-layout: fixed;">
                <thead>
                    <tr style="background-color: #fd8e45; color: #ffffff; ">
                        <th colspan="3" style="text-align: center; padding: 8px;">ID: {info['NOME']} | DOCUMENTO: {cnpj}</th>
                    </tr>
                    <tr style="background-color: #fd8e45; color: #ffffff;">
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">NF|LANÇAMENTO</th>
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">VALOR PRINCIPAL</th>
                        <th style="border: 1px solid #ddd;text-align: center;"width: 33%;>PARCELA</th>
                    </tr>
                </thead>
                <tbody>
            """
            #print(f"CNPJ_CPF: {cnpj}")
            #print(info['FONE'])
            # Loop para adicionar as linhas dinamicamente
            for nf, detalhes in info['NF'].items():
                faturas = ", ".join(detalhes['FATURA'])
                saldo = ", ".join(detalhes['SALDO'])
                # Primeiro, converta o saldo para float (ou Decimal, se necessário)
                saldo = saldo.split(",")[0].strip()  # Pega o primeiro valor, removendo espaços extras
                saldo = float(saldo.replace(",", "."))    # Se o saldo estiver com vírgulas, converta para ponto
                valor_formatado = f"R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                cd_lancamento = ", ".join(detalhes['CD_LANCAMENTO'])
                boletos = ", ".join(detalhes['BOLETO'])
                #adicionar linha no df
                nova_linha = {"EMPRESA":nome, "CD_LANCAMENTO":cd_lancamento,"FONE": fone}
                df_temp = pd.concat([df_temp, pd.DataFrame([nova_linha])], ignore_index=True)
                if ',' in boletos:
                    boletos = "UNIFICADO"
                if boletos == "UNIFICADO":
                    cd_lancamento = cd_lancamento.split(",")[0].strip()
                    nf = cd_lancamento
                    if nf.endswith('.0'):
                        nf = nf[:-2]
                html_table += f"""
                <tr>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{nf}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{valor_formatado}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center; ">{boletos}° </td>
                </tr>
                """
            # Fechando a tabela
            html_table += """
            <tr style="background-color: #27573d; color: #fff; margin: 0; text-align: center; padding: 8px; border-radius: 0 0 8px 8px; position: relative; bottom: 0;">
                    <th colspan="3" >  <p style="margin: 0; font-size: 12px;">Dúvidas? Entre em contato:</p>
                        <a href="https://wa.me/555433592230" style="color: #fff; text-decoration: none;">WhatsApp 54-3359-2230</a> | 
                        <a href="mailto:cobranca@robustec.com.br" style="color: #fff; text-decoration: none;"> cobranca@robustec.com.br</a>
                </th>
                    </tr>
                <tr>
                    <th colspan="3" >  <p style="color: #f98114; text-align: center; font-size: 13px;">
                        <strong> Caso já tenha sido realizado pagamento, favor desconsiderar </strong>
                        Atenção: Fique atento aos possíveis boletos fraudados. Nossos e-mails sempre terão robustec.com.br após o @
                    </p>
                    </th>
                    </tr>
            </tbody>
            </table>
            """
            
            # Corpo do e-mail em HTML
            # Corpo do e-mail com CSS inline

            # Resultado final
            # Separar e-mails
            emails = info['EMAILS']
            if isinstance(emails, list):  # Verificar se é uma lista
                emails = ", ".join(emails)  # Juntar os e-mails com vírgula
            email_list = emails.split(",")  # Separar os e-mails por vírgula
            corpo_html = f"""
            <div style="max-width: 600px; margin: 0 auto; font-family: Arial, sans-serif; background-color: #fff; border: 1px solid #ddd; border-radius: 8px;">
                <div style="text-align: center; padding: 5px;">
                    <img src="cid:logo" alt="Logo Robustec" style="max-width: 150px;">
                </div>
                <div style="padding: 10px;">
                    <h1 style="color: #f98114; text-align: center; font-size: 20px;">Aviso Importante!</h1>
                    <p style="font-size: 14px; color: #333;overflow-wrap: break-word;">
                        Prezado cliente,<br>
                        informamos que você possui a(s) seguinte(s) NF(s) com data de vencimento no dia de hoje.\n
                        </p>
                {html_table}
                </div>
                    
            </div>
            """
            # Minificar o HTML
            corpo_html_minificado = minify_html(corpo_html)
            #print("E-MAILS")
            for email in email_list:
                send_to = email.strip()  # Remove espaços antes/depois do e-mail
                send_to = send_to.lower() # garantir que toda a escrita de email esteja em minusculas
                status = send_advance(send_to, 'ROBUSTEC AVISO IMPORTANTE', corpo_html_minificado, hoje.date())
            if status == True:
               success = pd.concat([success, df_temp], ignore_index=True)
            else:
               error = pd.concat([error, df_temp], ignore_index=True)
            
            
            ############# 3 DIAS DE ATRASO #########################################
        print('verificando dicionário vencido a mais de 3 dias')
        for cnpj, info in grupo_passou_tres.items():
            df_temp = pd.DataFrame(columns=["EMPRESA", "CD_LANCAMENTO","FONE"])
            nome = info["NOME"]
            fone = info["FONE"]
            # Construção dinâmica da tabela
            html_table = f"""
            <table width="100%" cellpadding="8" cellspacing="0" style="border-collapse: collapse; font-size: 14px; table-layout: fixed;">
                <thead>
                    <tr style="background-color: #fd8e45; color: #ffffff; ">
                        <th colspan="3" style="text-align: center; padding: 8px;">ID: {info['NOME']} | DOCUMENTO: {cnpj}</th>
                    </tr>
                    <tr style="background-color: #fd8e45; color: #ffffff;">
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">NF|LANÇAMENTO</th>
                        <th style="border: 1px solid #ddd;text-align: center;width: 33%;">VALOR PRINCIPAL</th>
                        <th style="border: 1px solid #ddd;text-align: center;"width: 33%;>PARCELA</th>
                    </tr>
                </thead>
                <tbody>
            """
            #print(f"CNPJ_CPF: {cnpj}")
            #print(info['FONE'])
            # Loop para adicionar as linhas dinamicamente
            for nf, detalhes in info['NF'].items():
                faturas = ", ".join(detalhes['FATURA'])
                saldo = ", ".join(detalhes['SALDO'])
                # Primeiro, converta o saldo para float (ou Decimal, se necessário)
                saldo = saldo.split(",")[0].strip()  # Pega o primeiro valor, removendo espaços extras
                saldo = float(saldo.replace(",", "."))    # Se o saldo estiver com vírgulas, converta para ponto
                valor_formatado = f"R$ {saldo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                cd_lancamento = ", ".join(detalhes['CD_LANCAMENTO'])
                boletos = ", ".join(detalhes['BOLETO'])
                #adicionar linha no df
                nova_linha = {"EMPRESA":nome, "CD_LANCAMENTO":cd_lancamento,"FONE": fone}
                df_temp = pd.concat([df_temp, pd.DataFrame([nova_linha])], ignore_index=True)
                if ',' in boletos:
                    boletos = "UNIFICADO"
                if boletos == "UNIFICADO":
                    cd_lancamento = cd_lancamento.split(",")[0].strip()
                    nf = cd_lancamento
                    if nf.endswith('.0'):
                        nf = nf[:-2]
                html_table += f"""
                <tr>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{nf}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center;">{valor_formatado}</td>
                <td style=" border-bottom: 1px solid #ddd;text-align: center; ">{boletos}° </td>
                </tr>
                """
            # Fechando a tabela
            html_table += """
            <tr style="background-color: #27573d; color: #fff; margin: 0; text-align: center; padding: 8px; border-radius: 0 0 8px 8px; position: relative; bottom: 0;">
                    <th colspan="3" >  <p style="margin: 0; font-size: 12px;">Dúvidas? Entre em contato:</p>
                        <a href="https://wa.me/555433592230" style="color: #fff; text-decoration: none;">WhatsApp 54-3359-2230</a> | 
                        <a href="mailto:cobranca@robustec.com.br" style="color: #fff; text-decoration: none;"> cobranca@robustec.com.br</a>
                </th>
                    </tr>
                <tr>
                    <th colspan="3" >  
                        <strong> Caso já tenha sido realizado pagamento, favor desconsiderar </strong>
                        <p style="color: #f98114; text-align: center; font-size: 13px;">Atenção: Fique atento aos possíveis boletos fraudados. Nossos e-mails sempre terão robustec.com.br após o @
                    </p>
                    </th>
                    </tr>
            </tbody>
            </table>
            """
            # Corpo do e-mail em HTML
            # Corpo do e-mail com CSS inline
            # Resultado final
            # Separar e-mails
            emails = info['EMAILS']
            if isinstance(emails, list):  # Verificar se é uma lista
                emails = ", ".join(emails)  # Juntar os e-mails com vírgula
            email_list = emails.split(",")  # Separar os e-mails por vírgula
            corpo_html = f"""
            <div style="max-width: 600px; margin: 0 auto; font-family: Arial, sans-serif; background-color: #fff; border: 1px solid #ddd; border-radius: 8px;">
                <div style="text-align: center; padding: 5px;">
                    <img src="cid:logo" alt="Logo Robustec" style="max-width: 150px;">
                </div>
                <div style="padding: 10px;">
                    <h1 style="color: #f98114; text-align: center; font-size: 20px;">Aviso Importante!</h1>
                    <p style="font-size: 14px; color: #333;overflow-wrap: break-word;">
                        Prezado cliente,<br>
                        informamos que você possui a(s) seguinte(s) NF(s) atrasadas. \n
                        OBS: valor sofrerá alterações com juros e correções até a data de quitação. </p>
                {html_table}
                </div>
                    
            </div>
            """
            # Minificar o HTML
            corpo_html_minificado = minify_html(corpo_html)
            #print("E-MAILS")
            for email in email_list:
                send_to = email.strip()  # Remove espaços antes/depois do e-mail
                send_to = send_to.lower() # garantir que toda a escrita de email esteja em minusculas
                status = send_advance(send_to, 'ROBUSTEC AVISO IMPORTANTE', corpo_html_minificado, hoje.date())
            if status == True:
               success = pd.concat([success, df_temp], ignore_index=True)
            else:
               error = pd.concat([error, df_temp], ignore_index=True)
                
        send_basic(error,"E-mails com erro", "ti3@robustec.com.br")
        send_basic(success,"E-mails enviados com sucesso", "ti3@robustec.com.br")
    except Exception as e:
        import traceback
        print("Ocorreu um erro ao processar a linha:")
        print(f"Dados atuais da linha: {row}")  # Mostra os dados que estavam sendo processados
        print(f"Erro encontrado: {e}")          # Mostra a mensagem de erro
        traceback.print_exc()
        arquivo = rf'C:\projetos\RPA_COBRANCA\logs\ERROS\LOG_LINHA_777.txt'
        with open(arquivo, 'a') as arquivo:
            arquivo.write(f'Erro {e} ao tentar realizar envio de e-mail.\n')  

    file = r'C:\projetos\RPA_COBRANCA\logs\count_envios_LOG.txt'
    with open(file, 'a') as arquivo:
        arquivo.write(f'Foram enviados e-mails no dia {hoje}.\n')
except Exception as e:
    msg = f"Erro: {e}" 
    ass = "ERRO DE BANCO DE DADOS"
    to = "ti3@robustec.com.br"
    import traceback
    print("Ocorreu um erro ao processar a linha:")
    print(f"Dados atuais da linha: {row}")  # Mostra os dados que estavam sendo processados
    print(f"Erro encontrado: {e}")          # Mostra a mensagem de erro
    traceback.print_exc()  
    send_basic(msg,ass,to)