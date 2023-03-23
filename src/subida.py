from cmd import Cmd
import os
import shutil
import pandas as pd
import numpy as np
import datetime
import time
import traceback

from driver_email import enviar_mail_con_adjuntos

PROGRAMADOR = 'xxxxxxxxxxxxx@xxxxxxxxxxxxx.com'
USER = 'xxxxxxxxxxxxx@xxxxxxxxxxxxx.com'
PASSWORD = 'xxxxxxxxxxxxx'
ENCABEZADO_SUBIDA_DATOS = ["ID Cuenta o Nro. de Asig. (0)","ID SubCliente (1)","Activa (S, N) (2)","ID Supervisor (3)","ID Ejecutivo (4)","Nº de Asignación Nuevo (5)","Fecha de Contacto (6)","ID Acción (7)","ID Resultado (8)","Notas (9)","ID Usuario (10)","ID SubEstado (11)","ID SubCliente (12)","Importe Asignado (13)","Importe Histórico (14)","Observaciones (15)","Email (16)","ID Tipo de Teléfono (17)","Nro. de Teléfono (18)","Obs. de Teléfono (19)","ID Tipo de Domicilio (20)","Domicilio sin Formato (21)","Obs. de Domicilio (22)","ID Localidad (23)","ID Provincia (24)","Fecha de Vencimiento (25)","Número de Referencia (26)","Importe Factura (27)","Observaciones Factura (28)","ID Sucursal (29)","Fecha de Pago (30)","ID Concepto (31)","Importe de Pago(32)","ID Usuario  de Pago(33)","Rendido (S, N) (34)","Fecha Proximo Pago (35)","Importe Proximo Pago (36)","Fecha de Acuerdo (37)","Días de Vencimiento (38)","Importe de Acuerdo (39)","Sueldo (40)","Cantidad de Vehiculos (41)"	,"Datos Patrimoniales (42)"]


def limpiar_numeros(df_num):
    con_doble_guion = df_num['telefono'].str.contains('--')
    sin_doble_guion = ~ con_doble_guion
    con_054 = df_num['telefono'].str.contains('(054)',regex=False)
    sin_054 = ~ con_054
    con_guion_1 = df_num['telefono'].str.contains('-1-',regex=False)

    df_num['telefono_2'] = np.nan
    # limpiando los que tienen 11 011 y 0
    numeros_concatenar = df_num[ sin_doble_guion &  con_054 & ~con_guion_1 ]['telefono'].str.split('-',expand=True)
    df_num.loc[sin_doble_guion &  con_054 & ~con_guion_1 ,'telefono_2'] = (numeros_concatenar[1] + numeros_concatenar[2])\
                                                                                    .str.replace(r'^[0]+','',regex=True)\
                                                                                    .str.replace(r'^[54]+','',regex=True)\
                                                                                    .str.replace(r'^[0]+','',regex=True)
    # limpieza de los que tiene -1- en medio   
    con_1_medio = sin_doble_guion &  con_054 & con_guion_1 
    df_num.loc[con_1_medio,'telefono_2'] = df_num[con_1_medio ]['telefono'].str.split('-',expand=True)[2]\
                                                                                    .str.replace(r'^[0]+','',regex=True)\
                                                                                    .str.replace(r'^[54]+','',regex=True)\
                                                                                    .str.replace(r'^[0]+','',regex=True)
    
    # limpieza numeros CON DOBLE GUION
    df_num.loc[con_doble_guion, 'telefono_2'] = df_num[con_doble_guion]['telefono'].str.split('--',expand=True)[1]\
                                                                                    .str.replace(r'^[0]+','',regex=True)\
                                                                                    .str.replace(r'^[54]+','',regex=True)\
                                                                                    .str.replace(r'^[0]+','',regex=True)

    # resto de los numeros que quedaron vacios
    vacios = df_num['telefono_2'].isna()
    df_num.loc[vacios, 'telefono_2'] = df_num[vacios]['telefono'].str.replace('[^\d]+', '').str.replace(r'^[0]+','',regex=True).str.replace(r'^[54]+','',regex=True).str.replace(r'^[0]+','',regex=True)

    return df_num


def Escribir_Datos_Osiris(df, filename, cols_df, cols_osiris):
    
    Control_Carpeta_Subida()
    
    df_subida = pd.DataFrame(columns= ENCABEZADO_SUBIDA_DATOS) 
    df_subida[ cols_osiris ] = df[ cols_df ]
    df_subida.to_csv(f'Subida Osiris/{time.strftime("( %H.%M hs ) -")} {filename}', sep=';', index=False, encoding='ANSI')


def Control_Carpeta_Subida():
    'COntrola que exista una carpeta llamada Subida Osiris y sino la crea'
    pass
    # no la estoy usando


def Preparacion_Cuentas():

    "Condiciones"
    cr = pd.read_csv('cr.csv', sep=';', encoding='ANSI', dtype=str)
    columnas =['Nº de Asignacion (0)','Razon social (1)',	'ID Tipo de Documento (2)',	'DNI (3)',	'Domicilio (4)','ID Localidad (5)','ID Provincia (6)','Código Postal (7)','Observaciones Domicilio (8)'	,'Numero Telefono (9)'	,'Observaciones Telefono (10)'	,'Importe Asignado (11)'	,'Fecha de Ingreso (12)','Fecha de Deuda dd/mm/aaaa (13)','Importe Historico (14)'	,'Observaciones (15)'	,'Fecha Fin de Gestion (16)'	,'IDSucursal(17)']
    df_os = pd.DataFrame(columns=columnas)

    provincias = {
            'BUENOS AIRES':	'24',
            'CAPITAL FEDERAL':'23',
            'CATAMARCA':'22',
            'CHACO'	:'21',
            'CHUBUT':'20',
            'CORDOBA':'19',
            'CORRIENTES':'18',
            'ENTRE RIOS':'17',
            'FORMOSA':'16',
            'JUJUY':'15',
            'LA PAMPA':'14',
            'LA RIOJA':'13',
            'MENDOZA':'12',
            'MISIONES':'11',
            'NEUQUEN':'10',
            'RIO NEGRO':'9',
            'SALTA'	:'8',
            'SAN JUAN':'7',
            'SAN LUIS':'6',
            'SANTA CRUZ':'5',
            'SANTIAGO DEL ESTERO':	'4',
            'TIERRA DEL FUEGO'	:'3',
            'SANTA FE':	'2',
            'TUCUMAN':	'1'}

    date_now = datetime.date.today()
    years_to_add = date_now.year + 3

    date_1 = date_now.strftime('%d/%m/%Y')
    date_2 = date_now.replace(year=years_to_add).strftime('%d/%m/%Y')

    df_os['Nº de Asignacion (0)'] = cr['NRODOC']
    df_os['Razon social (1)'] = cr['NOMBRECOMPLETO'].str.title()#apply(lambda fila: fila.capitalize() )  #str.title()
    df_os['ID Tipo de Documento (2)'] = '1' 
    df_os['DNI (3)'] = cr['NRODOC']

    df_os['Domicilio (4)'] =  cr['CALLE'] + ' ' + cr['NUMERO'] + ' - '+ cr['PISO'] + ' ' + cr['DEPTO'] + ' - '+ cr['BARRIO'] + ' - ' + cr['LOCALIDAD']

    df_os['ID Localidad (5)'] = '0'

    df_os['ID Provincia (6)']  = cr['PROVINCIA'].apply(lambda fila: provincias[fila] )
    df_os['Código Postal (7)'] = cr['POSTAL']
    df_os['Importe Asignado (11)'] = cr['DEUDA_ACTUALIZADA'].str.replace(',','.')
    df_os['Fecha de Ingreso (12)'] = date_1
    df_os['Fecha de Deuda dd/mm/aaaa (13)'] = cr['INICIOMORA']
    df_os['Importe Historico (14)'] = cr['CAPITAL'].str.replace(',','.')
    df_os['Observaciones (15)'] = 'Gestor anterior: ' + cr['GESTOR_ANTERIOR'] + ' - '+ 'Score: ' + cr['SCORE'].str.replace(',','.').astype(float).round(2).astype(str)
    df_os['Fecha Fin de Gestion (16)'] = date_2
    df_os['IDSucursal(17)'] = '1'
    df_os['riesgo'] = cr['RIESGO']
    # df_os.to_csv('subida_cartera.csv',sep=';',encoding='ANSI', index = False)

    for name, df_sub in df_os.groupby('riesgo'):

        print( f'Ecribiendo: subida_cartera_{name}.csv' )
        df_sub = df_sub.drop('riesgo', inplace = False, axis=1)
        df_sub.to_csv(f'Subida Osiris/{time.strftime("( %H.%M hs ) -")} subida_cartera_{name}.csv',sep=';',encoding='ANSI', index = False)    


def Preparacion_Cuentas_Comafi():

    nombre_cartera = input('\nIngrese el nombre de la cartera que desea:  ')

    print('Iniciando preparacion')
    #lectura planilla modelo
    df_os = pd.read_csv('modelos/modelo_cuentas.csv',encoding='ANSI',sep=';')
    df = pd.read_excel('emerix.xlsx',dtype=str)
    col_utiles = {'Nº Doc':'dni','Apellido, Nombre':'nombre', 'Direccion':'direccion', 'Localidad':'localidad',
       'Cod. Pos.':'cod_postal', 'Provincia':'provincia', 'Telefono':'telefono', 'Sucursal':'sucursal', 
        'Banca':'banca', 'Cod. Linea':'cod_linea', 'Linea':'linea',
        'Deuda Vencida':'deuda_total', 'Cap.':'deuda_capital',
        'Dias Mora':'dias_mora', 'Inicio mora':'fecha_inicio', 'Fecha Ult. Pago':'fecha_ult_pago', 
        'Subcliente':'subcliente'}      
    provincias ={
            '0':'0',
            'BUENOS AIRES':	'24',
            'CAPITAL FEDERAL':'23',
            'CATAMARCA':'22',
            'CHACO'	:'21',
            'CHUBUT':'20',
            'CORDOBA':'19',
            'CORRIENTES':'18',
            'ENTRE RIOS':'17',
            'FORMOSA':'16',
            'JUJUY':'15',
            'LA PAMPA':'14',
            'LA RIOJA':'13',
            'MENDOZA':'12',
            'MISIONES':'11',
            'NEUQUEN':'10',
            'RIO NEGRO':'9',
            'SALTA'	:'8',
            'SAN JUAN':'7',
            'SAN LUIS':'6',
            'SANTA CRUZ':'5',
            'SANTIAGO DEL ESTERO':	'4',
            'TIERRA DEL FUEGO'	:'3',
            'SANTA FE':	'2',
            'TUCUMAN':	'1'}

    df = df[list(col_utiles.keys())]
    df =  df.rename( columns=col_utiles)

    #reemplazo de valores nulos
    df.loc[df['provincia'].isna(),'provincia'] = '0'
    
    # reemplazo de 'ñ' en nombres
    caracteres =  ['#' , 'Ð' , 'ð', '&' ]
    for car in caracteres: 
        n = df['nombre'].str.contains(car).sum()
        df['nombre'] = df['nombre'].str.replace(car,'ñ').str.title()    
        # n = df['nombre'].str.contains(car).sum()
        print(f'Caracter {car}: se remplazaron {n}')
    print('\n')
    print('\nComenzando escritura de archivos..\n\n')   
        
    date_now = datetime.date.today()
    years_to_add = date_now.year + 3

    date_1 = date_now.strftime('%d/%m/%Y')
    date_2 = date_now.replace(year=years_to_add).strftime('%d/%m/%Y')

    df_os['Nº de Asignacion (0)'] = df['dni']
    df_os['Razon social (1)'] = df['nombre']
    df_os['ID Tipo de Documento (2)'] = '1' 
    df_os['DNI (3)'] = df['dni']

    df_os['Domicilio (4)'] = df['direccion'] + ' - ' + df['localidad']
    df_os['ID Localidad (5)'] = '0'

    df_os['ID Provincia (6)']  = df['provincia'].apply(lambda fila: provincias[fila] )
    df_os['Código Postal (7)'] = df['cod_postal']

    df_os['Importe Asignado (11)'] = df['deuda_total'].astype(float).apply(np.ceil).astype(str).str.replace('.0','',regex=False)
    df_os['Fecha de Ingreso (12)'] = date_1
    df_os['Fecha de Deuda dd/mm/aaaa (13)'] = df['fecha_inicio']
    df_os['Importe Historico (14)'] = df['deuda_capital'].astype(float).apply(np.ceil).astype(str).str.replace('.0','',regex=False)

    df_os['Observaciones (15)'] = 'Fecha ultimo pago: ' + df['fecha_ult_pago']
    df_os['Fecha Fin de Gestion (16)'] = date_2
    df_os['IDSucursal(17)'] = '1'
    df_os['subcliente'] = df['subcliente']

    name_folder = f'Subida Osiris/{time.strftime("( %H.%M hs ) -")} {nombre_cartera}'
    if os.path.isdir(name_folder):
        shutil.rmtree(name_folder)
    os.mkdir(name_folder)
    # iterear un loop por los subcliente a traves de un grouby
    for name, df_sub in df_os.groupby('subcliente'):
        print( f'Ecribiendo: {name}.csv' )
        df_sub = df_sub.drop('subcliente', inplace = False, axis=1)
        df_sub.to_csv(f'{name_folder}/{name}.csv',sep=';',encoding='ANSI', index = False)    


def Preparacion_Datos():
    ' Necesita que este en la carpeta'
    print('Preparando planillas de datos...')
    Encabezado_Subida = ["ID Cuenta o Nro. de Asig. (0)","ID SubCliente (1)","Activa (S, N) (2)","ID Supervisor (3)","ID Ejecutivo (4)","Nº de Asignación Nuevo (5)","Fecha de Contacto (6)","ID Acción (7)","ID Resultado (8)","Notas (9)","ID Usuario (10)","ID SubEstado (11)","ID SubCliente (12)","Importe Asignado (13)","Importe Histórico (14)","Observaciones (15)","Email (16)","ID Tipo de Teléfono (17)","Nro. de Teléfono (18)","Obs. de Teléfono (19)","ID Tipo de Domicilio (20)","Domicilio sin Formato (21)","Obs. de Domicilio (22)","ID Localidad (23)","ID Provincia (24)","Fecha de Vencimiento (25)","Número de Referencia (26)","Importe Factura (27)","Observaciones Factura (28)","ID Sucursal (29)","Fecha de Pago (30)","ID Concepto (31)","Importe de Pago(32)","ID Usuario  de Pago(33)","Rendido (S, N) (34)","Fecha Proximo Pago (35)","Importe Proximo Pago (36)","Fecha de Acuerdo (37)","Días de Vencimiento (38)","Importe de Acuerdo (39)","Sueldo (40)","Cantidad de Vehiculos (41)"	,"Datos Patrimoniales (42)"]

    cr = pd.read_csv('cr.csv',sep=';',encoding= 'ANSI', dtype=str)
    
    cuentas_subidas = pd.read_csv('cuentas.csv', encoding = 'ANSI', sep = ';', dtype= str)
    cuentas_subidas = cuentas_subidas[['Cuenta','Mat. Unica']].rename(columns = {'Mat. Unica': 'DNI'}, inplace = False)

    df_cr = cr[['NRODOC','RIESGO','TIPO_CUENTA', 'TIPO_CUENTA', 'TEL_PARTICULAR', 'TEL_LABORAL', 'TEL_ALTERNATIVO',
        'TEL_CR_PARTICULAR', 'TEL_CR_LABORAL', 'TEL_CR_ALTERNATIVO', 'EMAIL','ULTIMO_PAGO']].rename(columns = {'NRODOC': 'DNI'}, inplace = False).copy()
    # print(df_cr.info())
    df_cr = pd.merge(cuentas_subidas, df_cr, how="inner", on="DNI")

    frames = list()
    print('Subiendo numeros...\n')
    print('----------------------')

    # MASIVOS

    # paso TEL_ALTERNATIVO esta vacio
    df = df_cr.loc[df_cr['TEL_ALTERNATIVO'].notnull(),['Cuenta','TEL_ALTERNATIVO']].rename(columns = { 'TEL_ALTERNATIVO': 'TEL'}, inplace = False).copy()
    df['ID_FONO'] = '1'
    print(f'{len(df)} Telefonos ALTERNATIVOS cargados com MASIVOS')
    frames.append(df)

    # paso TEL_PARTICULAR como masivo.    
    df = df_cr.loc[df_cr['TEL_ALTERNATIVO'].isnull() & df_cr['TEL_PARTICULAR'].notnull(),['Cuenta','TEL_PARTICULAR']].rename(columns = {'TEL_PARTICULAR': 'TEL'}, inplace = False).copy()
    df_cr.loc[df_cr['TEL_ALTERNATIVO'].isnull() & df_cr['TEL_PARTICULAR'].notnull(),'TEL_PARTICULAR'] = np.nan
    df['ID_FONO'] = '1'
    print(f'{len(df)} Telefonos PARTICULAR cargados com MASIVOS en cuentas que no poseen ALTERNATIVO')    
    frames.append(df)

    # FIJOS
    df = df_cr.loc[ df_cr['TEL_PARTICULAR'].notnull() , ['Cuenta','TEL_PARTICULAR'] ].rename(columns = {'TEL_PARTICULAR': 'TEL'}, inplace = False).copy()
    df['ID_FONO'] = '2'
    print(f'{len(df)} Telefonos ALTERNATIVOS cargados como FIJOS')
    frames.append(df)

    # LABORALES
    df = df_cr.loc[ df_cr['TEL_LABORAL'].notnull() , ['Cuenta','TEL_LABORAL'] ].rename(columns = { 'TEL_LABORAL': 'TEL'}, inplace = False).copy()
    df['ID_FONO'] = '3'
    print(f'{len(df)} Telefonos LABORALES cargados como LABORALES')
    frames.append(df)

    # OTROS
    df = df_cr.melt(id_vars=['Cuenta'],value_vars=['TEL_CR_PARTICULAR', 'TEL_CR_LABORAL','TEL_CR_ALTERNATIVO']).dropna().copy()
    df = df[['Cuenta','value']].rename(columns = { 'value': 'TEL'}, inplace = False)
    df['ID_FONO'] = '8'
    print(f'{len(df)} Telefonos OTROS_CR cargados como OTROS')
    # print('tamaño de loa datos: ', df.shape )
    frames.append(df)
    print('----------------------\n')

    df_tels = pd.concat(frames)

    # Depuracion
    df_tels['TEL'] = df_tels['TEL'].str.replace(r'[^0-9]+', '', regex=True)   # elimina todo lo que no sea un numero
    # df_tels['TEL'] = df_tels['TEL'].str.replace('-','')
    df_tels['TEL'] = df_tels['TEL'].str.replace(' ','')
    df_tels['TEL'] = df_tels['TEL'].replace('', np.nan)
    df_tels['TEL'].fillna(0)
    df_tels.to_csv('prueba_2.csv',index=False, sep=';')
    df_tels = df_tels.astype({'TEL': 'Int64'})
    df_tels = df_tels.astype({'TEL': 'str'})
    df_tels.drop(df_tels[df_tels.TEL == '0'].index, inplace=True)

    Escribir_Datos_Osiris(df_tels,'DATOS_CR_subida_telefonos.csv',['Cuenta','ID_FONO','TEL'],['ID Cuenta o Nro. de Asig. (0)',"ID Tipo de Teléfono (17)","Nro. de Teléfono (18)"])
    print(f'{len(df_tels)} TELEFONOS se guardaron en archivo: subida_telefono.csv')
    # reemplazar cualquier caractaer alfabetico que este dentro del numero para evitar roblema en futuro
    # borrar numero cero
    df_cr.loc[df_cr['EMAIL'].notnull(), 'EMAIL'] = df_cr.loc[df_cr['EMAIL'].notnull(), 'EMAIL'].str.replace(' ','')
    df_mail = df_cr.loc[df_cr['EMAIL'].notnull(),['Cuenta','EMAIL']].copy()
    Escribir_Datos_Osiris( df_mail, 'DATOS_CR_subida_mail.csv', ['Cuenta','EMAIL'],  ['ID Cuenta o Nro. de Asig. (0)',"Email (16)"])    
    print(f'{len(df_mail)} MAILS se guardaron en archivo: subida_mail.csv\n\n')


def Preparacion_Datos_Comafi():

    print('Preparando planillas de datos para comafi...')

    df_subida = pd.read_csv('modelos/modelo_datos.csv',encoding='ANSI',sep=';')

    df_num = pd.read_excel('emerix.xlsx',dtype=str)
    col_utiles = {'Nº Doc':'dni','Apellido, Nombre':'nombre', 'Direccion':'direccion', 'Localidad':'localidad',
        'Cod. Pos.':'cod_postal', 'Provincia':'provincia', 'Telefono':'telefono', 'Sucursal':'sucursal', 
            'Banca':'banca', 'Cod. Linea':'cod_linea', 'Linea':'linea',
            'Deuda Vencida':'deuda_total', 'Cap.':'deuda_capital',
            'Dias Mora':'dias_mora', 'Inicio mora':'fecha_inicio', 'Fecha Ult. Pago':'fecha_ult_pago', 
            'Subcliente':'subcliente'}
    df_num = df_num[list(col_utiles.keys())]
    df_num =  df_num.rename( columns=col_utiles)
    df_num  = df_num[ df_num['telefono'].notna() ]
    df_num = limpiar_numeros(df_num)
    df_num = df_num[['dni','telefono','telefono_2']]
    df_num['telefono_2'] = df_num[df_num['telefono_2'].apply(len)>=6]['telefono_2']
    df_num = df_num[df_num['telefono_2'].notna()]
    
    df_cuentas_subidas = pd.read_csv('cuentas.csv', encoding = 'ANSI', sep = ';', dtype= str)
    df_cuentas_subidas = df_cuentas_subidas[['Cuenta','Mat. Unica']].rename(columns = {'Mat. Unica': 'dni'}, inplace = False)
    
    
    df_numeros_cuentas =  pd.merge( df_cuentas_subidas, df_num ,how='inner', on='dni')
    df_numeros_cuentas.to_csv('verificacion.csv',sep=';',encoding='ANSI',index=False)
    df_subida[['ID Cuenta o Nro. de Asig. (0)',"Nro. de Teléfono (18)"]] = df_numeros_cuentas[['Cuenta','telefono_2']]
    df_subida["ID Tipo de Teléfono (17)"] = 1
    print('Guardando planilla subida...')
    df_subida.to_csv(f'Subida Osiris/{time.strftime("( %H.%M hs ) -")}DATOS_EMERIX_subida_telefono.csv', sep=';', index=False, encoding='ANSI')
    print('Guardado exitoso!')
    

    
def Datos_Riesgo():
    'NECESARIOS: tener el archivos.csv de riesgo y el archivo de las cuentas de osiris como cuentas.csv'
    
    col_numeros = { 'NÃºmero.1': 'tel_riesgo_1', 'NÃºmero.2':'tel_riesgo_2', 'NÃºmero.3':'tel_riesgo_3','NÃºmero.4':'tel_riesgo_4'}
    
    l = [ x for x in range(66)]
    riesgo = pd.read_csv('riesgo.csv',sep=';', encoding='ANSI', dtype= str, index_col=False,usecols=l)
    df_riesgo = riesgo[ ['DNI']+ list(col_numeros.keys()) +['NSE'] ]

    df_riesgo = df_riesgo.rename(columns = col_numeros, inplace = False)
    
    # Lectura de cuetnas de Osiris
    cuentas_subidas = pd.read_csv('cuentas.csv', encoding = 'ANSI', sep = ';', dtype= str)
    cuentas_subidas = cuentas_subidas[['Cuenta','Mat. Unica']].rename(columns = {'Mat. Unica': 'DNI'}, inplace = False)


    # obteniendo las cuentas qcruzadas de osiris y riesgo
    df_riesgo = pd.merge(cuentas_subidas, df_riesgo, how="inner", on="DNI")

    # NUMERO DE TELEFONOS
    frames = list()
    num_tel = len([ col_name  for col_name in list(df_riesgo.columns) if 'tel_riesgo' in col_name] )
    for i  in range(1,num_tel + 1 ):
        col_riesgo =  f'tel_riesgo_{i}'
        df = df_riesgo.loc[df_riesgo[col_riesgo].notnull(),['Cuenta',col_riesgo]].rename(columns ={col_riesgo: 'TEL'}).copy()
        df['OBS'] = f'RIESGO {i}'
        df['ID_FONO'] = '8'
        frames.append(df)
    df_tels_riesgo = pd.concat(frames)

    Escribir_Datos_Osiris(df_tels_riesgo,'RIESGO_telefonos.csv', ['Cuenta','ID_FONO','TEL','OBS'] , ["ID Cuenta o Nro. de Asig. (0)","ID Tipo de Teléfono (17)","Nro. de Teléfono (18)","Obs. de Teléfono (19)"])
    print('PLanilla de Telefonos de RIESGO escrita')
    
    # #DATOS PATRIMONIALES

def Datos_Info():
    'NECESARIO cuentas de osiris como cuentas.csv, cuentas deinfo exporto como info.xlsx'
    cuentas_subidas = pd.read_csv('cuentas.csv', sep = ';', dtype= str)
    cuentas_subidas = cuentas_subidas[['Cuenta','Mat. Unica']].rename(columns = {'Mat. Unica': 'DNI'}, inplace = False)

    info = pd.read_excel('info.xlsx', dtype= str,skiprows =1)
    df_info = info[['NUMERO DOCUMENTO','NUMERO 1','NUMERO 2','NUMERO 3','E-MAIL','REMUNERACION','RAZON SOCIAL','CANTIDAD.2','DETALLE.1','NSE']]
    df_info = df_info.rename(columns={'NUMERO DOCUMENTO':'DNI','NUMERO 1':'tel_info_1','NUMERO 2':'tel_info_2','NUMERO 3':'tel_info_3','E-MAIL':'MAIL_info','REMUNERACION':'sueldo_info','RAZON SOCIAL':'empleador_info','CANTIDAD.2':'q_vehiculos','DETALLE.1':'detalle_veh','NSE':'NSE_info'}, inplace = False).copy()
    df_info['q_vehiculos'] = df_info['q_vehiculos'].fillna('0')

    df_info = pd.merge(cuentas_subidas, df_info, how="inner", on="DNI")

    #PREPARACION DE NUMEROS
    frames = list()

    for i  in range(1,3):
        col_info =  f'tel_info_{i}'
        df = df_info.loc[df_info[col_info].notnull(),['Cuenta',col_info]].rename(columns ={col_info: 'TEL'}).copy()
        df['OBS'] = f'INFO {i}'
        df['ID_FONO'] = '8'
        frames.append(df)
    Escribir_Datos_Osiris(pd.concat(frames)[['Cuenta','ID_FONO','TEL','OBS']]
                          ,'INFO_telefonos.csv'
                          ,['Cuenta','ID_FONO','TEL','OBS']
                          ,["ID Cuenta o Nro. de Asig. (0)","ID Tipo de Teléfono (17)","Nro. de Teléfono (18)","Obs. de Teléfono (19)"]                          
                          )

    print('Planilla de TELEFONOS de INFO escrita')

    #sueldo
    Escribir_Datos_Osiris(df_info.loc[df_info['sueldo_info'].notnull(),['Cuenta','sueldo_info']]
                          ,'INFO_sueldo.csv'
                          ,['Cuenta','sueldo_info']
                          ,["ID Cuenta o Nro. de Asig. (0)","Sueldo (40)"]                          
                          )
    print('Planilla de SUELDOS de INFO escrita')

    #MAIL
    mails = df_info.loc[df_info['MAIL_info'].notnull(),'MAIL_info'].str.split(',',expand=True)
    renombre = {x: f'mail_{i+1}' for i,x in enumerate(list(mails.columns),0) }
    mails = mails.rename(columns = renombre)
    name_mails =list(renombre.values())
    df_info[name_mails] = mails[name_mails]
    concat_mails = list()
    for name in name_mails:
        concat_mails.append(df_info.loc[df_info[name].notnull(),['Cuenta',name]].rename(columns = {name:'MAIL_info'}))

    Escribir_Datos_Osiris(
        pd.concat(concat_mails) 
        ,'INFO_mail.csv'
        ,['Cuenta','MAIL_info']
        ,["ID Cuenta o Nro. de Asig. (0)","Email (16)"]                
    )
    print('Planilla de MAIL de INFO escrita')


    #MAIL
    Escribir_Datos_Osiris(
        df_info.loc[df_info['q_vehiculos'] != '0',['Cuenta','q_vehiculos']]
        ,'INFO_Qvehiculos.csv'
        ,['Cuenta','q_vehiculos']
        ,["ID Cuenta o Nro. de Asig. (0)","Cantidad de Vehiculos (41)"]
    )

    print('Planilla de Q VEHICULOS de INFO escrita')

    plantilla = 'Sueldo: ${sueldo_info_str} Empleador: {empleador_info} - Cantidad de Vehículos: {q_vehiculos} Detalle: {detalle_veh} - Nivel Socioeconómico: {NSE_info}'
    df_info['sueldo_info_float'] = df_info['sueldo_info'].astype({'sueldo_info':  'float'})
    df_info.loc[df_info['sueldo_info'].notnull(),'sueldo_info_int'] = df_info.loc[df_info['sueldo_info'].notnull(),'sueldo_info_float'].astype({'sueldo_info_float': 'int32'})

    df_info['sueldo_info_str'] = df_info['sueldo_info_int'].apply(lambda fila: "{:,}".format(fila).replace(",", "@").replace('.',',').replace('@','.'))


    df_info['primonial'] = df_info.apply( lambda fila: plantilla.format(**fila.to_dict()) , axis = 1 )  #str.title()

    #borrando sueldo 0
    df_info.loc[df_info['sueldo_info'].isnull(),'primonial']    = df_info.loc[df_info['sueldo_info'].isnull(),'primonial'].str.replace('Sueldo: $nan ','',regex=False)
    df_info.loc[df_info['empleador_info'].isnull(),'primonial'] = df_info.loc[df_info['empleador_info'].isnull(),'primonial'].str.replace('Empleador: nan - ','',regex=False)
    df_info.loc[df_info['q_vehiculos'] == '0','primonial']      = df_info.loc[df_info['q_vehiculos'] == '0','primonial'].str.replace('Cantidad de Vehículos: 0 ','',regex=False)
    df_info.loc[df_info['detalle_veh'].isnull(),'primonial']    = df_info.loc[df_info['detalle_veh'].isnull(),'primonial'].str.replace('Detalle: nan - ','',regex=False)
    df_info.loc[df_info['NSE_info'].isnull(),'primonial'] = df_info.loc[df_info['NSE_info'].isnull(),'primonial'].str.replace(' - Nivel Socioeconómico: nan','',regex=False)
    df_info.loc[df_info['NSE_info'].isnull(),'primonial'] = df_info.loc[df_info['NSE_info'].isnull(),'primonial'].str.replace('Nivel Socioeconómico: nan','',regex=False)

    df_info.loc[(df_info['sueldo_info'].isnull()) & (df_info['empleador_info'].isnull()) & (df_info['q_vehiculos'] == '0') & (df_info['detalle_veh'].isnull()) & (df_info['NSE_info'].isnull()),'primonial'] = np.nan
    df_info[['Cuenta','q_vehiculos','detalle_veh','primonial']].iloc[10]#[-1]
    
    Escribir_Datos_Osiris(df_info.loc[df_info['primonial'].notnull(),['Cuenta','primonial']]
                          ,'INFO_Patrimoniales.csv'
                          ,['Cuenta','primonial']
                          ,["ID Cuenta o Nro. de Asig. (0)","Datos Patrimoniales (42)"]
                          )
    print('Planilla de DATOS PATRIMONIALES de INFO escrita')



class Interfaz_Usuario(Cmd):


    def do_CUENTAS(self,args):
        "Funcion para prepara la planilla de cuentas para subir nueva cartera"
        print('\n\nCOMENZANDO ARMADO DE PLANILLA DE CUENTAS\n')
        print('Preparando...')
        try:
            Preparacion_Cuentas()
            print('PROCESO FINALIZADO.\n\n')

        except: 
            error = traceback.format_exc()            
            print(error)
            msg = f'Error durante preparacion planilla de CUENTAS.\n\n{error}'
            enviar_mail_con_adjuntos( USER,PASSWORD, PROGRAMADOR, 'ERROR PROGRAMA SUBIDA CARTERA', msg )

    def do_DATOS(self,args):
        "Funcion para prepara la planilla de DATOS telefonos y mails"
        print('\n\nCOMENZANDO ARMADO DE PLANILLA DE DATOS\n')
        print('Preparando...')
        try:
            Preparacion_Datos()
            print('PROCESO FINALIZADO.\n\n')

        except: 
            error = traceback.format_exc()            
            print(error)
            msg = f'Error durante preparacion planilla de DATOS.\n\n{error}'
            enviar_mail_con_adjuntos( USER,PASSWORD, PROGRAMADOR, 'ERROR PROGRAMA SUBIDA CARTERA', msg )

    def do_AYUDA(self,args):
        'ofrece ayuda para los requesitos de funcionamiento'

        instrucciones = ''' El programa tiene 2 OBJETIVOS: Preparar la planilla de Cuentas y de datos.\n\n
        REQUERIMIENTOS PARA QUE EL PROGRAMA FUNCIONE CORRECTAMENTE.\n
        --------------------------------------------  
        
        1. Para Preparacion PLANILLA DE CUENTAS de subida de cartera:
            - La CARTERA descargada de CR este guardada como "cr.csv" en la misma carpeta donde este este programa\n
        
        2. Para Preparacion PLANILLA DATOS (Telefonos y Mails). Se necesita:
            -  El Informe de Cuentas de la cartera recien subida se guarde como "cuentas.csv" en la misma carptea donde este programa\n
        
        3. Para Preparacion datos RIESGO ONLINE:
            - Planilla de riesgo guardada como "riesgo.csv" misma ubicacion que el programa
            - Cuentas de OSIRIS guardadas como "cuentas.csv" misma ubicacion que el programa\n
        
        4. Para Preparacion datos de INFO EXPERTO:
            - Planilla de riesgo guardada como "info.xlsx" misma ubicacion que el programa
            - Cuentas de OSIRIS guardadas como "cuentas.csv" misma ubicacion que el programa\n
                        
        5. Para Preparacion SUBIDA CUENTAS COMAFI :
            - La CARTERA de emerix preparada por el pelado este guardada como "emerix.xlsx" (es un archivo excel) en la misma carpeta donde este este programa
            - Se pedira que INGRESE EL NOMBRE DE LA CARTERA  para guardado\n    
            
        6. Para Preparacion PLANILLA DATOS (Telefonos y Mails). Se necesita:
            - La CARTERA de emerix preparada por el pelado este guardada como "emerix.xlsx" (es un archivo excel) en la misma carpeta donde este este programa
            - El Informe de Cuentas de la cartera recien subida se guarde como "cuentas.csv" en la misma carpeta donde este programa\n
        --------------------------------------------  

        '''
        print(instrucciones)
  
    def do_INFO(self,args):
        "Funcion para prepara la planilla de DATOS telefonos y mails"
        print('\n\nCOMENZANDO PREPARACION DATOS INFO EXPERTO\n')
        print('Preparando...')
        
        try:
            Datos_Info()
            print('PROCESO FINALIZADO.\n\n')
        
        except: 
            error = traceback.format_exc()            
            print(error)
            msg = f'Error durante preparacion planilla de INFO EXPERTO.\n\n{error}'
            enviar_mail_con_adjuntos( USER,PASSWORD, PROGRAMADOR, 'ERROR PROGRAMA SUBIDA CARTERA', msg )
        

  
    def do_RIESGO(self,args):
        "Funcion para prepara la planilla de DATOS telefonos y mails"
        print('\n\nCOMENZANDO PREPARACION DATOS RIESGO ONLINE\n')
        print('Preparando...')
        try:
            Datos_Riesgo()
            print('PROCESO FINALIZADO.\n\n')
        
        except: 
            error = traceback.format_exc()            
            print(error)
            msg = f'Error durante preparacion planilla de RIESGO ONLINE.\n\n{error}'
            enviar_mail_con_adjuntos( USER,PASSWORD, PROGRAMADOR, 'ERROR PROGRAMA SUBIDA CARTERA', msg )

    def do_CUENTAS_COMAFI(self,args):
        try:
            Preparacion_Cuentas_Comafi()
            print('PROCESO FINALIZADO.\n\n')
        except:
            
            error = traceback.format_exc()            
            print(error)
            msg = f'Error durante preparacion planilla de SUBIDA CUENTAS COMAFI.\n\n{error}'
            enviar_mail_con_adjuntos( USER,PASSWORD, PROGRAMADOR, 'ERROR PROGRAMA SUBIDA CARTERA CUENTAS COMAFI', msg )
            
  
    def do_DATOS_COMAFI(self,args):
        try:
            Preparacion_Datos_Comafi()
            print('PROCESO FINALIZADO.\n\n')
        except:
            
            error = traceback.format_exc()            
            print(error)
            msg = f'Error durante preparacion planilla de DATOS COMAFI.\n\n{error}'
            enviar_mail_con_adjuntos( USER,PASSWORD, PROGRAMADOR, 'ERROR PROGRAMA SUBIDA DATOS COMAFI', msg )
            
        pass
  
  
    def default(self,args):
        print("Error. El comando \'" + args + "\' no existe")
               
    def precmd(self,args):
        
        if args.lower() == 'help':
            return args.lower() 
        else:
            return args.upper()
        
    def do_EXIT(self,args):
        """Sale del programa"""
        print(''.center(70,'='))
        print("\n\nFINALIZANDO SISTEMA...")
        print(''.center(70,'-'))
        
        
        raise SystemExit



if __name__ == '__main__':

   

    # print(LOGO)
    interfaz = Interfaz_Usuario()
    interfaz.prompt = '>> '
    print(''.center(70,'-'))
    print('\n\t\tSUBIDA DE CARTERA\n')
    print(''.center(70,'='))
    comando = '''
        COMANDOS:
            - help : ver funciones disponibles (solo minuscula)
            - AYUDA: Requermientos para usar programa 
            - EXIT: Salir del programa.
        \n(Se aceptan minusculas)
    '''
    print(comando)
    interfaz.cmdloop("\nIniciando consola de comandos...\n")
