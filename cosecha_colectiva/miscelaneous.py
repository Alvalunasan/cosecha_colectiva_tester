
import pandas as pd
import numpy as np
import os
import pathlib
from cosecha_colectiva import config


def read_caf_excel_sheet_old(filename, sheet_type):

    df_data = pd.read_excel(filename, sheet_name=sheet_type, dtype=config.xls_types[sheet_type], header=config.xls_headers[sheet_type])
    return df_data

def read_caf_excel_sheet(filename, sheet_type):

    filename = filename.stem

    #filename = filename.replace('.xlsx','')
    id = config.dict_file_name_sheets_id[filename]

    base_url = 'https://docs.google.com/spreadsheets/d/'
    suffix = '/export?format=xlsx'
    url = base_url+id+suffix

    df_data = pd.read_excel(url,
                          sheet_name=sheet_type,
                          dtype=config.xls_types[sheet_type], header=config.xls_headers[sheet_type]
                  )
    return df_data


def get_dict_usuarios_xls(xls_name):

    #xls_filename = pathlib.Path(config.xls_dir, xls_name)

    df_grupo = read_caf_excel_sheet(pathlib.Path(xls_name), "Usuarios")
    dict_df_grupo = df_grupo.to_dict(orient='records')

    return dict_df_grupo

def get_dict_acuerdos_xls(xls_name, idx_acuerdos=0):

    #xls_filename = pathlib.Path(config.xls_dir, xls_name)

    df_acuerdos = read_caf_excel_sheet(pathlib.Path(xls_name), "Acuerdos")
    dict_df_acuerdos= df_acuerdos.to_dict(orient='records')
    dict_df_acuerdos = dict_df_acuerdos[idx_acuerdos]

    return dict_df_acuerdos

def get_specific_session_info(df_sesion, session_part, session_num):
    #Obtiene el "pedacito" de excel (ya hecho DF) correspondiente a una parte de la sesion y a un mes

    #Busca la fila de la clave en el excel ["AHORRO", "PRÉSTAMO", "ABONO", "MULTAS", "INTERÉS"]
    start_row_vec = df_sesion[config.column_xls_words] == session_part
    start_row = start_row_vec[start_row_vec].index.values

    # Si no se encuentra la palabra clave regresa vacío
    if len(start_row) == 0:
        return {}

    start_row = start_row[0]

    #Busca la palabra total despues de la palabra clave
    end_row_vec = df_sesion[config.column_xls_words] == "TOTAL"
    end_row_vec1 = end_row_vec[end_row_vec].index.values
    idx_end_row = np.argmax(end_row_vec1>start_row)
    end_row = end_row_vec1[idx_end_row]

    #Busca la columna que corresponde al indice del mes (Enero =  1, Febrero = 2, etc)
    month_word = config.month_words[session_num-1]
    idx_month_column = df_sesion.iloc[start_row+1,:].str.contains(month_word)
    idx_not_total_column = ~df_sesion.iloc[start_row+1,:].str.contains("TOTAL")

    idx_column_vec = idx_month_column & idx_not_total_column
    column_data = idx_column_vec[idx_column_vec].index.values[0]

    #Obtiene el "pedacito de excel correspondiente"
    specific_data = df_sesion.iloc[start_row+2:end_row, column_data]
    specific_data = specific_data.reset_index(drop=True)

    specific_data = specific_data.loc[specific_data != '0']
    specific_data = specific_data.loc[specific_data != 'NAN']

    specific_data = specific_data.str.split(',') 
    if session_part in config.xls_words_int:
        specific_data = specific_data.apply(lambda x: [int(s) for s in x])
    else:
        specific_data = specific_data.apply(lambda x: [float(s) for s in x])
    
    #Limpia datos i lo pasa a diccionario
    #specific_data = specific_data[specific_data != 0]
    specific_data = specific_data[~specific_data.isnull()]
    #specific_data = specific_data.to_frame()
    #specific_data = specific_data.to_dict()
 
    return specific_data

def fill_list(value_fill, size_list, default_value):

    if pd.isna(value_fill):
        return [default_value] * size_list
    if len(value_fill) < size_list:
        missing = size_list - len(value_fill)
        append = [default_value] * missing
        return value_fill + append
    return value_fill

def append_prestamo_df(df_prestamo, df_ampliacion, df_num_sesiones):

    #Junta los dataframe de prestamo ampliacion y numero de sesiones para nuevos prestamos

    if df_prestamo.shape[0] == 0:
            return df_prestamo
    else:
            
        df_prestamo = df_prestamo.join(df_ampliacion, how='left')
        df_prestamo = df_prestamo.join(df_num_sesiones, how='left')

        df_prestamo['num_prestamos'] = df_prestamo['PRÉSTAMO'].apply(lambda x: len(x))
        df_prestamo['AMPLIACIÓN'] = df_prestamo.apply(lambda x: fill_list(x['AMPLIACIÓN'], x['num_prestamos'], 0),  axis = 1)
        df_prestamo['NUM_SESIONES'] = df_prestamo.apply(lambda x: fill_list(x['NUM_SESIONES'], x['num_prestamos'], 6),  axis = 1)

        df_prestamo = df_prestamo.drop(['num_prestamos'], axis=1)

    return df_prestamo


def read_transform_info_xls(xls_name, session_num, dict_users, type_xls='MAYRA', hoja="Hoja1"):

    dict_session = get_session_info_xls(xls_name, session_num, type_xls=type_xls, hoja=hoja)
    dict_session = insert_id_users_dict_session(dict_session, dict_users)

    if type_xls != 'MAYRA':
        new_dict_sesison = dict()
        for dict_part in dict_session.keys():
            new_dict_sesison[dict_part] = pd.Series(dict_session[dict_part], name=dict_part, dtype=object).to_frame()
        dict_session = new_dict_sesison
        dict_session['PRÉSTAMO'] = append_prestamo_df(dict_session['PRÉSTAMO'], dict_session['AMPLIACIÓN'], dict_session['NUM_SESIONES'])

    return dict_session

def get_session_info_xls(xls_name, session_num, type_xls='MAYRA', hoja="Hoja1"):
    #Obtiene todos los movimientos para 1 sesion a partir de un excel

    #Lee el excel y lo pasa a DF
    xls_filename = pathlib.Path(config.xls_dir, xls_name)

    df_sesion = read_caf_excel_sheet(xls_filename, hoja)
    df_sesion = df_sesion.apply(lambda x: x.astype(str).str.upper())

    # toma los datos del DF para cada palabra clave ["AHORRO", "PRÉSTAMO", "ABONO", "MULTAS", "INTERÉS"]
    if type_xls == 'MAYRA':
        xls_session_words_dict = config.xls_session_words_dict
    else:
        xls_session_words_dict = config.xls_session_words_dict_2

    dict_session = dict()
    for session_part in xls_session_words_dict.keys():
        dict_session[session_part] = get_specific_session_info(df_sesion, session_part, session_num)


    # suma abono e interés en abono (asi se paga en la app)
    if type_xls == 'MAYRA':
        dict_session['ABONO'] =  {k: dict_session['ABONO'].get(k, 0) + dict_session['INTERÉS'].get(k, 0) for k in set(dict_session['ABONO']) | set(dict_session['INTERÉS'])}

    return dict_session


def merge_sesion_dicts_df(dicts_sesion, tipo):

    #Combina diccionarios de un tipo (ej. ABONO, ABONO2, ABONO3) en un solo dataframe por usuario

    df = pd.DataFrame.from_dict(dicts_sesion[0], orient='index', columns=[tipo])

    # Mezcla los valores del 2 y 3 en el 1
    for idx,dict_sesion in enumerate(dicts_sesion[1:]):
        df_2 = pd.DataFrame.from_dict(dict_sesion, orient='index', columns=[tipo+str(idx+2)])
        df = pd.merge(df, df_2, how='outer',left_index=True, right_index=True)

    # Nan a 0s y una sola columna con listas
    df = df.fillna(0)
    df[tipo+'S']= df.values.tolist()
    df = df[tipo+'S'].to_frame()

    return df

def insert_id_users_dict_session(dict_session, dict_users):

    id_users_dict = dict()
    for key in dict_session.keys():
        dict_specific = dict_session[key]
        id_users_dict[key] = {dict_users.get(k, k): v for k, v in dict_specific.items()}

    return id_users_dict


def chdir_to_root():

    root_dir_found = 0
    conf_file_found = 0
    while 1:
        
        current_dir = os.getcwd()
        cosecha_colectiva_dir = pathlib.Path(current_dir,'cosecha_colectiva')
        if os.path.isdir(cosecha_colectiva_dir):
            root_dir_found = 1
            if os.path.isfile(pathlib.Path(current_dir,'dj_local_conf.json')):
                conf_file_found = 1
        if root_dir_found:
            break
        os.chdir('..')
        new_current_dir = os.getcwd()
        if str(current_dir) == str(new_current_dir):
            break

    return root_dir_found, conf_file_found


def try_find_conf_file():

    root_dir_found, conf_file_found = chdir_to_root()
    if root_dir_found and conf_file_found:
        print('Local configuration file found !!, no need to run the configuration (unless configuration has changed)')
    elif root_dir_found:
        print('Local configuration file not found. Ignore this if you have a global config. Run configuration notebook otherwise')
    else:
        print('Root dir not found, change this notebook to the project folder')


def transform_dict_query(list_q):

    #Transforma una lista de diccionarios con una sola llave en query para borrar

    if list_q:
        key = list(list_q[0].keys())[0]
        
        str_list = ', '.join(str(e[key]) for e in list_q)
        final_str = key + ' in (' + str_list + ')'

    return final_str