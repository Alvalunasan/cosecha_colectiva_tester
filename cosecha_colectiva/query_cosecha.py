import datajoint as dj
import time
import pandas as pd

from cosecha_colectiva import miscelaneous as ms
from cosecha_colectiva import config

cosecha_db = dj.create_virtual_module(config.db_name, config.db_name)


def get_acuerdos_grupo(id_grupo):

    query = dict()
    query['Grupo_id'] = id_grupo
    query['Status'] = 1

    dict_acuerdos = (cosecha_db.Acuerdos & query).fetch(as_dict = True)

    return dict_acuerdos[0]

def get_all_acuerdos(id_grupo):

    query = dict()
    query['Grupo_id'] = id_grupo

    df_acuerdos = pd.DataFrame((cosecha_db.Acuerdos & query).fetch(as_dict = True))

    return df_acuerdos

def set_acuerdos_interes_0(id_grupo):

    query = dict()
    query['Grupo_id'] = id_grupo
    query['Status'] = 1

    acuerdo_id = (cosecha_db.Acuerdos & query).fetch('KEY', as_dict = True)
    acuerdo_id = acuerdo_id[0]['Acuerdo_id']

    query2 = dict()
    query2['Acuerdo_id'] = acuerdo_id
    query2['Tasa_interes'] = 0

    print(query2)

    cosecha_db.Acuerdos().update1(query2)

def write_first_acciones_socio(id_grupo, id_socio, acciones):

    query = dict()
    query['Grupo_id'] = id_grupo
    query['Socio_id'] = id_socio

    id_grupo_socio = (cosecha_db.GrupoSocio & query).fetch('KEY', as_dict = True)
    id_grupo_socio = id_grupo_socio[0]
    id_grupo_socio['Acciones'] = acciones

    cosecha_db.GrupoSocio().update1(id_grupo_socio)

def overwrite_ganancias_socio(id_socio, sesion_id, ganancias):

    query = dict()
    query['Sesion_id'] = sesion_id
    query['Socio_id'] = id_socio

    id_ganancia = (cosecha_db.Ganancias & query).fetch('KEY', as_dict = True)
    id_ganancia = id_ganancia[0]
    id_ganancia['Monto_ganancia'] = ganancias

    cosecha_db.Ganancias().update1(id_ganancia)

def overwrite_acuerdos_prestamos(df_idx_acuerdos, grupo_id, sesion_list):

    if df_idx_acuerdos.shape[0] > 0:
        prestamo_id_df = get_prestamo_n_socios(list(df_idx_acuerdos.index.to_list()), sesion_list)

        df_idx_acuerdos = pd.merge(df_idx_acuerdos, prestamo_id_df, how='inner', left_index=True, right_index=True)

        print(df_idx_acuerdos)

        #df_idx_acuerdos = df_idx_acuerdos.reset_index()

        df_acuerdos  = get_all_acuerdos(grupo_id)

        print(df_acuerdos)

        for socio in df_idx_acuerdos.index.to_list():
                
            for idx_prestamo,idx_acuerdo in enumerate(df_idx_acuerdos.loc[socio,'PRÉSTAMO']):

                query_prestamo = dict()
                query_prestamo["Prestamo_id"] = df_idx_acuerdos.loc[socio,'Prestamo_id'][idx_prestamo]
                this_acuerdo = df_acuerdos.loc[idx_acuerdo-1, 'Acuerdo_id']
                query_prestamo["Acuerdos_id"] = this_acuerdo

                print(query_prestamo)

                cosecha_db.Prestamos().update1(query_prestamo)
                time.sleep(1)


def write_first_caja(id_sesion, caja):

    dict_caja = dict()
    dict_caja['Sesion_id'] = id_sesion
    dict_caja['Caja'] = caja

    cosecha_db.Sesiones.update1(dict_caja)

    dict_acciones = dict()
    dict_acciones['Sesion_id'] = id_sesion
    dict_acciones['Acciones'] = caja

    cosecha_db.Sesiones.update1(dict_acciones)

def get_active_sesion(id_grupo):

    query = dict()
    query['Grupo_id'] = id_grupo
    query['Activa'] = 1

    id_sesion = (cosecha_db.Sesiones & query).fetch('KEY')

    if len(id_sesion) == 1:
        return id_sesion[0]['Sesion_id']
    elif len(id_sesion) == 0:
        print('ERROR: No hay sesiones activas para el grupo ', id_grupo)
        return -1
    else:
        print('ERROR: Hay más de una sesion activas para el grupo ', id_grupo)
        return id_sesion[-1]
    
def get_all_sesiones_grupo(id_grupo):

    query = dict()
    query['Grupo_id'] = id_grupo

    sesion_list = list((cosecha_db.Sesiones & query).fetch('KEY', as_dict=False))
    sesion_list = [v[0] for v in sesion_list]
    return sesion_list

    
def get_codigo_grupo(id_grupo):

    codigo_grupo = (cosecha_db.Grupos & "Grupo_id ="+str(id_grupo)).fetch('Codigo_grupo', as_dict=True)
    if codigo_grupo:
        return codigo_grupo[0]
    else:
        return -1
    
def get_socio(username):

    query = dict()
    query['Username'] = username

    socio = (cosecha_db.Socios & query).fetch('KEY', as_dict=True)
    return socio

def get_socios_from_CURP_list(curp_list):

    query = 'CURP IN ("' + '", "'.join(curp_list) + '")'

    socio = (cosecha_db.Socios & query).fetch('KEY','CURP', as_dict=True)
    return socio

def get_socios_grupo(id_grupo, as_orderded_dict=True):

    socio_id_list = (cosecha_db.GrupoSocio & "Grupo_id ="+str(id_grupo)).fetch('Socio_id', order_by='Socio_id', as_dict=True)

    if as_orderded_dict:
        socio_id_dict = dict()
        for count, user in enumerate(socio_id_list):    
            socio_id_dict[count] = user['Socio_id']
        return socio_id_dict

    return socio_id_list

def get_socios_acciones_grupo(id_grupo):

    socio_id_list = ((cosecha_db.GrupoSocio * cosecha_db.Socios.proj('CURP')) & "Grupo_id ="+str(id_grupo)).fetch(*config.columnas_socio_accion_plus, order_by='Socio_id', as_dict=True)

    return socio_id_list

    
def get_multas(user_list):

    user_query = [{ "Socio_id" : user} for user in user_list]
    multas = (cosecha_db.Multas & user_query).fetch('KEY', 'Socio_id', as_dict=True)

    return multas

def get_prestamos(user_list):

    user_query = [{ "Socio_id" : user} for user in user_list]
    prestamos = (cosecha_db.Prestamos & user_query).fetch('KEY', 'Socio_id', as_dict=True)

    return prestamos

def get_prestamos_activos(user_list):

    user_query = [{ "Socio_id" : user, 'Estatus_prestamo': 0} for user in user_list]
    prestamos = (cosecha_db.Prestamos & user_query).fetch('KEY', 'Socio_id', order_by=['Prestamo_id', 'Socio_id'], as_dict=True)

    return prestamos

def get_prestamo_n_socios(user_list, sesion_list):

    user_query = [{ "Socio_id" : user, 'Estatus_prestamo': 0} for user in user_list]
    sesion_query = "Sesion_id in (" + ', '.join(str(v) for v in sesion_list) + ')'
    prestamos = pd.DataFrame((cosecha_db.Prestamos & user_query & sesion_query).fetch('KEY', 'Socio_id', order_by=['Socio_id', 'Fecha_inicial', 'Prestamo_id'], as_dict=True))

    if prestamos.shape[0] > 0:
        prestamos = prestamos.groupby('Socio_id')['Prestamo_id'].apply(list).to_frame()

    return prestamos

def get_multas_n_socios(user_list, sesion_list):

    user_query = [{ "Socio_id" : user, 'Status': 0} for user in user_list]
    sesion_query = "Sesion_id in (" + ', '.join(str(v) for v in sesion_list) + ')'
    multas = pd.DataFrame((cosecha_db.Multas & user_query & sesion_query).fetch('KEY', 'Socio_id', order_by=['Socio_id', 'Multa_id'], as_dict=True))

    if multas.shape[0] > 0:
        multas = multas.groupby('Socio_id')['Multa_id'].apply(list).to_frame()

    return multas

def get_grupo_id_by_name(group_name):

    query = dict()
    query['Nombre_grupo'] = group_name

    grupo_id = (cosecha_db.Grupos & query).fetch('KEY', as_dict=True)

    if len(grupo_id) > 1:
        print(grupo_id)
        raise Exception('Mas de 1 grupo con el mismo nombre')
    elif len(grupo_id) == 0:
        return [0, False]
        raise Exception('No hay grupo con el nombre')
    else:
        grupo_id = grupo_id[0]['Grupo_id']
        return [grupo_id, True]
    
def restart_grupo_acciones(id_grupo, desde_sesion_0=False):

    grupo_q = dict()
    grupo_q['Grupo_id'] = id_grupo
    grupo_socios = (cosecha_db.GrupoSocio & grupo_q).fetch('KEY', as_dict=True)

    if not desde_sesion_0:
        dict_acuerdos = get_acuerdos_grupo(id_grupo)
        minimo_aportacion = float(dict_acuerdos['Minimo_aportacion'])
    else:
        minimo_aportacion = 0

    for socio in grupo_socios:

        socio['Acciones'] = minimo_aportacion
        socio['Status'] = 1
        cosecha_db.GrupoSocio().update1(socio)

def get_sesiones_grupo(id_grupo):

    grupo_q = dict()
    grupo_q['Grupo_id'] = id_grupo
    sesiones = (cosecha_db.Sesiones & grupo_q).fetch('KEY', order_by='Sesion_id', as_dict=True)

    return sesiones

def get_caja_sesion(id_sesion):
    
    sesion_q = dict()
    sesion_q['Sesion_id'] = id_sesion
    caja = (cosecha_db.Sesiones & sesion_q).fetch('Caja', as_dict=False)
    if len(caja) > 0:
        caja = float(caja[0])
    else:
        caja = 0

    return caja

def get_ganancias_sesiones(sesiones_dict):

    ganancias = (cosecha_db.Ganancias & sesiones_dict).fetch(*config.columnas_ganancias.keys(), as_dict=True)

    return ganancias

def get_interes_prestamo(sesiones_dict):

    interes_prestamo = (cosecha_db.InteresPrestamo & sesiones_dict).fetch(*config.columnas_interes_prestamo.keys(), as_dict=True)

    return interes_prestamo

def get_prestamos_sesiones(sesiones_dict, id_grupo):
    
    acuerdo_proj = cosecha_db.Acuerdos.proj(Acuerdos_id='Acuerdo_id', *config.columnas_acuerdos)
    prestamos = pd.DataFrame((cosecha_db.Prestamos * acuerdo_proj & sesiones_dict).fetch(*config.columnas_prestamos, as_dict=True))

    if prestamos.shape[0] > 0:
        socios_df  = pd.DataFrame((cosecha_db.GrupoSocio.proj('Socio_id', Status='Status_socio') & ("Grupo_id="+str(id_grupo))).fetch(as_dict=True))
        prestamos = pd.merge(prestamos, socios_df, how='inner', on='Socio_id')

    return prestamos

def get_interes_prestamo_sesiones(sesiones_dict):

    interes_prestamos = (cosecha_db.InteresPrestamo & sesiones_dict).fetch(*config.columnas_interes_prestamo.keys(), as_dict=True)

    return interes_prestamos

def get_multas_sesiones(sesiones_dict):
    
    multas = (cosecha_db.Multas & sesiones_dict).fetch(*config.columnas_multa, as_dict=True)

    return multas

def get_ultimo_prestamo():

    ultimo_prestamo  = (cosecha_db.Prestamos).fetch('KEY', order_by='Prestamo_id DESC', limit=1)
    ultimo_prestamo = ultimo_prestamo[0]['Prestamo_id']

    return ultimo_prestamo

def get_next_autoincrement_table(table_name):

    query= 'SELECT AUTO_INCREMENT FROM information_schema.TABLES where TABLE_NAME = "' + table_name + '"'
    con = dj.conn()
    return con.query(query).fetchone()[0]

def sobreescribe_acciones(df_acciones, id_grupo, costo_accion):

    socio_list = df_acciones.index.to_list()
    for idx_socio in range(df_acciones.shape[0]):

        socio = socio_list[idx_socio]
        acciones = df_acciones.loc[socio, 'COMPRA_ACCIONES'][0]*costo_accion

        write_first_acciones_socio(id_grupo, socio, acciones)

def sobreescribe_caja_acciones_sesion(df_acciones, sesion_id, costo_accion):

    socio_list = df_acciones.index.to_list()
    acciones = 0
    for idx_socio in range(df_acciones.shape[0]):
        socio = socio_list[idx_socio]
        acciones += df_acciones.loc[socio, 'COMPRA_ACCIONES'][0]*costo_accion

    query = dict()
    query['Sesion_id'] = sesion_id
    query['Caja'] = acciones
    query['Acciones'] = acciones

    cosecha_db.Sesiones().update1(query)

def abona_a_caja(monto_abono, sesion_id, overwrite=False):

    query = dict()
    query['Sesion_id'] = sesion_id

    caja = (cosecha_db.Sesiones & query).fetch('Caja', as_dict = True)

    query2 = dict()
    query2['Sesion_id'] = sesion_id
    if overwrite:
        query2['Caja'] = monto_abono
    else:
        query2['Caja'] = caja[0]['Caja'] + monto_abono

    cosecha_db.Sesiones().update1(query2)

def sobreescribe_ganancias(df_ganancias, sesion_id):

    socio_list = df_ganancias.index.to_list()
    for idx_socio in range(df_ganancias.shape[0]):

        socio = socio_list[idx_socio]
        ganancias = df_ganancias.loc[socio, 'GANANCIAS'][0]

        overwrite_ganancias_socio(socio, sesion_id, ganancias)

def suma_abonos(df_abono):

    socio_list = df_abono.index.to_list()
    abono = 0
    for idx_socio in range(df_abono.shape[0]):

        socio = socio_list[idx_socio]
        abono += abono.loc[socio, 'ABONO'][0]

    return abono

def delete_grupo(id_grupo, solo_sesiones=False, force_delete=False, desde_sesion_0=False):

    base_query = 'DELETE from ' + config.db_name + '.'

    ordered_tablas_name = ['transaccion_prestamos', 'transacciones', 'interes_prestamo', 'ganancias',
                'multas', 'prestamos', 'prestamos', 'asistencias', 'sesiones', 'acuerdos']
    
    if not solo_sesiones:
        ordered_tablas_name = ordered_tablas_name + ['grupo_socio', 'grupos']

    grupo_q = dict()
    grupo_q['Grupo_id'] = id_grupo

    grupo = (cosecha_db.Grupos & grupo_q).fetch('KEY', as_dict=True)

    grupo_socios = (cosecha_db.GrupoSocio & grupo_q).fetch('KEY', as_dict=True)

    acuerdos = (cosecha_db.Acuerdos & grupo_q).fetch('KEY', as_dict=True)

    #Selecciona todas las sesiones
    sesiones = (cosecha_db.Sesiones & grupo_q).fetch('KEY', order_by='Sesion_id', as_dict=True)
    #Conservar la primera sesión (compras iniciales de la caja)
    if solo_sesiones and not desde_sesion_0:
        sesiones = sesiones[1:]

    acuerdos_del = acuerdos

    if len(acuerdos) > 1 and solo_sesiones:
            acuerdos_del = acuerdos[1:]
    if len(acuerdos) == 1 and solo_sesiones:
            acuerdos_del = []

    asistencias = (cosecha_db.Asistencias & sesiones).fetch('KEY', as_dict=True)
    prestamos = (cosecha_db.Prestamos & sesiones).fetch('KEY', as_dict=True)

    prestamos_ampliacion = (cosecha_db.Prestamos & sesiones & 'Prestamo_original_id IS NOT NULL').fetch('KEY', as_dict=True)

    multas = (cosecha_db.Multas & sesiones).fetch('KEY', as_dict=True)
    ganancias = (cosecha_db.Ganancias & sesiones).fetch('KEY', as_dict=True)
    interes_prestamos = (cosecha_db.InteresPrestamo & sesiones).fetch('KEY', as_dict=True)
    transactions = (cosecha_db.Transacciones & sesiones).fetch('KEY', as_dict=True)

    transactions_prestamo = (cosecha_db.TransaccionPrestamos & transactions).fetch('KEY', as_dict=True)

    lista_todo = [transactions_prestamo, transactions, interes_prestamos, ganancias, 
                  multas, prestamos_ampliacion, prestamos, asistencias, sesiones, acuerdos_del]
    
    if not solo_sesiones:
        lista_todo = lista_todo + [grupo_socios, grupo]

    con = dj.conn()
    cont = 0
    for rubro in zip(lista_todo, ordered_tablas_name):
        cont += 1
        print(rubro)
        if rubro[0]:
            where_q = ms.transform_dict_query(rubro[0])
            query = base_query + rubro[1] + ' WHERE ' + where_q
            print(query)
            if not force_delete:
                time.sleep(0.1)
                keyboard = input("Press Enter to continue, (c) Entrer to cancel")
                if 'c' in keyboard:
                    break

            con.query(query)
            time.sleep(0.1)

    if solo_sesiones and len(acuerdos) > 1:
        query_update_acuerdo = {'Acuerdo_id': acuerdos[0]['Acuerdo_id'],
                                'Status': 1}

        cosecha_db.Acuerdos.update1(query_update_acuerdo)

    if  solo_sesiones: 
        restart_grupo_acciones(id_grupo, desde_sesion_0)

def insert_grupo(grupo_data):

    (cosecha_db.Grupos).insert(grupo_data)
    time.sleep(0.1)
    grupo_id = cosecha_db.Grupos.fetch('KEY', order_by='Grupo_id desc', limit=1)
    return grupo_id[0]


def prepare_insert_socio_grupo(xls_name, id_grupo):

    socios_xls = pd.DataFrame(ms.get_dict_usuarios_xls(xls_name))
    curp_list = socios_xls['CURP'].to_list()

    order_socios = socios_xls['CURP'].to_frame().reset_index()
    order_socios = order_socios.rename(columns={'index': 'xls_order'})

    socios_bd = pd.DataFrame(get_socios_from_CURP_list(curp_list))

    aux_df = pd.merge(left=socios_bd, right=order_socios, on='CURP', how='inner')
    aux_df = aux_df.sort_values(by='xls_order')
    aux_df = aux_df.reset_index(drop=True)

    aux_df['Tipo_socio'] = "SOCIO"
    aux_df.loc[0,'Tipo_socio'] = "ADMIN"
    aux_df.loc[1,'Tipo_socio'] = "SUPLENTE"

    aux_df['Status'] = 1
    aux_df['Acciones'] = 0
    aux_df['Grupo_id'] = id_grupo
    aux_df['Grupo_socio_id'] = 0

    aux_df = aux_df[config.columnas_socio_accion]
    aux_df = aux_df.drop('Grupo_socio_id', axis=1)

    insert_grupo_socio(aux_df.to_dict('records'))

    return aux_df

def insert_grupo_socio(grupo_socio_data):

    (cosecha_db.GrupoSocio).insert(grupo_socio_data)
    time.sleep(0.1)

def insert_acuerdos(acuerdos_data):

    (cosecha_db.Acuerdos).insert1(acuerdos_data)
    time.sleep(0.1)


def insert_sesion(sesion_data):


    (cosecha_db.Sesiones).insert1(sesion_data)
    time.sleep(0.1)
    return get_active_sesion(sesion_data['Grupo_id'])


def insert_prestamo(prestamo_data):

    (cosecha_db.Prestamos).insert(prestamo_data)
    time.sleep(0.1)
    prestamo_ids = cosecha_db.Prestamos.fetch('KEY', order_by='Prestamo_id desc', limit=len(prestamo_data))
    prestamo_ids = pd.DataFrame(prestamo_ids)
    prestamo_ids = prestamo_ids.sort_values(by='Prestamo_id').reset_index(drop=True)
    return prestamo_ids

def insert_multa(multa_data):

    (cosecha_db.Multas).insert(multa_data)
    time.sleep(0.1)
    multa_ids = cosecha_db.Multas.fetch('KEY', order_by='Multa_id desc', limit=len(multa_data))
    multa_ids = pd.DataFrame(multa_ids)
    multa_ids = multa_ids.sort_values(by='Multa_id').reset_index(drop=True)
    return multa_ids

def insert_transaccion(transaccion_data):

    (cosecha_db.Transacciones).insert(transaccion_data)
    time.sleep(0.1)
    transaccion_ids = cosecha_db.Transacciones.fetch('KEY', order_by='Transaccion_id desc', limit=len(transaccion_data))
    transaccion_ids = pd.DataFrame(transaccion_ids)
    transaccion_ids = transaccion_ids.sort_values(by='Transaccion_id').reset_index(drop=True)
    return transaccion_ids

def insert_transaccion_prestamo(transaccion_prestamo_data):

    (cosecha_db.TransaccionPrestamos).insert(transaccion_prestamo_data)
    time.sleep(0.1)
    transaccion_prestamo_ids = cosecha_db.TransaccionPrestamos.fetch('KEY', order_by='Transaccion_prestamo_id desc',
                                                                     limit=len(transaccion_prestamo_data))
    transaccion_prestamo_ids = pd.DataFrame(transaccion_prestamo_ids)
    transaccion_prestamo_ids = transaccion_prestamo_ids.sort_values(by='Transaccion_prestamo_id').reset_index(drop=True)
    return transaccion_prestamo_ids

def insert_interes_prestamo(interes_prestamo_data):

    (cosecha_db.InteresPrestamo).insert(interes_prestamo_data)
    time.sleep(0.1)
    interes_prestamo_ids = cosecha_db.InteresPrestamo.fetch('KEY', order_by='Interes_prestamo_id desc',
                                                                     limit=len(interes_prestamo_data))
    interes_prestamo_ids = pd.DataFrame(interes_prestamo_ids)
    interes_prestamo_ids = interes_prestamo_ids.sort_values(by='Interes_prestamo_id').reset_index(drop=True)
    return interes_prestamo_ids

def insert_ganancia(ganancia_data):

    (cosecha_db.Ganancias).insert(ganancia_data)
    time.sleep(0.1)
    ganancia_ids = cosecha_db.Ganancias.fetch('KEY', order_by='Ganancias_id desc', limit=len(ganancia_data))
    ganancia_ids = pd.DataFrame(ganancia_ids)
    ganancia_ids = ganancia_ids.sort_values(by='Ganancias_id').reset_index(drop=True)
    return ganancia_ids

def insert_asistencia(asistencia_data):

    (cosecha_db.Asistencias).insert(asistencia_data)
    time.sleep(0.1)

def update_socio_acciones(socio_acciones):

    for i in range(len(socio_acciones)):

        query = dict()
        query['Grupo_socio_id'] = socio_acciones[i]['Grupo_socio_id']
        query['Acciones'] = socio_acciones[i]['Acciones']

        (cosecha_db.GrupoSocio).update1(query)

def update_sesion(sesion_id=None, caja=None, acciones=None, ganancias=None, activa=0):
            
    query = dict()
    query['Sesion_id'] = sesion_id
    query['Caja'] = caja
    query['Acciones'] = acciones
    query['Ganancias'] = ganancias
    query['Activa'] = activa

    (cosecha_db.Sesiones).update1(query)

def update_prestamo(prestamo_data):
            
    (cosecha_db.Prestamos).update1(prestamo_data)

def update_multa(multa_data):
            
    (cosecha_db.Multas).update1(multa_data)