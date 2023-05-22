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

def get_socios_grupo(id_grupo, as_orderded_dict=True):

    socio_id_list = (cosecha_db.GrupoSocio & "Grupo_id ="+str(id_grupo)).fetch('Socio_id', order_by='Socio_id', as_dict=True)

    if as_orderded_dict:
        socio_id_dict = dict()
        for count, user in enumerate(socio_id_list):    
            socio_id_dict[count] = user['Socio_id']
        return socio_id_dict

    return socio_id_list

def get_socios_acciones_grupo(id_grupo):

    socio_id_list = (cosecha_db.GrupoSocio & "Grupo_id ="+str(id_grupo)).fetch(*config.columnas_socio_accion, order_by='Socio_id', as_dict=True)

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
        raise Exception('No hay grupo con el nombre')
    else:
        grupo_id = grupo_id[0]['Grupo_id']
        return grupo_id
    
def restart_grupo_acciones(id_grupo):

    grupo_q = dict()
    grupo_q['Grupo_id'] = id_grupo

    grupo_socios = (cosecha_db.GrupoSocio & grupo_q).fetch('KEY', as_dict=True)

    dict_acuerdos = get_acuerdos_grupo(id_grupo)

    minimo_aportacion = float(dict_acuerdos['Minimo_aportacion'])

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
    caja = float(caja[0])

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

def delete_grupo(id_grupo, solo_sesiones=False, force_delete=False):

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
    if solo_sesiones:
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
        restart_grupo_acciones(id_grupo)




