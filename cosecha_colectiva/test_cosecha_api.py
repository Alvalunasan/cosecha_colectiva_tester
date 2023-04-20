
import pandas as pd
import pathlib
import json
import requests
import time
import logging

from cosecha_colectiva import config
from cosecha_colectiva import miscelaneous as ms
from cosecha_colectiva import query_cosecha as qc


class CAF_API_general():

    @staticmethod
    def user_login(username, password):

        logging.info('API CALL, User Login: %s', username)

        dict_payload = {"Username": username, "Password": password}
        response = requests.request("POST", config.url_dict['url_login'], headers=config.default_headers, data=json.dumps(dict_payload))

        if not response:
            logging.error('%s', response.text)
            raise Exception('Login incorrecto')

        response_dict = json.loads(response.text)
        this_token = response_dict['token']
        user_header = config.default_headers.copy()
        user_header["Authorization"] = this_token
        socio_id = response_dict['data']['Socio_id']

        return user_header, socio_id
    
    @staticmethod
    def get_group_id_user(username, password):

        logging.info('API CALL, Get gruop user: %s', username)

        user_header,_ = CAF_API_general.user_login(username, password)
        response = requests.request("GET", config.url_dict['url_socio_grupo'], headers=user_header, data={})

        if not response:
            logging.error('%s', response.text)
            raise Exception('No se pudo obtener grupo id del usuario ')
        
        dict_response = json.loads(response.text)
        id_grupo = dict_response["data"][0]["Grupo_id"]

        return id_grupo
    
    @staticmethod
    def login_first_user_excel(xls_name):

        dict_df_grupo = ms.get_dict_usuarios_xls(xls_name)
        user_header, socio_id = CAF_API_general.user_login(dict_df_grupo[0]['Username'], dict_df_grupo[0]['Password'])
        return user_header, socio_id
    
    @staticmethod
    def get_all_users_id(xls_name):

        all_users_id = dict()
        dict_df_grupo = ms.get_dict_usuarios_xls(xls_name)
        for count, user in enumerate(dict_df_grupo):    
            _, socio_id = CAF_API_general.user_login(user['Username'], user["Password"])
            all_users_id[count] = socio_id
        return all_users_id
    
    @staticmethod
    def get_grupo_id_admin_header_xls(xls_name):

        dict_df_grupo = ms.get_dict_usuarios_xls(xls_name)
        id_grupo = CAF_API_general.get_group_id_user(dict_df_grupo[0]['Username'],dict_df_grupo[0]['Password'])
        admin_header, _ = CAF_API_general.user_login(dict_df_grupo[0]['Username'], dict_df_grupo[0]['Password'])

        return id_grupo, admin_header
    

class CAF_API_group_creation_tester():

    @staticmethod
    def create_users(xls_name):

        logging.info('API CALL, Crear usuarios: %s', xls_name)

        dict_df_grupo = ms.get_dict_usuarios_xls(xls_name)
        for user in dict_df_grupo:

            user_in_db = qc.get_socio(user['Username'])

            if user_in_db:
                logging.info('Usuario ya existe: %s', user_in_db)
                continue

            payload = json.dumps(user)
            time.sleep(0.1) 
            response = requests.request("POST", config.url_dict['url_socio'], headers=config.default_headers, data=payload)

            if not response:
                logging.error('%s', response.text)
                raise Exception('No se pudo crear el usuario ')

    
    @staticmethod
    def create_group(xls_name, user_header):

        logging.info('API CALL, Crear grupo: %s', xls_name)

        idx_dot = xls_name.find('.')
        nombre_grupo = xls_name[0:idx_dot]
        dict_grupo = config.default_group_data
        dict_grupo["Nombre_grupo"] = nombre_grupo
        payload = json.dumps(dict_grupo)
        response = requests.request("POST", config.url_dict['url_grupo'], headers=user_header, data=payload)

        if not response:
            logging.error('%s', response.text)
            raise Exception('No se pudo crear el grupo ')

        id_grupo = json.loads(response.text)['data']
        id_grupo = id_grupo['Grupo_id']
        
        return id_grupo


    @staticmethod
    def add_user_group(username, password, codigo_grupo_dict):

        logging.info('API CALL, Agrega usuario a grupo : %s %s', username, codigo_grupo_dict)

        user_header,_ = CAF_API_general.user_login(username, password)
        time.sleep(1)
        response = requests.request("POST", config.url_dict['url_socio_grupo'], headers=user_header, data=json.dumps(codigo_grupo_dict))
        time.sleep(1)
        if not response:
            logging.error('%s', response.text)
            raise Exception('No se pudo unir el usuario al grupo ')

    @staticmethod
    def add_xls_users_group(xls_name, id_grupo):

        codigo_grupo = qc.get_codigo_grupo(id_grupo)

        dict_df_grupo = ms.get_dict_usuarios_xls(xls_name)
        #El primer socio ya está en el grupo, se elimina de la lista
        dict_df_grupo = dict_df_grupo[1:]
        for user in dict_df_grupo:
            CAF_API_group_creation_tester.add_user_group(user['Username'], user["Password"], codigo_grupo)

    
    @staticmethod
    def create_acuerdos(xls_name, idx_acuerdos=0):

        logging.info('API CALL, Crear acuerdos: %s', xls_name)

        dict_df_grupo = ms.get_dict_usuarios_xls(xls_name)

        admin_header, id_admin = CAF_API_general.user_login(dict_df_grupo[0]['Username'],dict_df_grupo[0]['Password'])

        id_grupo = CAF_API_general.get_group_id_user(dict_df_grupo[0]['Username'],dict_df_grupo[0]['Password'])
        _, id_suplente = CAF_API_general.user_login(dict_df_grupo[1]['Username'],dict_df_grupo[1]['Password'])

        dict_df_acuerdos = ms.get_dict_acuerdos_xls(xls_name, idx_acuerdos=idx_acuerdos)
        dict_df_acuerdos['Id_socio_administrador'] = id_admin
        dict_df_acuerdos['Id_socio_administrador_suplente'] = id_suplente


        url_acuerdos = config.url_dict['url_acuerdos'].format(id_grupo=id_grupo)
        response = requests.request("POST", url_acuerdos, headers=admin_header, data=json.dumps(dict_df_acuerdos))

        if not response:
            logging.error('%s', response.text)
            raise Exception('No se pudieron crear los acuerdos ')

    @staticmethod
    def acciones_iniciales(xls_name, id_grupo, id_sesion):

        logging.info('"Compra" acciones iniciales %s %d', xls_name, id_grupo)

        dict_acuerdos = qc.get_acuerdos_grupo(id_grupo)

        #dict_socios = CAF_API_general.get_all_users_id(xls_name)
        dict_socios = qc.get_socios_grupo(id_grupo)
        list_socios = list(dict_socios.values())

        for socio in list_socios:
            qc.write_first_acciones_socio(id_grupo, socio, dict_acuerdos['Minimo_aportacion'])
        
        caja_final =  dict_acuerdos['Minimo_aportacion']*len(list_socios)
        qc.write_first_caja(id_sesion, caja_final)

    @staticmethod
    def main_create_group(xls_name):

        logging.info('Proceso creación grupo %s', xls_name)

        CAF_API_group_creation_tester.create_users(xls_name)
        time.sleep(0.1)
        admin_header,_ = CAF_API_general.login_first_user_excel(xls_name)
        time.sleep(0.1)
        id_grupo = CAF_API_group_creation_tester.create_group(xls_name, admin_header)
        time.sleep(0.1)
        CAF_API_group_creation_tester.add_xls_users_group(xls_name, id_grupo)
        time.sleep(0.1)
        CAF_API_sessions_tester.create_session(id_grupo, admin_header)

        id_sesion = qc.get_active_sesion(id_grupo)

        time.sleep(0.1)
        CAF_API_group_creation_tester.create_acuerdos(xls_name)
        time.sleep(0.1)
        CAF_API_group_creation_tester.acciones_iniciales(xls_name, id_grupo, id_sesion)
        time.sleep(0.1)
        CAF_API_sessions_tester.end_session(id_grupo, admin_header)


class CAF_API_sessions_tester():

    @staticmethod
    def create_session(id_grupo, admin_header):

        logging.info('API CALL, Crear sesion %d', id_grupo)

        payload_session = CAF_API_sessions_tester.create_session_payload(id_grupo)
        url_crear_sesion = config.url_dict['url_crear_sesion'].format(id_grupo=id_grupo)
        response = requests.request("POST", url_crear_sesion, headers=admin_header, data=json.dumps(payload_session))

        if not response:
            logging.error('%s', response.text)
            raise Exception('No se pudo crear sesion ')

    @staticmethod
    def end_session(id_grupo, admin_header):

        logging.info('API CALL, Finalizar sesion %d', id_grupo)

        url_finalizar_sesion = config.url_dict['url_finalizar_sesion'].format(id_grupo=id_grupo)
        response = requests.request("POST", url_finalizar_sesion, headers=admin_header, data={})

        if not response:
            logging.error('%s', response.text)
            raise Exception('No se pudo terminar sesion ')

    @staticmethod
    def create_session_payload(id_grupo):

        #Consulta todos los socios del grupo
        socios_dict = qc.get_socios_grupo(id_grupo, as_orderded_dict=False)

        payload_sesion = dict()
        payload_sesion["Socios"] = list()
        for user in socios_dict:
            user_dict = dict()
            user_dict["Socio_id"] = int(user["Socio_id"])
            user_dict["Presente"] = 1
            payload_sesion["Socios"].append(user_dict)

        return payload_sesion
    
    @staticmethod
    def compra_acciones(df_acciones, id_grupo, admin_header, costo_accion):

        logging.info('API CALL, Compra acciones: %s', df_acciones)

        for idx_socio in range(0, df_acciones.shape[0]):

            socio = df_acciones.index[idx_socio]
            payload = dict()
            payload["Cantidad"] = df_acciones.loc[socio, 'COMPRA_ACCIONES'][0]*costo_accion

            url_compra_accion = config.url_dict['url_compra_acciones'].format(id_grupo=id_grupo, id_socio=socio)

            logging.debug('Payload Compra acciones: %s', payload)
            logging.debug('url_compra_accion: %s', url_compra_accion)

            response = requests.request("POST", url_compra_accion, headers=admin_header, data=json.dumps(payload))

            if not response:
                logging.error('%s', response.text)
                raise Exception('No se pudo comprar acciones ')

            time.sleep(1)

    @staticmethod
    def retiro_acciones(df_retiro_acciones, id_grupo, admin_header, costo_accion):

        logging.info('API CALL, Compra acciones: %s', df_retiro_acciones)

        for idx_socio in range(0, df_retiro_acciones.shape[0]):

            socio = df_retiro_acciones.index[idx_socio]
            payload = dict()
            payload["Cantidad"] = df_retiro_acciones.loc[socio, 'RETIRO_ACCIONES'][0]*costo_accion

            url_retiro_accion = config.url_dict['url_retiro_acciones'].format(id_grupo=id_grupo, id_socio=socio)

            logging.debug('Payload Compra acciones: %s', payload)
            logging.debug('url_compra_accion: %s', url_retiro_accion)

            response = requests.request("POST", url_retiro_accion, headers=admin_header, data=json.dumps(payload))

            if not response:
                logging.error('%s', response.text)
                raise Exception('No se pudo retirar acciones ')

            time.sleep(1)

    @staticmethod
    def pagar_multas_multiples(df_multas, id_grupo, sesion_list, admin_header):

        if df_multas.shape[0] > 0:

            logging.info('API CALL, Pagar multas n: %s', df_multas)

            multas_id_df = qc.get_multas_n_socios(list(df_multas.index.to_list()), sesion_list)

            logging.debug('multas_df %s', multas_id_df)

            df_multas = pd.merge(df_multas, multas_id_df, how='inner', left_index=True, right_index=True)

            logging.debug('df_multas final%s', df_multas)
                        
            for socio in df_multas.index.to_list():
                
                payload = dict()
                payload['Multas'] = list()

                for idx_multa,bool_pago in enumerate(df_multas.loc[socio,'PAGO_MULTA']):

                    if bool_pago == 1:
                        this_multa_id = df_multas.loc[socio,'Multa_id'][idx_multa]
                        payload['Multas'].append(this_multa_id)
            
                url_pagar_multa = config.url_dict['url_pagar_multa'].format(id_grupo=id_grupo)

                logging.debug('payload pagar multa %s', payload)

                response = requests.request("PATCH", url_pagar_multa, headers=admin_header, data=json.dumps(payload))

                if not response:
                    logging.error('%s', response.text)
                    raise Exception('No se pudo pagar multa ')

                time.sleep(1)

    @staticmethod
    def insertar_multa(df_multa, id_grupo, admin_header):

        logging.info('API CALL, Insertar multa: %s', df_multa)

        for idx_socio in range(0, df_multa.shape[0]):

            socio = df_multa.index[idx_socio]

            for idx_multa, monto_multa in enumerate(df_multa.loc[socio,'MULTAS']):

                payload = dict()
                payload["Monto_multa"] = float(monto_multa)
                payload["Descripcion"] = "Multa # " + str(idx_multa+1) + " socio "  + str(socio) + " grupo: " +  str(id_grupo)

                url_crear_multa = config.url_dict['url_crear_multa'].format(id_grupo=id_grupo, id_socio=socio)

                logging.debug('Payload Insertar Multa acciones: %s', payload)
                logging.debug('url_crear_multa: %s', url_crear_multa)

                response = requests.request("POST", url_crear_multa, headers=admin_header, data=json.dumps(payload))

                if not response:
                    logging.error('%s', response.text)
                    raise Exception('No se pudo insertar multa ')

                time.sleep(1)

    @staticmethod
    def pagar_prestamos_multiples(df_abono, id_grupo, sesion_list, admin_header):

        if df_abono.shape[0] > 0:

            logging.info('API CALL, Pagar prestamo n: %s', df_abono)

            prestamo_id_df = qc.get_prestamo_n_socios(list(df_abono.index.to_list()), sesion_list)

            logging.debug('prestamo_id_df %s', prestamo_id_df)

            df_abono = pd.merge(df_abono, prestamo_id_df, how='inner', left_index=True, right_index=True)

            logging.debug('DF_ABONO final%s', df_abono)
                        
            for socio in df_abono.index.to_list():
                
                payload = dict()
                payload['Prestamos'] = list()

                #idx_prestamo = 0
                for idx_abono,monto in enumerate(df_abono.loc[socio,'ABONO']):

                    if monto > 0:
                        this_prestamo = dict()
                        this_prestamo["Prestamo_id"] = df_abono.loc[socio,'Prestamo_id'][idx_abono]
                        this_prestamo["Monto_abono"] = df_abono.loc[socio,'ABONO'][idx_abono]
                        payload['Prestamos'].append(this_prestamo)
                        #idx_prestamo += 1
            

                url_pagar_prestamo = config.url_dict['url_pagar_prestamo'].format(id_grupo=id_grupo)

                logging.debug('payload pagar presatmos %s', payload)

                response = requests.request("PATCH", url_pagar_prestamo, headers=admin_header, data=json.dumps(payload))

                if not response:
                    logging.error('%s', response.text)
                    raise Exception('No se pudo pagar prestamo ')

                time.sleep(1)

    @staticmethod
    def generar_prestamos_multiples(df_prestamo, id_grupo, sesion_list, admin_header):

        if df_prestamo.shape[0] > 0:

            logging.info('API CALL, Generar prestamo multiples: %s', df_prestamo)
            
            prestamo_id_df = qc.get_prestamo_n_socios(list(df_prestamo.index.to_list()), sesion_list)

            logging.debug('prestamo_id_df %s', prestamo_id_df)

            #revisa si existe algun préstamo ya vigente (por si se necesita ampliar)
            df_prestamo = pd.merge(df_prestamo, prestamo_id_df, how='left', left_index=True, right_index=True)

            logging.debug('DF_ABONO final%s', df_prestamo)
                        
            for socio_prestamo in df_prestamo.index.to_list():

                for idx_prestamo,monto in enumerate(df_prestamo.loc[socio_prestamo,'PRÉSTAMO']):
                
                    if monto > 0:
                        payload = dict()
                        payload['Monto_prestamo'] = monto
                        #Obtiene el numero de sesiones (por default 6)
                        num_sesiones = df_prestamo.loc[socio_prestamo,'NUM_SESIONES']
                        if isinstance(num_sesiones, list):
                            payload['Num_sesiones'] = num_sesiones[idx_prestamo]
                        else:
                            payload['Num_sesiones'] = 6
                        #Obtiene el valor de ampliacion (por default False)
                        ampliacion = df_prestamo.loc[socio_prestamo,'AMPLIACIÓN']
                        if isinstance(ampliacion, list):
                            ampliacion = bool(ampliacion[idx_prestamo])
                        else:
                            ampliacion = False

                        payload['Observaciones']  = "Prestamo socio " + str(socio_prestamo) + " grupo: " +  str(id_grupo)
                        
                        if ampliacion:
                            payload['Prestamo_original_id'] = df_prestamo.loc[socio_prestamo,'Prestamo_id'][idx_prestamo]

                        if ampliacion:
                            url_generar_prestamo = config.url_dict['url_ampliar_prestamo'].format(id_grupo=id_grupo, id_socio=socio_prestamo)
                        else:
                            url_generar_prestamo = config.url_dict['url_generar_prestamo'].format(id_grupo=id_grupo, id_socio=socio_prestamo)

                        logging.debug('url generar prestamo %s', url_generar_prestamo)
                        logging.debug('payload generar/ampliar prestamo %s', payload)

                        response = requests.request("POST", url_generar_prestamo, headers=admin_header, data=json.dumps(payload))

                        if not response:
                            logging.error('%s', response.text)
                            raise Exception('No se pudo generat prestamo ')

                        time.sleep(1)

    @staticmethod
    def main_create_sesion(xls_name, session_num, type_xls='MAYRA'):

        logging.info('Inicia Sesion: %s %s', xls_name, config.month_words[session_num-1])

        #Crear sesion
        id_grupo, admin_header = CAF_API_general.get_grupo_id_admin_header_xls(xls_name)
        CAF_API_sessions_tester.create_session(id_grupo, admin_header)

        sesion_list = qc.get_all_sesiones_grupo(id_grupo)

        # toma la info de la sesion del excel y lo "combina" con los id de usuarios
        #dict_users = CAF_API_general.get_all_users_id(xls_name)
        dict_users = qc.get_socios_grupo(id_grupo)
        dict_session = ms.read_transform_info_xls(xls_name, session_num, dict_users, type_xls=type_xls)
        logging.info(dict_session)

        # Se crearon acuerdos
        if dict_session['NUEVOS_ACUERDOS'].shape[0] > 0:
            idx_acuerdos = int(dict_session['NUEVOS_ACUERDOS'].iloc[0,0][0])
            CAF_API_group_creation_tester.create_acuerdos(xls_name, idx_acuerdos=idx_acuerdos)
            
        dict_acuerdos = qc.get_acuerdos_grupo(id_grupo)

        #Compra acciones
        CAF_API_sessions_tester.compra_acciones(dict_session["COMPRA_ACCIONES"], id_grupo, admin_header, dict_acuerdos['Costo_acciones'])

        #Multas
        CAF_API_sessions_tester.insertar_multa(dict_session['MULTAS'], id_grupo, admin_header)
        CAF_API_sessions_tester.pagar_multas_multiples(dict_session['PAGO_MULTA'], id_grupo, sesion_list, admin_header)

        #Pago prestamos
        CAF_API_sessions_tester.pagar_prestamos_multiples(dict_session['ABONO'],id_grupo, sesion_list, admin_header)

        #Generacion de prestamos
        CAF_API_sessions_tester.generar_prestamos_multiples(dict_session['PRÉSTAMO'], id_grupo, sesion_list, admin_header)

        #Retiro de acciones
        CAF_API_sessions_tester.retiro_acciones(dict_session['RETIRO_ACCIONES'], id_grupo, admin_header, dict_acuerdos['Costo_acciones'])
        
        #Finalizar sesion
        CAF_API_sessions_tester.end_session(id_grupo, admin_header)




'''
DEPRECATED, funciones mono prestamo, mono multa
   @staticmethod
    def pagar_multas(df_pago_multa, id_grupo, admin_header):

        logging.info('API CALL, Pagar multa: %s', df_pago_multa)

        multas_ids = qc.get_multas(list(dict_multa.keys()))

        prestamo_id_df = qc.get_prestamo_n_socios(list(df_abono.index.to_list()))

        logging.debug('prestamo_id_df %s', prestamo_id_df)

        df_abono = pd.merge(df_abono, prestamo_id_df, how='left', left_index=True, right_index=True)

        logging.debug('DF_ABONO final%s', df_abono)

        for idx_socio in range(0, df_pago_multa.shape[0]):

            payload = dict()
            payload["Multas"] = [int(multa_id['Multa_id'])]

            url_pagar_multa = config.url_dict['url_pagar_multa'].format(id_grupo=id_grupo)

            response = requests.request("PATCH", url_pagar_multa, headers=admin_header, data=json.dumps(payload))

            if not response:
                logging.error('%s', response.text)
                raise Exception('No se pudo pagar multa ')

            time.sleep(1)

    @staticmethod
    def generar_prestamo(dict_prestamo, id_grupo, admin_header):

        logging.info('API CALL, Generar prestamo: %s', dict_prestamo)
                       
        for socio_prestamo in dict_prestamo.items():
            
            payload = dict()
            payload['Monto_prestamo'] = socio_prestamo[1]
            payload['Num_sesiones'] = 6
            payload['Observaciones']  = "Prestamo socio " + str(socio_prestamo[0]) + " grupo: " +  str(id_grupo)
            payload['Estatus_ampliacion']  = False
            payload['Prestamo_original_id'] = None

            url_generar_prestamo = config.url_dict['url_generar_prestamo'].format(id_grupo=id_grupo, id_socio=socio_prestamo[0])

            print(payload)
            print(url_generar_prestamo)

            response = requests.request("POST", url_generar_prestamo, headers=admin_header, data=json.dumps(payload))

            if not response:
                logging.error('%s', response.text)
                raise Exception('No se pudo generat prestamo ')

            time.sleep(1)

    @staticmethod
    def pagar_prestamo(dict_abono, id_grupo, admin_header):

        logging.info('API CALL, Pagar prestamo: %s', dict_abono)

        prestamos_ids = qc.get_prestamos_activos(list(dict_abono.keys()))
                       
        for prestamo_id in prestamos_ids:
            
            payload = dict()
            payload['Prestamos'] = list()
            this_prestamo = dict()
            this_prestamo["Prestamo_id"] = [int(prestamo_id['Prestamo_id'])]
            this_prestamo["Monto_abono"] = dict_abono[prestamo_id['Socio_id']]
            payload['Prestamos'].append(this_prestamo)
           
            url_pagar_prestamo = config.url_dict['url_pagar_prestamo'].format(id_grupo=id_grupo)

            response = requests.request("PATCH", url_pagar_prestamo, headers=admin_header, data=json.dumps(payload))

            if not response:
                logging.error('%s', response.text)
                raise Exception('No se pudo pagar prestamo ')

            time.sleep(1)


'''