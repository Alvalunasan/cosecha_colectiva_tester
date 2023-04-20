

import pandas as pd
import logging
import numpy as np
from datetime import date, datetime

from cosecha_colectiva import miscelaneous as ms
from cosecha_colectiva import query_cosecha as qc
from cosecha_colectiva import config



class CAF_DB_Monitor():

    #Funciones estáticas primero

    @staticmethod
    def calcula_limite_credito_final(monto_prestamos, acciones, razon_limite_credito):

        limite_credito = acciones*razon_limite_credito - monto_prestamos
        return limite_credito
    
    @staticmethod
    def calcula_limite_credito_teorico(acciones, razon_limite_credito):

        limite_credito = acciones*razon_limite_credito
        return limite_credito
    
    @staticmethod
    def puede_pedir_prestamo(num_prestamos, creditos_simultaneos):

        if num_prestamos < creditos_simultaneos:
            return True
        else:
            return False
        
    @staticmethod
    def calcula_interes_futuro(series_prestamo):

        interes_ampliacion = 0
        if series_prestamo['Estatus_ampliacion'] == 1:
            interes_ampliacion = series_prestamo['Interes_ampliacion']

        interes_morosidad = 0
        if series_prestamo['Sesiones_restantes'] <= 0:
            interes_morosidad = series_prestamo['Interes_morosidad']

        interes_final = series_prestamo['Tasa_interes'] + interes_ampliacion + interes_morosidad

        monto_calculo = series_prestamo['Monto_prestamo']
        if series_prestamo['Mod_calculo_interes'] != 1:
            monto_calculo = monto_calculo - series_prestamo['Monto_pagado'] + series_prestamo['Interes_generado'] - series_prestamo['Interes_pagado']

        interes_futuro = monto_calculo*interes_final/100
        
        return interes_futuro



    def __init__(self, id_grupo) -> None:


        self.id_grupo = id_grupo
        self.acuerdos = pd.Series(qc.get_acuerdos_grupo(self.id_grupo))
        self.socios_acciones = pd.DataFrame(qc.get_socios_acciones_grupo(self.id_grupo))
        self.sesiones = qc.get_sesiones_grupo(self.id_grupo)

        self.ganancias = pd.DataFrame(qc.get_ganancias_sesiones(self.sesiones))
        if self.ganancias.shape[0] == 0:
            self.ganancias = pd.DataFrame(columns=config.columnas_ganancias.keys())
        for ganancia_var in config.columnas_ganancias:
                var_type = config.columnas_ganancias[ganancia_var]
                if var_type:
                    self.ganancias[ganancia_var] = self.ganancias[ganancia_var].astype(var_type)

        self.ganancias_acum = self.acumular_ganancias()

        self.multas = pd.DataFrame(qc.get_multas_sesiones(self.sesiones))
        if self.multas.shape[0] == 0:
            self.multas = pd.DataFrame(columns=config.columnas_multa_final)
        else:
            for new_col in config.columnas_multa_extra:
                self.multas[new_col] = 0

        self.prestamos = pd.DataFrame(qc.get_prestamos_sesiones(self.sesiones))
        if self.prestamos.shape[0] == 0:
            self.prestamos = pd.DataFrame(columns=config.columnas_prestamos_final)
        else:
            for new_col in config.columnas_extras_prestamo:
                self.prestamos[new_col] = 0

        self.interes_prestamo = pd.DataFrame(qc.get_interes_prestamo_sesiones(self.sesiones))
        if self.interes_prestamo.shape[0] == 0:
            self.interes_prestamo = pd.DataFrame(columns=config.columnas_interes_prestamo.keys())
        for interes_prestamo_var in config.columnas_interes_prestamo:
                var_type = config.columnas_interes_prestamo[interes_prestamo_var]
                if var_type:
                    self.interes_prestamo[interes_prestamo_var] = self.interes_prestamo[interes_prestamo_var].astype(var_type)

        self.limite_credito_socios = self.calcular_limite_credito()
        self.calcular_interes_futuro()

        max_sesion = max([x['Sesion_id'] for x in self.sesiones])
        self.caja = qc.get_caja_sesion(max_sesion)


    def acumular_ganancias(self):

        resumen_ganancias = pd.DataFrame([])
        if self.ganancias.shape[0] > 0:

            this_ganancias = self.ganancias.loc[self.ganancias['Entregada'] == 0, :]

            resumen_ganancias = this_ganancias.groupby(['Socio_id']).agg({'Monto_ganancia': [('Monto_ganancia', 'sum')]})
            resumen_ganancias.columns = resumen_ganancias.columns.droplevel()

        return resumen_ganancias
    
    def calcular_limite_credito(self):

        #Calcula limite de credito para todos los socios dados los prestamos actuales

        prestamos_socios = self.socios_acciones.copy()
        if self.prestamos.shape[0] > 0:

            prestamos_socios = self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, :]

            prestamos_socios = prestamos_socios.groupby(['Socio_id']).agg({'Monto_prestamo': [('monto_total', 'sum'), ('num_prestamos', 'count')]})
            prestamos_socios.columns = prestamos_socios.columns.droplevel()
            prestamos_socios = prestamos_socios.reset_index()

            prestamos_socios = self.socios_acciones.merge(prestamos_socios, on='Socio_id', how='left')
            prestamos_socios = prestamos_socios.fillna(0)

        else:
            prestamos_socios['monto_total'] = 0
            prestamos_socios['num_prestamos'] = 0

        prestamos_socios['Limite_credito_final'] =\
            prestamos_socios.apply(lambda x: self.calcula_limite_credito_final(x['monto_total'], x['Acciones'], self.acuerdos['Limite_credito']), axis=1)
        prestamos_socios['Limite_credito_teorico'] =\
            prestamos_socios.apply(lambda x: self.calcula_limite_credito_teorico(x['Acciones'], self.acuerdos['Limite_credito']), axis=1)

        prestamos_socios['puede_pedir_prestamo'] =\
            prestamos_socios.apply(lambda x: self.puede_pedir_prestamo(x['num_prestamos'], self.acuerdos['Creditos_simultaneos']), axis=1)

        return prestamos_socios
    
    def calcular_interes_futuro(self):

        if self.prestamos.shape[0] > 0:

            self.prestamos['interes_futuro'] =\
                self.prestamos.apply(lambda x: self.calcula_interes_futuro(x), axis=1)
            
            self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 1, config.columnas_extras_prestamo] = 0


    def actualiza_acciones(self, df_acciones, costo_accion, tipo='COMPRA_ACCIONES'):

        if df_acciones.shape[0] > 0:

            df_acciones[tipo] =  df_acciones[tipo].apply(lambda x: x[0])
            df_acciones[tipo] = df_acciones[tipo]*costo_accion
            df_acciones[tipo] = df_acciones[tipo].astype(int)
            df_acciones = df_acciones.rename(columns={tipo: "Acciones"})

            if tipo == 'RETIRO_ACCIONES':
                df_acciones['Acciones'] = df_acciones['Acciones']*-1
        
            self.socios_acciones = self.socios_acciones.set_index('Socio_id')

            self.socios_acciones = self.socios_acciones.join(df_acciones, rsuffix='_new', how='left')
            self.socios_acciones = self.socios_acciones.fillna(0)

            self.socios_acciones['Acciones'] = self.socios_acciones['Acciones'] + self.socios_acciones['Acciones_new']
            self.socios_acciones = self.socios_acciones.drop(['Acciones_new'], axis=1)
            self.socios_acciones = self.socios_acciones.reset_index()

            self.caja = self.caja + df_acciones['Acciones'].sum()

    def actualiza_interes_prestamos(self, sesion_id):

        if self.prestamos.shape[0] > 0:

            self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, 'Interes_generado'] =\
            self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, 'Interes_generado'] + self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, 'interes_futuro']
            self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, 'Sesiones_restantes'] =\
                + self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, 'Sesiones_restantes'] - 1
            

            if self.interes_prestamo.shape[0] == 0:
                next_interes_prestamo_id = qc.get_next_autoincrement_table('interes_prestamo')
            else:
                next_interes_prestamo_id = self.interes_prestamo['Interes_prestamo_id'].max() + 10

            interes_prestamo_sesion = self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, ['Prestamo_id', 'Interes_generado', 'Estatus_ampliacion', 'Sesiones_restantes']].copy()
            interes_prestamo_sesion['Sesion_id'] = sesion_id
            interes_prestamo_sesion['Interes_prestamo_id'] = range(next_interes_prestamo_id, next_interes_prestamo_id + interes_prestamo_sesion.shape[0]*10,10)
            interes_prestamo_sesion['Tipo_interes'] = 0
            interes_prestamo_sesion.loc[(interes_prestamo_sesion['Sesiones_restantes'] < 0) & (interes_prestamo_sesion['Estatus_ampliacion'] == 0), 'Tipo_interes'] = 1
            interes_prestamo_sesion.loc[(interes_prestamo_sesion['Sesiones_restantes'] >= 0) & (interes_prestamo_sesion['Estatus_ampliacion'] == 1), 'Tipo_interes'] = 2
            interes_prestamo_sesion.loc[(interes_prestamo_sesion['Sesiones_restantes'] < 0) & (interes_prestamo_sesion['Estatus_ampliacion'] == 1), 'Tipo_interes'] = 3

            interes_prestamo_sesion = interes_prestamo_sesion.drop(['Estatus_ampliacion', 'Sesiones_restantes'], axis=1)
            interes_prestamo_sesion = interes_prestamo_sesion.rename(columns={'Interes_generado': "Monto_interes"})
            interes_prestamo_sesion = interes_prestamo_sesion[config.columnas_interes_prestamo.keys()]

            self.interes_prestamo = pd.concat([self.interes_prestamo, interes_prestamo_sesion])
            self.interes_prestamo = self.interes_prestamo.reset_index(drop=True)


    def actualiza_pago_prestamos(self, df_abono):

        if df_abono.shape[0] > 0:

            self.prestamos['Prestamo_id'] = self.prestamos['Prestamo_id'].astype(int)
            prestamos_activos = self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0]
            prestamo_id_df = prestamos_activos.groupby('Socio_id')['Prestamo_id'].apply(list).to_frame()

            df_abono.index.rename('Socio_id', inplace=True)
            df_abono = df_abono.join(prestamo_id_df, how='inner')
            df_abono = df_abono.explode(['ABONO', 'Prestamo_id'])
            df_abono['Prestamo_id'] = df_abono['Prestamo_id'].astype(int)
            df_abono = df_abono.set_index('Prestamo_id')

            # prestamos_activos['Prestamo_id'] = prestamos_activos['Prestamo_id'].astype(int)
            prestamos_activos = prestamos_activos.set_index('Prestamo_id')
            prestamos_activos = prestamos_activos.join(df_abono, how='left')

            #Revisa lo que sobró de abono del prestamo después del interés
            prestamos_activos['debe_interes'] = prestamos_activos['Interes_generado'] - prestamos_activos['Interes_pagado']
            prestamos_activos['sobrante_abono'] = prestamos_activos['ABONO'] - prestamos_activos['debe_interes']

            #Si tiene más abono, lo suma al monto pagado e "iguala" el Interes pagado al interés generado
            prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'Monto_pagado'] =\
                prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'Monto_pagado'] + prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'sobrante_abono']
            

            prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'Interes_pagado'] =\
                prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'Interes_generado']
            prestamos_activos.loc[prestamos_activos['sobrante_abono'] < 0, 'Interes_pagado'] =\
                prestamos_activos.loc[prestamos_activos['sobrante_abono'] < 0, 'Interes_pagado'] + prestamos_activos.loc[prestamos_activos['sobrante_abono'] < 0, 'ABONO']
            
            #Calcula el interes pagado en esta sesion
            prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'Ultimo_interes_pagado'] =\
                prestamos_activos.loc[prestamos_activos['sobrante_abono'] >= 0, 'debe_interes'] 
            
            prestamos_activos.loc[prestamos_activos['sobrante_abono'] < 0, 'Ultimo_interes_pagado'] =\
                prestamos_activos.loc[prestamos_activos['sobrante_abono'] < 0, 'ABONO'] 
                        
            prestamos_activos.loc[prestamos_activos['Monto_pagado'] >= prestamos_activos['Monto_prestamo'], 'Estatus_prestamo'] = 1
            idx_pagado_mas = prestamos_activos['Monto_pagado'] > (prestamos_activos['Monto_prestamo'] + 0.05)

            if idx_pagado_mas.any():
                logging.debug('Se pago de más')
                logging.debug('%s', prestamos_activos)
                raise Exception('El monto pagado es mayor al monto del prestamo')

            # Elimina las columnas sobrantes de presatmo
            prestamos_activos['Ultimo_abono'] = prestamos_activos['ABONO']
            prestamos_activos = prestamos_activos.drop(['ABONO'], axis=1)

            self.prestamos = self.prestamos.set_index('Prestamo_id')
            self.prestamos.loc[prestamos_activos.index] = prestamos_activos
            self.prestamos = self.prestamos.reset_index()

            self.caja = self.caja + df_abono['ABONO'].sum()

    def actualiza_pago_multas(self, df_pago_multa):

        if self.multas.shape[0] > 0:
            self.multas['Pago_en_sesion'] = 0

        if df_pago_multa.shape[0] > 0:

            self.multas['Multa_id'] = self.multas['Multa_id'].astype(int)
            multas_activas = self.multas.loc[self.multas['Status'] == 0]
            multa_id_df = multas_activas.groupby('Socio_id')['Multa_id'].apply(list).to_frame()

            df_pago_multa.index.rename('Socio_id', inplace=True)
            df_pago_multa = df_pago_multa.join(multa_id_df, how='inner')
            df_pago_multa = df_pago_multa.explode(['PAGO_MULTA', 'Multa_id'])
            df_pago_multa['Multa_id'] = df_pago_multa['Multa_id'].astype(int)
            df_pago_multa = df_pago_multa.set_index('Multa_id')

            
            multas_activas = multas_activas.set_index('Multa_id')
            multas_activas = multas_activas.join(df_pago_multa, how='left')

            multas_activas.loc[multas_activas['PAGO_MULTA'] == 1, 'Pago_en_sesion'] = 1
            multas_activas['PAGO_MULTA'] = multas_activas['PAGO_MULTA'].fillna(0)
            multas_activas.loc[multas_activas['Pago_en_sesion'] == 1, 'Status'] = 1            

            # Elimina las columnas sobrantes de multas
            multas_activas = multas_activas.drop(['PAGO_MULTA'], axis=1)

            self.multas = self.multas.set_index('Multa_id')
            self.multas.loc[multas_activas.index] = multas_activas
            self.multas = self.multas.reset_index()

            self.caja = self.caja + multas_activas.loc[multas_activas['Pago_en_sesion'] == 1, 'Monto_multa'].sum()

    def actualiza_prestamos(self, df_prestamos, sesion_list):

        if df_prestamos.shape[0] > 0:

            df_prestamos.index.name = 'Socio_id'

            if self.prestamos.shape[0] > 0:
                prestamo_id_df = self.prestamos.groupby('Socio_id')['Prestamo_id'].apply(list).to_frame()
                df_prestamos = df_prestamos.join(prestamo_id_df, how='left')
 
                df_prestamos1 = df_prestamos.loc[df_prestamos['Prestamo_id'].isnull()]
                if df_prestamos1.shape[0] > 0:
                    df_prestamos1 = df_prestamos1.explode(['PRÉSTAMO', 'AMPLIACIÓN', 'NUM_SESIONES'])
                df_prestamos2 = df_prestamos.loc[~df_prestamos['Prestamo_id'].isnull()]
                if df_prestamos2.shape[0] > 0:
                    df_prestamos2 = df_prestamos2.explode(['PRÉSTAMO', 'AMPLIACIÓN', 'NUM_SESIONES', 'Prestamo_id'])
                df_prestamos = pd.concat([df_prestamos1, df_prestamos2])
            else:
                df_prestamos = df_prestamos.explode(['PRÉSTAMO', 'AMPLIACIÓN', 'NUM_SESIONES'])
                df_prestamos['Prestamo_id'] = -1

            df_prestamos = df_prestamos.reset_index()
            df_prestamos = df_prestamos.rename(columns={'index': "Socio_id"})
            df_prestamos = pd.merge(df_prestamos, self.limite_credito_socios, on='Socio_id', how='inner')

            for prestamo_idx in range(df_prestamos.shape[0]):

                socio          = df_prestamos.loc[prestamo_idx, 'Socio_id']
                ampliacion     = df_prestamos.loc[prestamo_idx, 'AMPLIACIÓN']
                puede_pedir    = df_prestamos.loc[prestamo_idx, 'puede_pedir_prestamo']
                limite_credito = df_prestamos.loc[prestamo_idx, 'Limite_credito_final']
                monto_pedir    = df_prestamos.loc[prestamo_idx, 'PRÉSTAMO']
                num_sesiones   = df_prestamos.loc[prestamo_idx, 'NUM_SESIONES']
                prestamo_id_original    = df_prestamos.loc[prestamo_idx, 'Prestamo_id']

                if not ampliacion and not puede_pedir:
                    logging.debug('El socio no puede pedir más prestamos %s', df_prestamos.loc[prestamo_idx, :])
                    continue
                if limite_credito < monto_pedir:
                    logging.debug('El socio no tiene suficiente līmite de crédito para el préstamo %s', df_prestamos.loc[prestamo_idx, :])
                    continue
                if not ampliacion:
                     series_prestamo = self.crea_nuevo_prestamo(monto_pedir, ampliacion, num_sesiones, socio, sesion_list[-1]['Sesion_id'])
                     self.prestamos.loc[self.prestamos.shape[0]] = series_prestamo
                     self.caja = self.caja - monto_pedir
                     continue
                if ampliacion:

                    prestamo_ampliar = self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, :].squeeze()

                    debe_interes = prestamo_ampliar['Interes_generado'] - prestamo_ampliar['Interes_pagado']
                    debe_monto = prestamo_ampliar['Monto_prestamo'] - prestamo_ampliar['Monto_pagado']
                    debe_total = debe_interes + debe_monto

                    if monto_pedir < debe_total:
                        logging.debug('El monto de ampliacion no alcanza a cubrir la deuda del prestamo')
                        continue

                    # "Paga" el prestamo
                    self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Interes_pagado'] =\
                          self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Interes_generado']
                    self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Ultimo_interes_pagado'] = debe_interes
                    self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Monto_pagado'] =\
                          self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Monto_prestamo']
                    self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Monto_pagado'] =\
                          self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Monto_prestamo']
                    self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Estatus_prestamo'] = 1
                    #self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Sesiones_restantes'] =\
                    #    self.prestamos.loc[self.prestamos['Prestamo_id'] == prestamo_id_original, 'Sesiones_restantes'] -1

                    series_prestamo = self.crea_nuevo_prestamo(monto_pedir, ampliacion, num_sesiones, socio, sesion_list[-1]['Sesion_id'], prestamo_original=prestamo_id_original)
                    self.prestamos.loc[self.prestamos.shape[0]] = series_prestamo
                    self.caja = self.caja - (monto_pedir - debe_total)
    
    def crea_nuevo_prestamo(self, monto, ampliacion, num_sesiones, socio, sesion, prestamo_original=None):

        if self.prestamos.shape[0] > 0:
            ultimo_prestamo = self.prestamos['Prestamo_id'].max() + 10
        else:
            ultimo_prestamo = qc.get_next_autoincrement_table('prestamos')

        dict_prestamo = {
            'Prestamo_id': ultimo_prestamo,
            'Acuerdos_id': self.acuerdos['Acuerdo_id'],
            'Monto_prestamo': monto,
            'Monto_pagado': 0,
            'Interes_generado': 0,
            'Interes_pagado': 0,
            'Estatus_ampliacion': bool(ampliacion),
            'Num_sesiones': num_sesiones,
            'Sesiones_restantes': num_sesiones,
            'Estatus_prestamo': 0,
            'Socio_id': socio,
            'Sesion_id': sesion,
            'Prestamo_original_id': prestamo_original,
            'interes_futuro': 0,
            'Ultimo_interes_pagado': 0,
            'debe_interes': 0,
            'sobrante_abono': 0,
            'Ultimo_abono': 0
        }

        dict_acuerdos = self.acuerdos[config.columnas_acuerdos].to_dict()

        dict_prestamo.update(dict_acuerdos)

        series_prestamo = pd.Series(dict_prestamo)
        #series_prestamo['interes_futuro'] = self.calcula_interes_futuro(series_prestamo)

        return series_prestamo
    
    def actualiza_multas(self, df_multas, sesion_list):

        if df_multas.shape[0] > 0:

            df_multas.index.name = 'Socio_id'

            df_multas = df_multas.explode(['MULTAS'])

            df_multas = df_multas.reset_index()
            df_multas = df_multas.rename(columns={'index': "Socio_id"})

            last_socio = 0
            idx_multa_socio = 0
            for multa_idx in range(df_multas.shape[0]):

                socio          = df_multas.loc[multa_idx, 'Socio_id']
                monto_multa    = df_multas.loc[multa_idx, 'MULTAS']

                if last_socio != socio:
                    idx_multa_socio = 0

                last_socio = socio
                
                series_multa = self.crea_nueva_multa(monto_multa, socio, sesion_list[-1]['Sesion_id'], idx_multa_socio, self.id_grupo)
                self.multas.loc[self.multas.shape[0]] = series_multa

                idx_multa_socio += 1
                
    
    def crea_nueva_multa(self, monto_multa, socio, sesion, idx_multa_socio):

        if self.multas.shape[0] > 0:
            ultima_multa = self.multas['Multa_id'].max() + 10
        else:
            ultima_multa = qc.get_next_autoincrement_table('multas')

        dict_multa = {
            'Multa_id': ultima_multa,
            'Monto_multa': monto_multa,
            'Descripcion': "Multa # " + str(idx_multa_socio+1) + " socio "  + str(socio) + " grupo: " +  str(self.id_grupo),
            'Status': 0,
            'Sesion_id': sesion,
            'Socio_id': socio,
            'Pago_en_sesion': 0
        }

        series_multa = pd.Series(dict_multa)

        return series_multa
    
    def calcula_ganancias(self, sessions):

        # Las ganancias de la 1a sesion son las de los acuerdos, 
        # De la 2da sesion hay que sacar el id de la BD,
        # A partir de la 3a ya la sacamos del id 
        if len(sessions) > 2:
            last_id_ganancia = self.ganancias['Ganancias_id'].max() + 10
        else:
            last_id_ganancia = qc.get_next_autoincrement_table('ganancias')
        
        ganancias_prestamos = 0
        if self.prestamos.shape[0] > 0:
            ganancias_prestamos = self.prestamos['Ultimo_interes_pagado'].sum()

        ganancias_multas = 0
        if self.multas.shape[0] > 0:
            ganancias_multas = self.multas.loc[self.multas['Pago_en_sesion'] ==1, 'Monto_multa'].sum()

        ganancias_totales_sesion = ganancias_prestamos + ganancias_multas

        ganancias_sesion = self.socios_acciones[['Acciones', 'Socio_id']].copy()
        total_acciones = ganancias_sesion['Acciones'].sum()
        ganancias_sesion['Acciones'] = ganancias_sesion['Acciones']*ganancias_totales_sesion/total_acciones
        ganancias_sesion = ganancias_sesion.rename(columns={'Acciones': 'Monto_ganancia'})
        ganancias_sesion['Entregada'] = 0
        ganancias_sesion['Sesion_id'] = sessions[-1]['Sesion_id']
        ganancias_sesion['Ganancias_id'] = range(last_id_ganancia, last_id_ganancia + ganancias_sesion.shape[0]*10,10)

        self.ganancias = pd.concat([self.ganancias, ganancias_sesion])

        self.ganancias = self.ganancias.reset_index(drop=True)

    def actualiza_sesiones(self):

        if len(self.sesiones) <= 1:
            next_session  = qc.get_next_autoincrement_table('sesiones')
        else:
            next_session = self.sesiones[-1]['Sesion_id'] + 10

        dicto = {'Sesion_id': next_session}

        self.sesiones.append(dicto)

    def crea_nuevos_acuerdos(self, df_acuerdos, xls_name):

        if df_acuerdos.shape[0] > 0:
            new_dict_acuerdos = ms.get_dict_acuerdos_xls(xls_name, idx_acuerdos=1)
            acuerdos_series = pd.Series(new_dict_acuerdos)

            acuerdos_series['Acuerdo_id'] = qc.get_next_autoincrement_table('acuerdos')
            acuerdos_series['Grupo_id'] = self.acuerdos['Grupo_id']
            acuerdos_series['Fecha_acuerdos'] = date.today()
            acuerdos_series['Fecha_acuerdos_fin'] = datetime.strptime(acuerdos_series['Fecha_acuerdos_fin'], '%Y-%m-%d').date()
            acuerdos_series['Status'] = 1
            acuerdos_series['Id_socio_administrador'] = self.acuerdos['Id_socio_administrador']
            acuerdos_series['Id_socio_administrador_suplente'] = self.acuerdos['Id_socio_administrador_suplente']

            acuerdos_series = acuerdos_series.to_frame().T
            for acuerdo_var in config.acuerdos_var_type:
                var_type = config.acuerdos_var_type[acuerdo_var]
                if var_type:
                    acuerdos_series[acuerdo_var] = acuerdos_series[acuerdo_var].astype(var_type)

            acuerdos_series = acuerdos_series.squeeze(axis = 0)
            acuerdos_series = acuerdos_series[self.acuerdos.keys()]

            self.acuerdos = acuerdos_series

    def compara_db_monitor(self):

        sesiones = qc.get_sesiones_grupo(self.id_grupo)
        [status, dfs] = self.compara_dfs(pd.DataFrame(sesiones), pd.DataFrame(self.sesiones), 'sesiones')
        if not status:
            return [status, dfs]

        acuerdos = pd.Series(qc.get_acuerdos_grupo(self.id_grupo))
        [status, dfs] = self.compara_dfs(acuerdos, self.acuerdos, 'acuerdos')
        if not status:
            return [status, dfs]
        
        socios_acciones = pd.DataFrame(qc.get_socios_acciones_grupo(self.id_grupo))
        [status, dfs] = self.compara_dfs(socios_acciones, self.socios_acciones, 'socios-acciones')
        if not status:
            return [status, dfs]
        
        prestamos = pd.DataFrame(qc.get_prestamos_sesiones(sesiones))
        if prestamos.shape[0] == 0:
            prestamos = pd.DataFrame(columns=config.columnas_prestamos)
        [status, dfs] = self.compara_dfs(prestamos[config.columnas_prestamos], self.prestamos[config.columnas_prestamos], 'prestamos')
        if not status:
            return [status, dfs]

        multas = pd.DataFrame(qc.get_multas_sesiones(sesiones))
        if multas.shape[0] == 0:
            multas = pd.DataFrame(columns=config.columnas_multa)
        [status, dfs] = self.compara_dfs(multas[config.columnas_multa], self.multas[config.columnas_multa], 'multas')
        if not status:
            return [status, dfs]

        ganancias = pd.DataFrame(qc.get_ganancias_sesiones(sesiones))
        [status, dfs] = self.compara_dfs(ganancias, self.ganancias, 'ganancias')
        if not status:
            return [status, dfs]

        max_sesion = max([x['Sesion_id'] for x in sesiones])
        caja = qc.get_caja_sesion(max_sesion)
        if not self.caja == caja:
            print('caja no igual')
            return [False, [self.caja, caja]]
        
        return [True, []]

    @staticmethod
    def compara_dfs(df1, df2, tipo):

        status = True

        if tipo in ['ganancias']:
            if df1.shape[0] == 0 and df2.shape[0] == 0:
                comparacion = True
            else:
                comp = np.isclose(df1,df2)
                comparacion = np.all(comp)
        else:
            comp = df1.compare(df2)
            if comp.shape[0] == 0:
                comparacion = True
            else:
                comparacion = False

        if not comparacion:
            print(tipo+ ' No coinciden monitor y db')
            status = False
        else:
            pass
            # print(tipo + ' coinciden !!')

        return [status, [df1,df2]]


        
    def actualiza_todo_sesion(self, xls_name, session_num, type_xls='MAYRA'):

        dict_users = qc.get_socios_grupo(self.id_grupo)
        dict_session = ms.read_transform_info_xls(xls_name, session_num, dict_users, type_xls=type_xls)

        # self.acuerdos = pd.Series(qc.get_acuerdos_grupo(id_grupo))
        self.actualiza_sesiones()
        self.crea_nuevos_acuerdos(dict_session['NUEVOS_ACUERDOS'], xls_name)

        self.actualiza_interes_prestamos(self.sesiones[-1]['Sesion_id'])

        self.actualiza_multas(dict_session['MULTAS'], self.sesiones)

        self.actualiza_acciones(dict_session['COMPRA_ACCIONES'], self.acuerdos['Costo_acciones'], tipo='COMPRA_ACCIONES')
        self.limite_credito_socios = self.calcular_limite_credito()

        self.actualiza_pago_prestamos(dict_session['ABONO'])
        
        self.actualiza_prestamos(dict_session['PRÉSTAMO'], self.sesiones)
        
        self.actualiza_pago_multas(dict_session['PAGO_MULTA'])

        self.actualiza_acciones(dict_session['RETIRO_ACCIONES'], self.acuerdos['Costo_acciones'], tipo='RETIRO_ACCIONES')

        self.calcula_ganancias(self.sesiones)

        self.interes_futuro_prestamo = self.calcular_interes_futuro()