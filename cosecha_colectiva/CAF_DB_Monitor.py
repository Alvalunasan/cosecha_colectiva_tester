

import pandas as pd
import logging
import numpy as np
import copy
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
 
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

        interes_futuro = round(monto_calculo*interes_final*2/100,0)/2

        return interes_futuro



    def __init__(self, id_grupo, xls_name) -> None:


        self.id_grupo = id_grupo
        self.acuerdos = pd.Series(qc.get_acuerdos_grupo(self.id_grupo))
        self.socios_acciones = pd.DataFrame(qc.get_socios_acciones_grupo(self.id_grupo))

        self.reorder_socios_acciones_by_xls(xls_name)

        self.sesiones = qc.get_sesiones_grupo(self.id_grupo)
        self.sesiones_bd = pd.DataFrame(columns=config.bd_columnas_sesiones)
        self.transacciones_bd = pd.DataFrame(columns=config.bd_columnas_transacciones)
        self.transacciones_prestamo_bd = pd.DataFrame(columns=config.bd_columnas_transacciones_prestamos)

        self.ganancias = pd.DataFrame(qc.get_ganancias_sesiones(self.sesiones))
        if self.ganancias.shape[0] == 0:
            self.ganancias = pd.DataFrame(columns=config.columnas_ganancias.keys())
        for ganancia_var in config.columnas_ganancias:
                var_type = config.columnas_ganancias[ganancia_var]
                if var_type:
                    self.ganancias[ganancia_var] = self.ganancias[ganancia_var].astype(var_type)

        self.ganancias_acum = self.acumular_ganancias()
        self.ganancias_sesion = None

        self.multas = pd.DataFrame(qc.get_multas_sesiones(self.sesiones))
        if self.multas.shape[0] == 0:
            self.multas = pd.DataFrame(columns=config.columnas_multa_final)
        else:
            for new_col in config.columnas_multa_extra:
                self.multas[new_col] = 0

        self.prestamos = qc.get_prestamos_sesiones(self.sesiones, self.id_grupo)
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

        if len(self.sesiones) > 0:
            max_sesion = max([x['Sesion_id'] for x in self.sesiones])
        else:
            max_sesion = 1
        
        self.caja = qc.get_caja_sesion(max_sesion)


    def reorder_socios_acciones_by_xls(self, xls_name):

        self.socios_xls = pd.DataFrame(ms.get_dict_usuarios_xls(xls_name))

        if self.socios_xls.shape[0] != self.socios_acciones.shape[0]:
            raise('Numero de socios en Excel no coincide con los usuarios de la base de datos')

        mini_socios = self.socios_xls['CURP'].to_frame().copy()
        mini_socios = mini_socios.reset_index()
        mini_socios = mini_socios.rename(columns={'index': 'xls_order'})
        mini_socios
        aux_df = pd.merge(left=self.socios_acciones, right=mini_socios, on='CURP', how='inner')

        if aux_df.shape[0] < self.socios_acciones.shape[0]:
            raise('CURPS en Excel no coincide con los CURPS de losusuarios de la base de datos')
        
        self.socios_acciones = aux_df.sort_values(by='xls_order')
        self.socios_acciones = self.socios_acciones.reset_index(drop=True)


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
            #df_acciones[tipo] = df_acciones[tipo]*costo_accion
            df_acciones[tipo] = df_acciones[tipo].astype(int)
            df_acciones = df_acciones.rename(columns={tipo: "Acciones"})

            #bd_columnas_transacciones = ['Transaccion_id', 'Cantidad_movimiento', 'Caja', 'Timestamp', 'Sesion_id', 'Socio_id', 'Acuerdo_id', 'Catalogo_id']

            if tipo == 'RETIRO_ACCIONES':
                df_acciones['Acciones'] = df_acciones['Acciones']*-1

                    
            self.socios_acciones = self.socios_acciones.set_index('Socio_id')

            self.socios_acciones = self.socios_acciones.join(df_acciones, rsuffix='_new', how='left')
            self.socios_acciones = self.socios_acciones.fillna(0)

            self.socios_acciones['Acciones'] = self.socios_acciones['Acciones'] + self.socios_acciones['Acciones_new']
            self.socios_acciones = self.socios_acciones.drop(['Acciones_new'], axis=1)
            self.socios_acciones = self.socios_acciones.reset_index()
            self.socios_acciones = self.socios_acciones[config.columnas_socio_accion_plus]
            self.socios_acciones['Acciones'] = self.socios_acciones['Acciones'].astype(int)

            self.caja = self.caja + df_acciones['Acciones'].sum()

    def actualiza_interes_prestamos(self, sesion_id):

        if self.prestamos.shape[0] > 0:

            self.prestamos.loc[(self.prestamos['Estatus_prestamo'] == 0) & (self.prestamos['Status_socio'] == 1), 'Interes_generado'] =\
            self.prestamos.loc[(self.prestamos['Estatus_prestamo'] == 0) & (self.prestamos['Status_socio'] == 1), 'Interes_generado'] + self.prestamos.loc[self.prestamos['Estatus_prestamo'] == 0, 'interes_futuro']
            self.prestamos.loc[(self.prestamos['Estatus_prestamo'] == 0) & (self.prestamos['Status_socio'] == 1), 'Sesiones_restantes'] =\
                + self.prestamos.loc[(self.prestamos['Estatus_prestamo'] == 0) & (self.prestamos['Status_socio'] == 1), 'Sesiones_restantes'] - 1
            

            # Al principo, nadie ha pagado nada hasta que haya abono
            self.prestamos['Ultimo_interes_pagado'] = 0
            
            next_interes_prestamo_id = -1000
            #if self.interes_prestamo.shape[0] == 0:
            #    next_interes_prestamo_id = qc.get_next_autoincrement_table('interes_prestamo')
            #else:
            #    next_interes_prestamo_id = self.interes_prestamo['Interes_prestamo_id'].max() + 1

            interes_prestamo_sesion = self.prestamos.loc[(self.prestamos['Estatus_prestamo'] == 0) & (self.prestamos['Status_socio'] == 1), ['Prestamo_id', 'interes_futuro', 'Estatus_ampliacion', 'Sesiones_restantes']].copy()
            interes_prestamo_sesion = interes_prestamo_sesion.sort_values(by=['Prestamo_id'], ascending=[False])
            interes_prestamo_sesion['Sesion_id'] = sesion_id
            interes_prestamo_sesion['Interes_prestamo_id'] = range(next_interes_prestamo_id, next_interes_prestamo_id + interes_prestamo_sesion.shape[0]*1,1)
            interes_prestamo_sesion['Tipo_interes'] = 0
            interes_prestamo_sesion.loc[(interes_prestamo_sesion['Sesiones_restantes'] < 0) & (interes_prestamo_sesion['Estatus_ampliacion'] == 0), 'Tipo_interes'] = 1
            interes_prestamo_sesion.loc[(interes_prestamo_sesion['Sesiones_restantes'] >= 0) & (interes_prestamo_sesion['Estatus_ampliacion'] == 1), 'Tipo_interes'] = 2
            interes_prestamo_sesion.loc[(interes_prestamo_sesion['Sesiones_restantes'] < 0) & (interes_prestamo_sesion['Estatus_ampliacion'] == 1), 'Tipo_interes'] = 3

            interes_prestamo_sesion = interes_prestamo_sesion.drop(['Estatus_ampliacion', 'Sesiones_restantes'], axis=1)
            interes_prestamo_sesion = interes_prestamo_sesion.rename(columns={'interes_futuro': "Monto_interes"})
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
                df_prestamos['Prestamo_id'] = -100

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

        ultimo_prestamo = -1000
        #if self.prestamos.shape[0] > 0:
        #    ultimo_prestamo = self.prestamos['Prestamo_id'].max() + 1
        #else:
        #    ultimo_prestamo = qc.get_next_autoincrement_table('prestamos')

        status_socio  = self.socios_acciones.loc[self.socios_acciones['Socio_id'] == socio, 'Status'].values[0]

        dict_prestamo = {
            'Prestamo_id': ultimo_prestamo,
            'Acuerdos_id': self.acuerdos['Acuerdo_id'],
            'Monto_prestamo': monto,
            'Monto_pagado': 0,
            'Interes_generado': 0,
            'Interes_pagado': 0,
            'Fecha_inicial': datetime.strftime(date.today(), format='%Y-%m-%d'),
            'Fecha_final':  datetime.strftime(date.today()+ relativedelta(months=num_sesiones), format='%Y-%m-%d'),
            'Observaciones': 'Préstamo generado en script automático',
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
            'Ultimo_abono': 0,
            'Status_socio': status_socio
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
                
                series_multa = self.crea_nueva_multa(monto_multa, socio, sesion_list[-1]['Sesion_id'], idx_multa_socio)
                self.multas.loc[self.multas.shape[0]] = series_multa

                idx_multa_socio += 1
                
    
    def crea_nueva_multa(self, monto_multa, socio, sesion, idx_multa_socio):

        ultima_multa = -1000
        #if self.multas.shape[0] > 0:
        #    ultima_multa = self.multas['Multa_id'].max() + 1
        #else:
        #    ultima_multa = qc.get_next_autoincrement_table('multas')

        dict_multa = {
            'Multa_id': ultima_multa,
            'Monto_multa': monto_multa,
            'Descripcion': "Multa # " + str(idx_multa_socio+1) + " socio "  + str(socio) + " grupo: " +  str(self.id_grupo) + "con script",
            'Status': 0,
            'Sesion_id': sesion,
            'Socio_id': socio,
            'Pago_en_sesion': 0,
            'Transaccion_id': None,
            'created_at': datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S'),
        }

        series_multa = pd.Series(dict_multa)

        return series_multa
    
    def calcula_ganancias(self, sessions):

        # Las ganancias de la 1a sesion son las de los acuerdos, 
        # De la 2da sesion hay que sacar el id de la BD,
        # A partir de la 3a ya la sacamos del id 
        last_id_ganancia = -1000
        #if len(sessions) > 2:
        #    last_id_ganancia = self.ganancias['Ganancias_id'].max() + 1
        #else:
        #    last_id_ganancia = qc.get_next_autoincrement_table('ganancias')
        
        ganancias_prestamos = 0
        if self.prestamos.shape[0] > 0:
            ganancias_prestamos = self.prestamos['Ultimo_interes_pagado'].sum()

        ganancias_multas = 0
        if self.multas.shape[0] > 0:
            ganancias_multas = self.multas.loc[self.multas['Pago_en_sesion'] ==1, 'Monto_multa'].sum()

        ganancias_totales_sesion = ganancias_prestamos + ganancias_multas

        ganancias_sesion = self.socios_acciones.loc[self.socios_acciones['Status'] == 1, ['Acciones', 'Socio_id']].copy()
        ganancias_sesion = ganancias_sesion.sort_values(by='Acciones')

        total_acciones = ganancias_sesion['Acciones'].sum()

        ganancias_sesion['Monto_ganancia_floor'] = np.floor(ganancias_sesion['Acciones']*ganancias_totales_sesion*2/total_acciones)/2
        ganancias_sesion['sobrante_Monto_ganancia'] = 0

        ganancias_totales_floor = ganancias_sesion['Monto_ganancia_floor'].sum()
        sob_acciones = (ganancias_totales_sesion - ganancias_totales_floor)*2 

        if sob_acciones > 0:
            ganancias_sesion.loc[0:sob_acciones-1,'sobrante_Monto_ganancia'] = 0.5

        ganancias_sesion['Monto_ganancia'] = ganancias_sesion['Monto_ganancia_floor'] + ganancias_sesion['sobrante_Monto_ganancia']
        ganancias_sesion['Entregada'] = 0
        ganancias_sesion['Sesion_id'] = sessions[-1]['Sesion_id']
        ganancias_sesion['Ganancias_id'] = range(last_id_ganancia, last_id_ganancia + ganancias_sesion.shape[0]*1,1)
        ganancias_sesion['Ganancia_accion'] = ganancias_totales_sesion/total_acciones
        ganancias_sesion['periodo'] = 'nuevo'

        self.ganancias_sesion = copy.deepcopy(ganancias_sesion)
        ganancias_sesion = ganancias_sesion.drop(['Monto_ganancia_floor', 'sobrante_Monto_ganancia', 'Acciones'], axis=1)

        self.ganancias = pd.concat([self.ganancias, ganancias_sesion])

        self.ganancias = self.ganancias.reset_index(drop=True)

    def actualiza_sesiones(self, bd_update=False):

        if bd_update:
            base_sesion = {
                'Fecha': datetime.strftime(date.today(), format='%Y-%m-%d'),
                'Activa': 1,
                'Fecha_prox_reunion': datetime.strftime(date.today()+ relativedelta(months=1), format='%Y-%m-%d'),
                'Lugar_prox_reunion': 'Sesion creada en script',
                'Tipo_sesion': 1,
                'Grupo_id': self.id_grupo,
                'created_at': datetime.strftime(date.today(), '%Y-%m-%d')
            }
            if self.sesiones_bd.shape[0] == 0:
                base_sesion['Caja'] = 0
                base_sesion['Acciones'] = 0
                base_sesion['Ganancias'] = 0
            else:
                base_sesion['Caja'] = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Caja']
                base_sesion['Acciones'] = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Acciones']
                base_sesion['Ganancias'] = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Ganancias']
            base_sesion['Sesion_id'] = qc.insert_sesion(base_sesion)

            self.sesiones_bd = pd.concat([self.sesiones_bd, pd.Series(base_sesion).to_frame().T], axis=0, ignore_index=True)
            next_session = base_sesion['Sesion_id']

        else:
            if len(self.sesiones) <= 1:
                next_session  = qc.get_next_autoincrement_table('sesiones')
            else:
                next_session = self.sesiones[-1]['Sesion_id'] + 1

        dicto = {'Sesion_id': next_session}

        self.sesiones.append(dicto)

    def crea_nuevos_acuerdos(self, df_acuerdos, xls_name):

        if df_acuerdos.shape[0] > 0:
            new_dict_acuerdos = ms.get_dict_acuerdos_xls(xls_name, idx_acuerdos=1)
            acuerdos_series = pd.Series(new_dict_acuerdos)

            #acuerdos_series['Acuerdo_id'] = qc.get_next_autoincrement_table('acuerdos')
            acuerdos_series['Acuerdo_id'] = -100
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

    def cambia_status_socio(self, df_status_socio):

        if df_status_socio.shape[0] > 0:

            for socio in df_status_socio.index.to_list():

                new_status = int(df_status_socio.loc[socio, 'STATUS_SOCIOS'][0])

                if new_status == -1:
                    new_status = 0
                
                self.socios_acciones.loc[self.socios_acciones['Socio_id'] == socio, 'Status'] = new_status
                self.prestamos.loc[self.prestamos['Socio_id'] == socio, 'Status_socio'] = new_status

    def compara_db_monitor(self):

        sesiones = qc.get_sesiones_grupo(self.id_grupo)
        [status, dfs] = self.compara_dfs(pd.DataFrame(sesiones), pd.DataFrame(self.sesiones), 'sesiones', drop_columns=['Sesion_id'])
        if not status:
            return [status, dfs]

        acuerdos = pd.Series(qc.get_acuerdos_grupo(self.id_grupo))
        [status, dfs] = self.compara_dfs(acuerdos, self.acuerdos, 'acuerdos')
        if not status:
            return [status, dfs]
        
        socios_acciones = pd.DataFrame(qc.get_socios_acciones_grupo(self.id_grupo))
        [status, dfs] = self.compara_dfs(socios_acciones[config.columnas_socio_accion], self.socios_acciones[config.columnas_socio_accion], 'socios-acciones')
        if not status:
            return [status, dfs]
        
        prestamos = qc.get_prestamos_sesiones(sesiones, self.id_grupo)
        if prestamos.shape[0] == 0:
            prestamos = pd.DataFrame(columns=config.columnas_prestamos)
        [status, dfs] = self.compara_dfs(prestamos[config.columnas_prestamos], self.prestamos[config.columnas_prestamos], 'prestamos',  drop_columns=['Prestamo_id','Sesion_id', 'Socio_id'])
        if not status:
            return [status, dfs]

        multas = pd.DataFrame(qc.get_multas_sesiones(sesiones))
        if multas.shape[0] == 0:
            multas = pd.DataFrame(columns=config.columnas_multa)
        [status, dfs] = self.compara_dfs(multas[config.columnas_multa], self.multas[config.columnas_multa], 'multas', drop_columns=['Socio_id', 'Sesion_id'])
        if not status:
            return [status, dfs]

        ganancias = pd.DataFrame(qc.get_ganancias_sesiones(sesiones))
        [status, dfs] = self.compara_dfs(ganancias, self.ganancias, 'ganancias', drop_columns=['Socio_id', 'Sesion_id', 'Ganancias_id'])
        if not status:
            return [status, dfs]
        
        interes_prestamo = pd.DataFrame(qc.get_interes_prestamo(sesiones))
        if interes_prestamo.shape[0] == 0:
            interes_prestamo = pd.DataFrame(columns=config.columnas_interes_prestamo)
        interes_prestamo = interes_prestamo.sort_values(by=['Prestamo_id', 'Sesion_id'])
        interes_prestamo = interes_prestamo.reset_index(drop=True)
        interes_prestamo_comp = self.interes_prestamo.sort_values(by=['Prestamo_id', 'Sesion_id'])
        interes_prestamo_comp = interes_prestamo_comp.reset_index(drop=True)
        [status, dfs] = self.compara_dfs(interes_prestamo[config.columnas_interes_prestamo_comp.keys()], 
                                         interes_prestamo_comp[config.columnas_interes_prestamo_comp.keys()], 'interes_prestamos', drop_columns=['Prestamo_id', 'Sesion_id'])
        
        if not status:
            return [status, dfs]

        max_sesion = max([x['Sesion_id'] for x in sesiones])
        caja = qc.get_caja_sesion(max_sesion)
        if not self.caja == caja:
            print('caja no igual')
            return [False, [self.caja, caja]]
        
        return [True, []]

    @staticmethod
    def compara_dfs(df1, df2, tipo, drop_columns=[]):

        status = True

        if len(drop_columns) > 0:
            df1 = df1.drop(drop_columns, axis=1)
            df2 = df2.drop(drop_columns, axis=1)

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
    
    def get_current_caja(self):

        if self.transacciones_bd.shape[0] == 0:
            current_caja = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Caja']
        else:
            current_caja = self.transacciones_bd.loc[self.transacciones_bd.shape[0]-1, 'Caja']

        return current_caja

    def append_transacciones_bd(self, df_transacciones, tipo='COMPRA_ACCIONES'):

        if df_transacciones.shape[0] > 0:

            current_caja = self.get_current_caja()

            these_transactions = df_transacciones.copy()
            these_transactions = these_transactions.rename(columns={tipo: 'Cantidad_movimiento'})

            if tipo in ['PRÉSTAMO']:
                these_transactions['Cantidad_movimiento'] = these_transactions['Cantidad_movimiento']*-1
            
            these_transactions['Caja'] = these_transactions['Cantidad_movimiento'].cumsum()

            these_transactions['Caja'] += current_caja
            these_transactions['Timestamp'] = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
            these_transactions['Sesion_id'] = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
            these_transactions = these_transactions.reset_index(drop=False)
            these_transactions = these_transactions.rename(columns={'index': 'Socio_id'})
            these_transactions['Acuerdo_id'] = self.acuerdos['Acuerdo_id']
            these_transactions['Catalogo_id'] = config.tipo_xls_catalogo_bd[tipo]

            self.transacciones_bd = pd.concat([self.transacciones_bd, these_transactions], axis=0, ignore_index=True)


    def append_transacciones_pago_prestamo_bd(self):

        these_transactions = self.prestamos.loc[self.prestamos['Ultimo_abono'] > 0, :].copy()

        if these_transactions.shape[0] > 0:

            current_caja = self.get_current_caja()

            these_transactions = these_transactions.rename(columns={'Ultimo_abono': 'Cantidad_movimiento'})            
            these_transactions['Caja'] = these_transactions['Cantidad_movimiento'].cumsum()

            these_transactions['Caja'] += current_caja
            these_transactions['Timestamp'] = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
            these_transactions['Sesion_id'] = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
            these_transactions['Acuerdo_id'] = self.acuerdos['Acuerdo_id']
            these_transactions['Transaccion_id'] = -1000
            these_transactions['Catalogo_id'] = 'ABONO_PRESTAMO'
            these_transactions['Prestamo_id'] = these_transactions['Prestamo_id'].astype(int)

            these_transactions = these_transactions[config.bd_columnas_transacciones+['Prestamo_id', 'Ultimo_interes_pagado', 'sobrante_abono']]
            these_transactions = these_transactions.reset_index(drop=True)
 
            self.transacciones_bd = pd.concat([self.transacciones_bd, these_transactions], axis=0, ignore_index=True)

    def append_transacciones_entrega_prestamo_bd(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        these_prestamos = self.prestamos.loc[self.prestamos['Sesion_id'] == this_sesion, :].copy()

        if these_prestamos.shape[0] > 0:

            current_caja = self.get_current_caja()

            these_prestamos['Monto_prestamo'] = these_prestamos['Monto_prestamo']*-1
            these_prestamos = these_prestamos.rename(columns={'Monto_prestamo': 'Cantidad_movimiento'})
            these_prestamos['Caja'] = these_prestamos['Cantidad_movimiento'].cumsum()
            these_prestamos['Caja'] = these_prestamos['Caja'] + current_caja
            these_prestamos['Timestamp'] = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
            these_prestamos['Acuerdo_id'] = self.acuerdos['Acuerdo_id']
            these_prestamos['Catalogo_id'] = 'ENTREGA_PRESTAMO'
            these_prestamos['Transaccion_id'] = 0

            these_prestamos = these_prestamos[config.bd_columnas_transacciones+['Prestamo_id']]


            self.transacciones_bd = pd.concat([self.transacciones_bd, these_prestamos], axis=0, ignore_index=True)

    def append_transacciones_multas_bd(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        these_multas = self.multas.loc[self.multas['Pago_en_sesion'] == 1, :].copy()

        if these_multas.shape[0] > 0:

            current_caja = self.get_current_caja()

            these_multas = these_multas.rename(columns={'Monto_multa': 'Cantidad_movimiento'})
            these_multas['Caja'] = these_multas['Cantidad_movimiento'].cumsum()
            these_multas['Caja'] = these_multas['Caja'] + current_caja
            these_multas['Timestamp'] = datetime.strftime(datetime.now(), format='%Y-%m-%d %H:%M:%S')
            these_multas['Acuerdo_id'] = self.acuerdos['Acuerdo_id']
            these_multas['Catalogo_id'] = 'PAGO_MULTA'
            these_multas['Transaccion_id'] = 0
            these_multas['Sesion_id'] = this_sesion

            these_multas = these_multas[config.bd_columnas_transacciones+['Multa_id']]

            self.transacciones_bd = pd.concat([self.transacciones_bd, these_multas], axis=0, ignore_index=True)


    def prepare_insert_prestamo(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        these_prestamos = self.prestamos.loc[self.prestamos['Sesion_id'] == this_sesion, :].copy()

        if these_prestamos.shape[0] > 0:
            these_prestamos = these_prestamos[config.bd_columnas_prestamo]
            these_prestamos = these_prestamos.drop('Prestamo_id', axis=1)
            these_prestamos = these_prestamos.to_dict('records')

            prestamo_ids = qc.insert_prestamo(these_prestamos)
            index_prestamos = self.prestamos.loc[self.prestamos['Sesion_id'] == this_sesion, 'Prestamo_id'].index
            prestamo_ids = prestamo_ids.set_index(index_prestamos)
            self.prestamos.loc[self.prestamos['Sesion_id'] == this_sesion, 'Prestamo_id'] = prestamo_ids['Prestamo_id']

    def prepare_insert_multa(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        these_multas = self.multas.loc[self.multas['Sesion_id'] == this_sesion, :].copy()

        if these_multas.shape[0] > 0:
            these_multas = these_multas[config.columnas_multa]
            these_multas = these_multas.drop('Multa_id', axis=1)
            these_multas = these_multas.to_dict('records')

            multa_ids = qc.insert_multa(these_multas)
            index_multas = self.multas.loc[self.prestamos['Sesion_id'] == this_sesion, 'Multa_id'].index
            multa_ids = multa_ids.set_index(index_multas)
            self.multas.loc[self.multas['Sesion_id'] == this_sesion, 'Multa_id'] = multa_ids['Multa_id']


    def prepare_insert_transacciones(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        these_transacciones = self.transacciones_bd.loc[self.transacciones_bd['Sesion_id'] == this_sesion, :].copy()

        if these_transacciones.shape[0] > 0:
            these_transacciones = these_transacciones[config.bd_columnas_transacciones]
            these_transacciones = these_transacciones.drop('Transaccion_id', axis=1)
            these_transacciones = these_transacciones.to_dict('records')
            
            transaccion_ids = qc.insert_transaccion(these_transacciones)
            index_transacciones = self.transacciones_bd.loc[self.transacciones_bd['Sesion_id'] == this_sesion, 'Transaccion_id'].index
            transaccion_ids = transaccion_ids.set_index(index_transacciones)
            self.transacciones_bd.loc[self.transacciones_bd['Sesion_id'] == this_sesion, 'Transaccion_id'] = transaccion_ids['Transaccion_id']


    def prepare_insert_transacciones_prestamo(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        pago_prestamo_now = self.transacciones_bd.loc[((self.transacciones_bd['Sesion_id'] == this_sesion) & 
                                                      (self.transacciones_bd['Catalogo_id'] == "ABONO_PRESTAMO")),:].copy()
        pago_prestamo_now = pago_prestamo_now.reset_index(drop=True)
        pago_prestamo_now['Prestamo_id'] = pago_prestamo_now['Prestamo_id'].astype("Int64")

        if pago_prestamo_now.shape[0] > 0:

            pago_prestamo_now['Transaccion_prestamo_id'] = 0
            pago_prestamo_now = pago_prestamo_now.rename(columns={'sobrante_abono':'Monto_abono_prestamo' ,'Ultimo_interes_pagado': 'Monto_abono_interes'})
            pago_prestamo_now = pago_prestamo_now[config.bd_columnas_transacciones_prestamos]
            pago_prestamo_now_dict = pago_prestamo_now.to_dict('records')
            pago_prestamo_now = pago_prestamo_now.reset_index()

            transaccion_prestamos_ids = qc.insert_transaccion_prestamo(pago_prestamo_now_dict)
            pago_prestamo_now['Transaccion_prestamo_id'] = transaccion_prestamos_ids['Transaccion_prestamo_id']

            self.transacciones_prestamo_bd = pd.concat([self.transacciones_prestamo_bd, pago_prestamo_now], axis=0, ignore_index=True)


    def prepare_insert_interes_prestamo(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        interes_prestamo_now = self.interes_prestamo.loc[(self.interes_prestamo['Sesion_id'] == this_sesion),:].copy()

        if interes_prestamo_now.shape[0] > 0:

            interes_prestamo_now = interes_prestamo_now.drop('Interes_prestamo_id', axis=1)
            interes_prestamo_now = interes_prestamo_now.to_dict('records')

            interes_prestamo_ids = qc.insert_interes_prestamo(interes_prestamo_now)
            index_interes_prestamo = self.interes_prestamo.loc[self.interes_prestamo['Sesion_id'] == this_sesion, 'Interes_prestamo_id'].index
            interes_prestamo_ids = interes_prestamo_ids.set_index(index_interes_prestamo)
            self.interes_prestamo.loc[self.interes_prestamo['Sesion_id'] == this_sesion, 'Interes_prestamo_id'] = interes_prestamo_ids['Interes_prestamo_id']


    def prepare_insert_ganancias(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        these_ganancias = self.ganancias.loc[self.ganancias['Sesion_id'] == this_sesion, :].copy()

        if these_ganancias.shape[0] > 0:
            these_ganancias = these_ganancias[config.columnas_ganancias.keys()]
            these_ganancias = these_ganancias.drop('Ganancias_id', axis=1)
            these_ganancias = these_ganancias.to_dict('records')

            ganancia_ids = qc.insert_ganancia(these_ganancias)
            index_ganancias = self.ganancias.loc[self.ganancias['Sesion_id'] == this_sesion, 'Ganancias_id'].index
            ganancia_ids = ganancia_ids.set_index(index_ganancias)
            self.ganancias.loc[self.ganancias['Sesion_id'] == this_sesion, 'Ganancias_id'] = ganancia_ids['Ganancias_id']


    def prepare_insert_asistencia(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']

        these_asistencias = self.socios_acciones.copy()
        these_asistencias['Presente'] = 1
        these_asistencias['Sesion_id'] = this_sesion
        these_asistencias = these_asistencias[config.columnas_asistencia].to_dict('records')

        qc.insert_asistencia(these_asistencias)

    def prepare_update_prestamo(self):

        if self.old_prestamos.shape[0] > 0:

            old_prestamos_active = self.old_prestamos.loc[self.old_prestamos['Estatus_prestamo'] == 0, :].copy()
            #old_prestamos = self.prestamos.loc[self.prestamos['Sesion_id'] != this_sesion, :].copy()

            if old_prestamos_active.shape[0] > 0:

                old_prestamos_active = pd.merge(left=self.old_prestamos, right=self.prestamos, on='Prestamo_id', how='left', suffixes=("","_new"))

                for i in range(old_prestamos_active.shape[0]):

                    dict_update_prestamo = dict()
                    dict_update_prestamo['Prestamo_id'] = old_prestamos_active.loc[i,'Prestamo_id']
                    for col_update in config.columnas_update_prestamo:
                        dict_update_prestamo[col_update] = old_prestamos_active.loc[i,col_update+'_new']
                    
                    qc.update_prestamo(dict_update_prestamo)

    def prepare_update_multa(self):

        this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
        pago_multa_now = self.transacciones_bd.loc[(self.transacciones_bd['Sesion_id'] == this_sesion) and 
                                                   (self.transacciones_bd['Catalogo_id'] == "PAGO_MULTA"),:]
        pago_multa_now['Multa_id'] = pago_multa_now['Multa_id'].astype("Int64")

        if pago_multa_now.shape[0] > 0:

            for i in range(pago_multa_now.shape[0]):

                dict_update_multa = dict()
                dict_update_multa['Multa_id'] = pago_multa_now.loc[i,'Multa_id']
                dict_update_multa['Status'] = pago_multa_now.loc[i,'Status']
                dict_update_multa['Transaccion_id'] = pago_multa_now.loc[i,'Transaccion_id']
                
                qc.update_multa(dict_update_multa)


    def actualiza_todo_sesion(self, xls_name, session_num, type_xls='MAYRA', bd_update=False):

        #dict_users = qc.get_socios_grupo(self.id_grupo)
        #print(dict_users)
        dict_users = self.socios_acciones['Socio_id'].to_dict()
        dict_session = ms.read_transform_info_xls(xls_name, session_num, dict_users, type_xls=type_xls)

        self.old_prestamos = self.prestamos.copy(deep=True)
        self.old_multas = self.multas.copy(deep=True)

        # self.acuerdos = pd.Series(qc.get_acuerdos_grupo(id_grupo))
        self.actualiza_sesiones(bd_update)

        self.actualiza_interes_prestamos(self.sesiones[-1]['Sesion_id'])

        self.cambia_status_socio(dict_session['STATUS_SOCIOS'])

        self.crea_nuevos_acuerdos(dict_session['NUEVOS_ACUERDOS'], xls_name)

        self.actualiza_multas(dict_session['MULTAS'], self.sesiones)

        self.actualiza_acciones(dict_session['COMPRA_ACCIONES'], self.acuerdos['Costo_acciones'], tipo='COMPRA_ACCIONES')

        self.actualiza_pago_prestamos(dict_session['ABONO'])

        self.limite_credito_socios = self.calcular_limite_credito()
        
        self.actualiza_prestamos(dict_session['PRÉSTAMO'], self.sesiones)
        
        self.actualiza_pago_multas(dict_session['PAGO_MULTA'])

        self.actualiza_acciones(dict_session['RETIRO_ACCIONES'], self.acuerdos['Costo_acciones'], tipo='RETIRO_ACCIONES')

        self.calcula_ganancias(self.sesiones)

        if bd_update:

            self.prepare_insert_asistencia()

            self.prepare_insert_interes_prestamo()

            self.prepare_insert_prestamo()
            self.prepare_insert_multa()

            # Enlista todas las transacciones de la sesion (table TRANSACCIONES)
            self.append_transacciones_bd(dict_session['COMPRA_ACCIONES'], tipo='COMPRA_ACCIONES')
            self.append_transacciones_pago_prestamo_bd()
            self.append_transacciones_entrega_prestamo_bd()
            self.append_transacciones_multas_bd()
            self.append_transacciones_bd(dict_session['RETIRO_ACCIONES'], tipo='RETIRO_ACCIONES')
            self.prepare_insert_transacciones()

            self.prepare_update_prestamo()
            self.prepare_insert_transacciones_prestamo()

            self.prepare_insert_ganancias()

            qc.update_socio_acciones(self.socios_acciones[config.columnas_socio_accion].to_dict('records'))

            this_sesion = self.sesiones_bd.loc[self.sesiones_bd.shape[0]-1, 'Sesion_id']
            qc.update_sesion(sesion_id=this_sesion, caja=self.caja, 
                             acciones=self.socios_acciones['Acciones'].sum(),
                             ganancias=self.ganancias['Monto_ganancia'].sum(),
                             activa=0)





        self.interes_futuro_prestamo = self.calcular_interes_futuro()