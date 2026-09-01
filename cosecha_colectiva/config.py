
import os
import pathlib

api_url = 'https://8jipefwkrc.execute-api.us-east-1.amazonaws.com'

url_dict = {
    'url_login': api_url+"/auth/login",
    'url_login_restore': api_url+"/auth/restore_password",
    'url_login_register': api_url+"/auth/register",
    'url_groups_create':  api_url+"/groups",
    'url_groups_join': api_url+"/groups/join",
    'url_groups_deactivate_user': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/update_status",
    'url_groups_delete_user': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/delete_user",
    'url_groups_deposit_common_fund': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/deposit_to_common_fund",
    'url_groups_retreat_common_fund': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/retreat_to_common_fund",
    'url_groups_get_users': api_url+"/groups/{id_grupo}/users",
    'url_groups_get_active_users': api_url+"/groups/{id_grupo}/users?status=active",
    'url_groups_get_group': api_url+"/groups/{id_grupo}",
    'url_groups_get_group_user_info': api_url+"/groups/{id_grupo}/users/{id_socio}",
    'url_users_get_active_sessions': api_url+"/users/get_active_sessions",
    'url_users_get_groups': api_url+"/users/groups",
    'url_sessions_create': api_url+"/groups/{id_grupo}/sessions",
    'url_sessions_share_profit': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/profit/share_profit",
    'url_sessions_late_users': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/register_retardments",
    'url_sessions_schedule_next': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/schedule",
    'url_sessions_url_sign': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/sign",
    'url_sessions_get_session': api_url+"/groups/{id_grupo}/sessions/{id_sesion}",
    'url_sessions_get_summary': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/resume",
    'url_sessions_get_active_session': api_url+"/groups/{id_grupo}/get_active_session",
    'url_sessions_get_movements': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/movements",
    'url_sessions_get_user_movements': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/movements",
    'url_sessions_get_inassistance': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/get_inassistance",
    'url_sessions_end_session': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/finalize",
    'url_agreements_create': api_url+"/groups/{id_grupo}/agreements",
    'url_agreements_personalized_create': api_url+"/groups/{id_grupo}/personalized_agreements",
    'url_agreements_get_personalized_create': api_url+"/groups/{id_grupo}/personalized_agreements",
    'url_loans_create': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/loans",
    'url_loans_extend': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/loans/{id_prestamo}/extend",
    'url_loans_pay': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/loans/pay",
    'url_loans_get_one': api_url+"/groups/{id_grupo}/users/{id_socio}/loans/{id_prestamo}",
    'url_loans_get_mult': api_url+"/groups/{id_grupo}/users/{id_socio}/loans/",
    'url_loans_observations': api_url+"/loans/observations",
    'url_loans_get_users': api_url+"/groups/{id_grupo}/loans/users/",
    'url_loans_get_loan_info': api_url+"/groups/{id_grupo}/loans/users/loans_data",
    'url_penalties_assign': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/penalties",
    'url_penalties_predefined': api_url+"/groups/{id_grupo}/sessions/predefined_penalties",
    'url_penalties_assign_predefined': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/assign_predefined",
    'url_penalties_pay': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/penalties/pay",
    'url_penalties_pay_predefined': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/predefined_penalties/pay",
    'url_penalties_get_penalties_user': api_url+"/groups/{id_grupo}/users/{id_socio}/penalties",
    'url_penalties_get_all_penalties_user': api_url+"/groups/{id_grupo}/penalties?status=payed,unpayed",
    'url_penalties_get_predefined_penalties_user': api_url+"/groups/{id_grupo}/users/{id_socio}/predefined_penalties",
    'url_penalties_get_predefined_penalties': api_url+"/groups/{id_grupo}/predefined_penalties",
    'url_penalties_get_users': api_url+"/groups/{id_grupo}/penalties/users",
    'url_penalties_get_users': api_url+"/groups/{id_grupo}/predefined_penalties/users",
    'url_actions_buy': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/actions",
    'url_actions_retreat': api_url+"/groups/{id_grupo}/sessions/{id_sesion}/users/{id_socio}/actions/retreat",
    'url_actions_get': api_url+"/groups/{id_grupo}/users/{id_socio}/actions?status=payed,unpayed",
}


default_headers = headers = {
  'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VybmFtZSI6ImphdmllciIsIlNvY2lvX2lkIjo1NCwiaWF0IjoxNjY0Mjk0MzEyfQ.OTil_J1PWovrJWOCdAluB86eiZYMB5qC_zXvl5dFZ5w',
  'Content-Type': 'application/json',
  "Accept": "application/json",
}

current_dir = os.path.dirname(os.path.realpath(__file__))
xls_dir = pathlib.Path(pathlib.Path(current_dir).parent.absolute(),'xls_grupos')

log_dir = pathlib.Path(pathlib.Path(current_dir).parent.absolute(),'logs')

xls_types = {
    'Usuarios': {"Fecha_nac": "str", "CP": "str", "Telefono": "str", "Password": "str"},
    'Grupo': str,
    'Acuerdos': str,
    'Hoja1': str,
    'EstadisticaInicial': str
}
xls_headers = {
    'Usuarios': 0,
    'Acuerdos': 0,
    'Grupo': 0,
    'Hoja1': None,
    'EstadisticaInicial': None
}

default_group_data = {
  "Localidad": "Querétaro",
  "Municipio": "Querétaro",
  "Estado": "Querétaro",
  "CP": "76903",
  "Pais": "México"
}

xls_session_words_dict = {
    
  "AHORRO": 0,
  "PRÉSTAMO": 1,
  "ABONO": 2,
  "MULTAS": 3,
  "INTERÉS": 4
}

xls_session_words_dict_2 = {
    
  "COMPRA_ACCIONES": 0,
  "PAGO_MULTA": 1,
  "ABONO": 2,
  "PRÉSTAMO": 3,
  "AMPLIACIÓN": 4,
  "NUM_SESIONES": 5,
  "RETIRO_ACCIONES": 6,
  "MULTAS": 7,
  "NUEVOS_ACUERDOS": 8,
  "NUEVOS_SOCIOS": 9,
  "STATUS_SOCIOS": 10,
  "GANANCIAS": 11
}

xls_words_int = ['COMPRA_ACCIONES', 'PAGO_MULTA', 'AMPLIACIÓN', 'NUM_SESIONES', 'RETIRO_ACCIONES']

column_xls_words = 1
month_words = ["ENE", "FEB", "MAR", "ABR", "MAY", "JUN", "JUL", "AGO", "SEP", "OCT", "NOV", "DIC"]

db_name = 'cosecha'


columnas_ganancias = {
    'Ganancias_id': int,
    'Ganancia_accion': float,
    'Monto_ganancia': float,
    'Entregada': int,
    'Socio_id': int,
    'Sesion_id': int,
    'periodo': str
} 



columnas_interes_prestamo = {
    'Interes_prestamo_id': int,
    'Prestamo_id': int,
    'Sesion_id': int,
    'Monto_interes': float,
    'Tipo_interes': int
}

columnas_interes_prestamo_comp = columnas_interes_prestamo.copy()
columnas_interes_prestamo_comp.pop("Interes_prestamo_id", None)

columnas_socio_accion = ['Grupo_socio_id', 'Socio_id', 'Grupo_id', 'Tipo_socio', 'Acciones', 'Status', 'unique_key']

columnas_socio = ['CURP']

columnas_socio_accion_plus = columnas_socio_accion+columnas_socio

columnas_acuerdos = ['Grupo_id', 'Status', 'Tasa_interes', 'Limite_credito', 'Creditos_simultaneos', 
                    'Interes_morosidad', 'Ampliacion_prestamos', 'Interes_ampliacion', 
                    'Mod_calculo_interes', 'Tasa_interes_prestamo_grande']

columnas_acuerdos_prestamo = ['Grupo_id', 'Tasa_interes', 'Limite_credito', 'Creditos_simultaneos', 
                    'Interes_morosidad', 'Ampliacion_prestamos', 'Interes_ampliacion', 
                    'Mod_calculo_interes', 'Tasa_interes_prestamo_grande']


columnas_asistencia = ['Socio_id', 'Sesion_id', 'Presente']

bd_columnas_prestamo = ['Prestamo_id', 'Monto_prestamo', 'Monto_pagado', 'Interes_generado', 'Interes_pagado',
                        'Fecha_inicial', 'Fecha_final', 'Estatus_ampliacion', 'Observaciones', 'Num_sesiones', 
                        'Sesiones_restantes', 'Estatus_prestamo', 'Socio_id', 'Sesion_id', 'Acuerdos_id', 'Prestamo_original_id']

columnas_prestamos = bd_columnas_prestamo + columnas_acuerdos_prestamo

columnas_extras_prestamo = ['interes_futuro', 'Ultimo_interes_pagado', 'debe_interes', 'sobrante_abono', 'Ultimo_abono', 'Status_socio']

columnas_prestamos_final = columnas_prestamos + columnas_extras_prestamo

columnas_update_prestamo = ['Monto_pagado', 'Interes_generado', 'Interes_pagado', 'Sesiones_restantes',
                            'Estatus_prestamo', 'Estatus_ampliacion', 'Prestamo_original_id']

columnas_multa = ['Multa_id', 'Monto_multa', 'Descripcion', 'Status', 'Sesion_id', 'Socio_id', 'Transaccion_id', 'created_at']
columnas_multa_extra = ['Pago_en_sesion']

columnas_multa_final = columnas_multa + columnas_multa_extra

bd_columnas_sesiones = ['Sesion_id', 'Fecha', 'Activa', 'Caja', 'Acciones', 'Ganancias', 'Fecha_prox_reunion', 'Lugar_prox_reunion', 'Tipo_sesion', 'Grupo_id', 'created_at']

bd_columnas_transacciones = ['Transaccion_id', 'Cantidad_movimiento', 'Caja', 'Timestamp', 'Sesion_id', 'Socio_id', 'Acuerdo_id', 'Catalogo_id']

bd_columnas_transacciones_prestamos = ['Transaccion_prestamo_id', 'Prestamo_id', 'Transaccion_id', 'Monto_abono_prestamo', 'Monto_abono_interes']

bd_columnas_interes_prestamo = ['Interes_prestamo_id', 'Prestamo_id', 'Sesion_id', 'Monto_interes', 'Tipo_interes']

acuerdos_var_type ={
    'Acuerdo_id': int,
    'Grupo_id': int,
    'Fecha_acuerdos':False,
    'Fecha_acuerdos_fin':False,
    'Status': int,
    'Periodo_reuniones': int,
    'Periodo_cargos': int,
    'Limite_inasistencias': int,
    'Minimo_aportacion': int,
    'Costo_acciones': int,
    'Tasa_interes': float,
    'Limite_credito': int,
    'Porcentaje_fondo_comun': int,
    'Creditos_simultaneos': int,
    'Interes_morosidad': float,
    'Ampliacion_prestamos': int,
    'Interes_ampliacion': float,
    'Mod_calculo_interes': int,
    'Tasa_interes_prestamo_grande': float,
    'Id_socio_administrador': int,
    'Id_socio_administrador_suplente': int
}

socio_acciones_min_columnas = ['']


dict_file_name_sheets_id = {
  'GrupoCafam': '1zH7yr2HcpdOPa2_rgUSYJvsdmPXKP4n8HnunF3e0Fpo',
  'CAF_GrupoTroll': '15DHCtIefdGAN2U6mmauchlIAp_7XGDdOKH0Ydwj3RQw',
  'CAF_GrupoTesteador': '1Hak3P-S9R-nnLAJiMWG0NJD5p6VYJBG4p9Mv94yhTyY',
  'CAF_GrupoTest': '1AW0yEOczQxp31GKyMDJfyQ0UCEQdlZyRs2eroYYc4UY',
  'CAF_GrupoMesero': '104_k4Lp_L06k6WBrg4L70nReOXtWVqYnONHrRCcEPgM',
  'CAF_GrupoHidalgo': '1bIZhM7sriUn6W0C3xhggqll_L80dSlY564fV7TxUZ9',
  'CAF_GrupoDomador': '1BaIdWwwf-D1dtoQ7ndiawGt7YbbMY_-UkB_mIr2jbyA',
  'CAF_GrupoCalcetin': '1TlZY_Q42dgfzWDEWo30TabPsZTEsABPqjdVyUSXy2Nk',
  'CAF_GrupoCafam': '12_xaG2gVzYJlvqxB5boWfCWWIsCgbI5j3icfmbYQFVY',
  'CAF_GrupoAstronomo': '1SvXHIPjpFJTOisxbnywv7q2oR7JuEh_McBsJhX44tQg',
  'CAF_GrupoAstronomo': '1SvXHIPjpFJTOisxbnywv7q2oR7JuEh_McBsJhX44tQg',
  'Alcancía Viva': '1j_5iiWZc4_mJQrLkkCJ_RPNnRtQmltkG4xLQCEfGPPE',
  'Alcancía Viva Test': '10nsHasjnFOfJU91gDpGVzU3CrmwAYoKuwg7w1ztbJwY',
  'Licenciados_Asociados': '1wT3AM5PicaYV2Xb0zhAsuiIcNq1z8fDgJXHB1dM4vgI',
  'Licenciados_Asociados_Test': '19J_mY3CTbAaGw0UsvJjmzFb4ogwovV0SPRsu_yAr29M',
  'Tropa Gusgones_2.0': '1Id_SdXxQAazStVcml8r6xOYSrhJlCmpQ3zkD_fPuv80',
  'Tropa Gusgones_2.0_Test': '1RN8X5oGUfySW-vj2MTOn2PTWQNTCBLKIW8Tg1SbcqEA',
  'Verde_Futuro': '1cmgpC1G2CMQQ7GBmhH2ysBYwB7IUOYafjxMXuysNGgM',
  'Verde_Futuro_Test': '1c9yXCTfw2XYm0mZn_0QGACEsad6zuZR0C6tIF9xABjg',
  'Juntas_ahorramos': '1FoyLFtCozhO3OAuPuhQAB5XntdOkLdaiaQdD1wZkX-g',
  'Juntes_ahorramos': '1vzxtidvPzXJlf1gElEKhbPkVpCY5IUUni1oPGMgeK0U',
  'Juntos_ahorramos': '1bL90wDGK8__iapMrMR1FrbFlh8ex0lnlc2FNNCw7IvY',
  'Procoseq': '11lSzrH-cOJ1ru5--GHE2M2dq-2mLzhh_n02jgObvMW0',
  'CAFAM': '1Fz-4xSiklTlW3m7SMf0Z-bZLbdyys3XcUUgxf5WMrzo',
  'La Colmena': '1WNT4gL6yfZb6z0kgJo0GknV7ZRZxE6H6ThdK_pGssXc',
  'La Llave Beatriz': '1sTreMnia6Ez54uHMRUXgpgE4Um9gZbZzxKSBuLzJ1E4',
  'Caja ahorro magueyeros': '1OR13miCG2O3Rc8gk-DgjVtmhxrmAV3w4EbHdJ3XSS4w',
  'Caja la llave': '1WazkPgG1qg7MqXBNZjRNMqqsHsCpCI5PNJ-grs8wq1I',
  'Agrosemilla sin varo': '1yj7-CIEKjFyz9KSTayRdVDVBggkdMOD3-tiDNj5UQ80',
  'Caja llanitos Hidalgo': '1k_qWCRkdZoxZLeI5y0R3i2SYmD8uKMgFEtdCf4l6Sco',
  'Caja Cadereyta': '1FxIHK7NNRQz95_RlfEeuMeSS900KwGWC5dn9Y9nbxqs',
}

tipo_xls_catalogo_bd = {
    'COMPRA_ACCIONES': 'COMPRA_ACCION',
    'ABONO': 'ABONO_PRESTAMO',
    'RETIRO_ACCIONES': 'RETIRO_ACCION',
    'PRÉSTAMO': 'ENTREGA_PRESTAMO'
}

list_viejos_grupos_sheets_id = [
'1J27LD66BBIBqgFAujwDV-JHYFka0HkBtJDbJPN7NhT0',
'1cUQrMn8Ab29CM4369D6QdJ71Fsc-mXwGPmk6RX64rv8',
'1JGfGj0WNU7kFED_gtJg9kk6aXZjImp5YJlz155Oz9gs',

]