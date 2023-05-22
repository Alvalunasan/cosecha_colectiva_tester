
import os
import pathlib

api_url = 'https://cosechacolectiva.herokuapp.com/api/'

url_dict = {
'url_socio': api_url+"socios",
'url_login': api_url+"socios/login",
'url_grupo': api_url+"grupos",
'url_info_grupo': api_url+"grupos/{id_grupo}",
'url_socio_grupo': api_url+ "socios/grupos",
'url_acuerdos': api_url+"grupos/{id_grupo}/acuerdos",
'url_crear_sesion': api_url+"grupos/{id_grupo}/sesiones",
'url_finalizar_sesion': api_url+"grupos/{id_grupo}/sesiones/finalizar",
'url_status_socio': api_url+"grupos/{id_grupo}/socios/{id_socio}/socios",
'url_compra_acciones': api_url+"grupos/{id_grupo}/socios/{id_socio}/acciones",
'url_retiro_acciones': api_url+"grupos/{id_grupo}/socios/{id_socio}/acciones/retirar",
'url_crear_multa': api_url+"grupos/{id_grupo}/socios/{id_socio}/multas/",
'url_pagar_multa': api_url+"grupos/{id_grupo}/multas/",
'url_generar_prestamo': api_url+"grupos/{id_grupo}/socios/{id_socio}/prestamos/",
'url_ampliar_prestamo': api_url+"grupos/{id_grupo}/socios/{id_socio}/prestamos/prestamos",
'url_pagar_prestamo': api_url+"grupos/{id_grupo}/prestamos/"
}

default_headers = headers = {
  'Authorization': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJVc2VybmFtZSI6ImphdmllciIsIlNvY2lvX2lkIjo1NCwiaWF0IjoxNjY0Mjk0MzEyfQ.OTil_J1PWovrJWOCdAluB86eiZYMB5qC_zXvl5dFZ5w',
  'Content-Type': 'application/json'
}

current_dir = os.path.dirname(os.path.realpath(__file__))
xls_dir = pathlib.Path(pathlib.Path(current_dir).parent.absolute(),'xls_grupos')

log_dir = pathlib.Path(pathlib.Path(current_dir).parent.absolute(),'logs')

xls_types = {
    'Usuarios': {"Fecha_nac": "str", "CP": "str", "Telefono": "str", "Password": "str"},
    'Acuerdos': str,
    'Hoja1': str,
    'EstadisticaInicial': str
}
xls_headers = {
    'Usuarios': 0,
    'Acuerdos': 0,
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

db_name = 'heroku_59f792929604e63'


columnas_ganancias = {
    'Ganancias_id': int,
    'Monto_ganancia': float,
    'Entregada': int,
    'Socio_id': int,
    'Sesion_id': int
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

columnas_socio_accion = ['Socio_id', 'Tipo_socio', 'Acciones', 'Status']

columnas_acuerdos = ['Grupo_id', 'Status', 'Tasa_interes', 'Limite_credito', 'Creditos_simultaneos', 
                    'Interes_morosidad', 'Ampliacion_prestamos', 'Interes_ampliacion', 
                    'Mod_calculo_interes', 'Tasa_interes_prestamo_grande']

columnas_acuerdos_prestamo = ['Grupo_id', 'Tasa_interes', 'Limite_credito', 'Creditos_simultaneos', 
                    'Interes_morosidad', 'Ampliacion_prestamos', 'Interes_ampliacion', 
                    'Mod_calculo_interes', 'Tasa_interes_prestamo_grande']
columnas_prestamos = ['Prestamo_id', 'Monto_prestamo', 'Monto_pagado', 'Interes_generado', 'Interes_pagado',
                    'Estatus_ampliacion', 'Num_sesiones', 'Sesiones_restantes', 'Estatus_prestamo', 'Socio_id',
                    'Sesion_id', 'Acuerdos_id', 'Prestamo_original_id'] + columnas_acuerdos_prestamo

columnas_extras_prestamo = ['interes_futuro', 'Ultimo_interes_pagado', 'debe_interes', 'sobrante_abono', 'Ultimo_abono', 'Status_socio']

columnas_prestamos_final = columnas_prestamos + columnas_extras_prestamo

columnas_multa = ['Multa_id', 'Monto_multa', 'Descripcion', 'Status', 'Sesion_id', 'Socio_id']

columnas_multa_extra = ['Pago_en_sesion']

columnas_multa_final = columnas_multa + columnas_multa_extra


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