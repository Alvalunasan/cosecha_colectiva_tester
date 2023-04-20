

import pandas as pd

from cosecha_colectiva import test_cosecha_api as tca


def test_crear_grupo(xls_name):

    tca.CAF_API_group_creation_tester.main_create_group(xls_name)


def test_sesion_months(xls_name,start_month, end_month, type_xls='MAYRA'):

    for month in range(start_month, end_month+1):
        tca.CAF_API_sessions_tester.main_create_sesion(xls_name, month, type_xls=type_xls)
