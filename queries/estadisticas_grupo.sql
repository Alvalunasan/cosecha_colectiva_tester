


WITH grupo_sesion AS
  (
select 
g.Grupo_id,
s.sesion_id
from railway.grupos g 

inner join railway.sesiones s
on s.grupo_id = g.grupo_id

where g.datos_dashboard = 1 
)

select 

g.*,
grupo_prestamos.grupo_id_prestamos,
ifnull(grupo_prestamos.sum_monto_prestamo,0) as sum_monto_prestamo,
ifnull(grupo_prestamos.promedio_monto_prestamo,0) as promedio_monto_prestamo,
ifnull(grupo_prestamos.sum_monto_pagado,0) as sum_monto_pagado,
ifnull(grupo_prestamos.sum_interes_generado,0) as sum_interes_generado,
ifnull(grupo_prestamos.sum_interes_pagado,0) as sum_interes_pagado,
ifnull(grupo_prestamos.num_prestamos_ampliacion,0) as num_prestamos_ampliacion,
ifnull(grupo_prestamos.monto_riesgo,0) as monto_riesgo,
ifnull(grupo_prestamos.num_prestamos_vencidos,0) as num_prestamos_vencidos,
ifnull(grupo_prestamos.num_prestamos_pagados,0) as num_prestamos_pagados,
ifnull(grupo_prestamos.num_prestamos_vigentes,0) as num_prestamos_vigentes,
ifnull(grupo_prestamos.num_prestamos,0) as num_prestamos,
grupo_prestamos.sum_monto_prestamo / grupo_transacciones.sum_compra_acciones as prestamo_vs_ahorro,
grupo_multas.*,
grupo_prestamos.sum_interes_pagado as ganancias_interes,
grupo_prestamos.sum_interes_pagado + grupo_multas.sum_monto_multa_pagadas as ganancias_interes_multas,
grupo_asistencias.*,
grupo_socio.*,
grupo_ganancias.*,
max(grupo_acuerdos.tasa_interes) as tasa_interes,
max(grupo_acuerdos.limite_credito) as limite_credito,
grupo_sesion_stats.num_sesiones as num_sesiones,
grupo_transacciones.*,
max(gc.Color_grupo) as color_grupo

from railway.grupos g

left join
(
select
grupo_sesion.Grupo_id as grupo_id_prestamos,
sum(p.monto_prestamo) as sum_monto_prestamo,
avg(p.monto_prestamo) as promedio_monto_prestamo,
sum(p.monto_pagado) as sum_monto_pagado,
sum(p.interes_generado) as sum_interes_generado,
sum(p.interes_pagado) as sum_interes_pagado,
sum(p.estatus_ampliacion) as num_prestamos_ampliacion,
sum(case when p.sesiones_restantes = 1 then p.monto_prestamo-p.monto_pagado else 0 end) as monto_riesgo,
sum(case when p.sesiones_restantes < 0 then 1 else 0 end) as num_prestamos_vencidos,
sum(p.estatus_prestamo) as num_prestamos_pagados,
sum(case when p.estatus_prestamo = 0 then 1 else 0 end) as num_prestamos_vigentes,
count(p.prestamo_id) as num_prestamos


from railway.prestamos p 


inner join grupo_sesion
on grupo_sesion.Sesion_id = p.Sesion_id

group by grupo_sesion.grupo_id
) grupo_prestamos
on grupo_prestamos.grupo_id_prestamos = g.Grupo_id


left join
(
select 

grupo_sesion.Grupo_id as grupo_id_multas,
sum(m.monto_multa) as sum_monto_multa_total,
sum(case when m.status =1 then m.monto_multa else 0 end) as sum_monto_multa_pagadas,
count(m.multa_id) as num_multas


from railway.multas m

inner join grupo_sesion
on grupo_sesion.Sesion_id = m.Sesion_id

group by grupo_sesion.grupo_id
) grupo_multas
on grupo_multas.grupo_id_multas = g.Grupo_id

left join
(
select 

grupo_sesion.Grupo_id as grupo_id_asistencias,
sum(case when a.presente>=1 then 1 else 0 end) / count(a.asistencia_id) as tasa_asistencia,
sum(case when a.presente=2 then 1 else 0 end) / count(a.asistencia_id) as tasa_retardos


from railway.asistencias a

inner join grupo_sesion
on grupo_sesion.Sesion_id = a.Sesion_id

group by grupo_sesion.grupo_id
) grupo_asistencias
on grupo_asistencias.grupo_id_asistencias = g.Grupo_id

left join
(
select 

gs.Grupo_id as grupo_id_socio,
count(gs.grupo_socio_id) as num_socios,
sum(gs.acciones) as total_acciones,
sum(case when gs.status=1 then 1 else 0 end) as num_socios_activos,
sum(case when gs.status=2 then 1 else 0 end) as num_socios_congelados,
sum(case when gs.status=0 then 1 else 0 end) as num_socios_inactivos,
sum(case when gs.status=1 then 1 else 0 end)/count(gs.grupo_socio_id) as tasa_socios_activos,
sum(case when gs.status=2 then 1 else 0 end)/count(gs.grupo_socio_id) as tasa_socios_congelados,
sum(case when gs.status=0 then 1 else 0 end)/count(gs.grupo_socio_id) as tasa_socios_inactivos,
sum(case when s.Sexo = 'M' then 1 else 0 end) as num_mujeres,
sum(case when s.Sexo = 'H' then 1 else 0 end) as num_hombres,
sum(case when (TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 18) then 1 else 0 end) as num_0_18, 
sum(case when TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) > 18 and TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 35 then 1 else 0 end) as num_19_35, 
sum(case when TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) > 35 and TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 65 then 1 else 0 end) as num_36_65,
sum(case when TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) > 66 then 1 else 0 end) as num_66_plus,
avg(TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE())) as edad_promedio


from railway.grupo_socio gs

inner join socios s
on gs.Socio_id = s.socio_id

group by gs.grupo_id
) grupo_socio
on grupo_socio.grupo_id_socio = g.grupo_id

left join
(
select 

grupo_sesion.Grupo_id as grupo_id_ganancias,
sum(ga.monto_ganancia) as ganancias_totales,
sum(case when Entregada =1 then ga.monto_ganancia else 0 end) as ganancias_entregadas


from railway.ganancias ga

inner join grupo_sesion
on grupo_sesion.Sesion_id = ga.Sesion_id

group by grupo_sesion.grupo_id
) grupo_ganancias
on grupo_ganancias.grupo_id_ganancias = g.Grupo_id


left join
(
select 
a.grupo_id as grupo_id_acuerdos,
max(a.tasa_interes) as tasa_interes,
max(a.limite_credito) as limite_credito

from acuerdos a 

where a.status=1
group by grupo_id
) grupo_acuerdos
on grupo_acuerdos.grupo_id_acuerdos = g.grupo_id


left join
(
select
grupo_id as grupo_id_sesion,
count(s.sesion_id) as num_sesiones


from sesiones s
group by grupo_id
) grupo_sesion_stats
on  grupo_sesion_stats.grupo_id_sesion = g.grupo_id


left join
(
select 

grupo_sesion.Grupo_id as grupo_id_transacciones,
sum(case when catalogo_id = 'ABONO_PRESTAMO' then cantidad_movimiento else 0 end) as sum_abonos_prestamos,
sum(case when catalogo_id = 'ABONO_PRESTAMO' then 1 else 0 end) as count_abonos_prestamos,
sum(case when catalogo_id = 'COMPRA_ACCION' then cantidad_movimiento else 0 end) as sum_compra_acciones,
sum(case when catalogo_id = 'COMPRA_ACCION' then 1 else 0 end) as count_compra_acciones,
sum(case when catalogo_id = 'ENTREGA_PRESTAMO' then -1*cantidad_movimiento else 0 end) as sum_entrega_prestamos,
sum(case when catalogo_id = 'ENTREGA_PRESTAMO' then 1 else 0 end) as count_entrega_prestamos,
sum(case when catalogo_id = 'PAGO_MULTA' then cantidad_movimiento else 0 end) as sum_pago_multas,
sum(case when catalogo_id = 'PAGO_MULTA' then 1 else 0 end) as count_pago_multas,
sum(case when catalogo_id = 'RETIRO_ACCION' then -1*cantidad_movimiento else 0 end) as sum_retiro_acciones,
sum(case when catalogo_id = 'RETIRO_ACCION' then 1 else 0 end) as count_retiro_acciones



from railway.transacciones t

inner join grupo_sesion
on grupo_sesion.Sesion_id = t.Sesion_id

group by grupo_sesion.grupo_id
) grupo_transacciones
on grupo_transacciones.grupo_id_transacciones = g.Grupo_id

left join grupos_colores gc
on gc.grupo_id = g.grupo_id

where g.datos_dashboard = 1

group by g.Grupo_id


UNION 


(
select 

ogsd.*,
gc.Color_grupo

from old_grupos_stats_dashboard  ogsd

left join grupos_colores gc
on gc.grupo_id = ogsd.grupo_id
)
