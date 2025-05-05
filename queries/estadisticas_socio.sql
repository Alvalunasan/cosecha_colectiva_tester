
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

socio_grupo.grupo_id,
socio_grupo.Nombre_grupo,
concat('Socio ', rank() OVER ( partition by socio_grupo.grupo_id order by socio_grupo.socio_id), ' ',socio_grupo.nombre_grupo) AS num_socio_grupo,
s.socio_id,
s.nombres,
s.apellidos,
s.nacionalidad,
s.sexo,
s.Escolaridad,
s.Ocupacion,
s.Estado_civil,
s.Hijos,
s.Localidad,
s.Municipio,
s.Estado,
s.Fecha_reg,
TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) edad,
case 
when (TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 2) then "NA"
when (TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) > 2 and TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 18) then "-18"
when (TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) > 18 and TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 35) then "19-35"
when (TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) > 35 and TIMESTAMPDIFF(YEAR, s.Fecha_nac, CURDATE()) <= 65) then "36-65"
else "66+" end as rango_edad,
case 
when min(socio_grupo.status)=1 then 'Activo'
when min(socio_grupo.status)=2 then 'Congelado'
when min(socio_grupo.status)=0 then 'Inactivo'
end as status_socio,
socio_prestamos.socio_id_prestamos,
socio_prestamos.grupo_id_prestamos,
ifnull(socio_prestamos.sum_monto_prestamo,0) as sum_monto_prestamo,
ifnull(socio_prestamos.promedio_monto_prestamo,0) as promedio_monto_prestamo,
ifnull(socio_prestamos.sum_monto_pagado,0) as sum_monto_pagado,
ifnull(socio_prestamos.sum_interes_generado,0) as sum_interes_generado,
ifnull(socio_prestamos.sum_interes_pagado,0) as sum_interes_pagado,
ifnull(socio_prestamos.num_prestamos_ampliacion,0) as num_prestamos_ampliacion,
ifnull(socio_prestamos.monto_riesgo,0) as monto_riesgo,
ifnull(socio_prestamos.num_prestamos_vencidos,0) as num_prestamos_vencidos,
ifnull(socio_prestamos.num_prestamos_pagados,0) as num_prestamos_pagados,
ifnull(socio_prestamos.num_prestamos_vigentes,0) as num_prestamos_vigentes,
ifnull(socio_prestamos.num_prestamos,0) as num_prestamos,
sum(socio_grupo.acciones) as acciones_actuales,
case when sum(socio_transacciones.sum_compra_acciones) > 0 then socio_prestamos.sum_monto_prestamo / sum(socio_transacciones.sum_compra_acciones) else NULL end as prestamo_vs_ahorro,
socio_multas.*,
socio_asistencias.*,
socio_ganancias.*,
socio_transacciones.*


from railway.socios s


inner join 
(
select 
g.Grupo_id,
g.Nombre_grupo,
gs.socio_id,
gs.status,
gs.acciones
from railway.grupos g 

inner join railway.grupo_socio gs 
on gs.grupo_id = g.grupo_id

where g.datos_dashboard = 1 
) socio_grupo
on socio_grupo.socio_id = s.socio_id



left join
(
select
p.socio_id as socio_id_prestamos,
grupo_sesion.grupo_id as grupo_id_prestamos,
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

group by grupo_sesion.grupo_id, p.socio_id
) socio_prestamos
on socio_prestamos.grupo_id_prestamos = socio_grupo.grupo_id 
and socio_prestamos.socio_id_prestamos = s.socio_id



left join
(
select 

m.socio_id as socio_id_multas,
grupo_sesion.grupo_id as grupo_id_multas,
sum(m.monto_multa) as sum_monto_multa_total,
sum(case when m.status =1 then m.monto_multa else 0 end) as sum_monto_multa_pagadas,
count(m.multa_id) as num_multas


from railway.multas m

inner join grupo_sesion
on grupo_sesion.Sesion_id = m.Sesion_id

group by grupo_sesion.grupo_id, m.socio_id
) socio_multas
on socio_multas.grupo_id_multas = socio_grupo.Grupo_id
and socio_multas.socio_id_multas = s.socio_id 


left join
(
select 

a.Socio_id  as socio_id_asistencias,
grupo_sesion.Grupo_id as grupo_id_asistencias,
count(a.asistencia_id) as num_sesiones_grupo,
sum(case when a.presente>=1 then 1 else 0 end) as num_sesiones_presente,
sum(case when a.presente>=1 then 1 else 0 end) / count(a.asistencia_id) as tasa_asistencia,
sum(case when a.presente=2 then 1 else 0 end) / count(a.asistencia_id) as tasa_retardos


from railway.asistencias a

inner join grupo_sesion
on grupo_sesion.Sesion_id = a.Sesion_id

group by grupo_sesion.grupo_id, a.Socio_id
) socio_asistencias
on socio_asistencias.grupo_id_asistencias = socio_grupo.Grupo_id
and socio_asistencias.socio_id_asistencias = s.socio_id


left join
(
select 

ga.Socio_id  as socio_id_ganancias,
grupo_sesion.Grupo_id as grupo_id_ganancias,
sum(ga.monto_ganancia) as ganancias_totales,
sum(case when Entregada =1 then ga.monto_ganancia else 0 end) as ganancias_recibidas


from railway.ganancias ga

inner join grupo_sesion
on grupo_sesion.Sesion_id = ga.Sesion_id

group by grupo_sesion.grupo_id, ga.Socio_id
) socio_ganancias
on socio_ganancias.grupo_id_ganancias = socio_grupo.Grupo_id
and socio_ganancias.socio_id_ganancias = s.socio_id


left join
(
select 

t.Socio_id  as socio_id_transacciones,
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

group by grupo_sesion.grupo_id, t.socio_id
) socio_transacciones
on socio_transacciones.grupo_id_transacciones = socio_grupo.Grupo_id
and socio_transacciones.socio_id_transacciones = s.socio_id


group by socio_grupo.grupo_id, socio_grupo.socio_id


union

select * from old_socios_stats_dashboard ossd 