
WITH grupo_sesion AS
  (
select 
g.Grupo_id,
s.sesion_id
from railway.grupos g 

inner join railway.sesiones s
on s.grupo_id = g.grupo_id

where g.status = 1 
)

select 

g.*,
sum(gs.acciones) as total_acciones,
grupo_prestamos.*,
grupo_prestamos.sum_monto_prestamo / sum(gs.acciones) as prestamo_vs_ahorro,
grupo_multas.*,
grupo_prestamos.sum_interes_pagado as ganancias_interes,
grupo_prestamos.sum_interes_pagado + grupo_multas.sum_monto_multa_pagadas as ganancias_totales,
grupo_asistencias.*,
grupo_socio.*

from railway.grupos g

inner join railway.grupo_socio gs 
on gs.Grupo_id  = g.Grupo_id

left join
(
select
grupo_sesion.Grupo_id as grupo_id_prestamos,
sum(p.monto_prestamo) as sum_monto_prestamo,
sum(p.monto_pagado) as sum_monto_pagado,
sum(p.interes_generado) as sum_interes_generado,
sum(p.interes_pagado) as sum_interes_pagado,
sum(p.estatus_ampliacion) as num_prestamos_ampliacion,
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

inner join
(
select 

gs.Grupo_id as grupo_id_socio,
count(gs.grupo_socio_id) as num_socios,
sum(case when gs.status=1 then 1 else 0 end) as num_socios_activos,
sum(case when gs.status=2 then 1 else 0 end) as num_socios_congelados,
sum(case when gs.status=0 then 1 else 0 end) as num_socios_inactivos,
sum(case when gs.status=1 then 1 else 0 end)/count(gs.grupo_socio_id) as tasa_socios_activos,
sum(case when gs.status=2 then 1 else 0 end)/count(gs.grupo_socio_id) as tasa_socios_congelados,
sum(case when gs.status=0 then 1 else 0 end)/count(gs.grupo_socio_id) as tasa_socios_inactivos


from railway.grupo_socio gs
group by gs.grupo_id
) grupo_socio
on grupo_socio.grupo_id_socio = g.grupo_id



where g.Status = 1

group by g.Grupo_id
