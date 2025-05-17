


with socio_sesion as
(
select
s.*,
gs.socio_id,
gs.acciones as socio_acciones,
gs.Status,
gs2.num_socios_grupo,
g.nombre_grupo,
gc.Color_grupo

from sesiones s


inner join grupo_socio gs 
on gs.Grupo_id = s.grupo_id


inner join 
(
select
gs1.grupo_id,
count(gs1.grupo_socio_id) as num_socios_grupo

from grupo_socio gs1 

group by gs1.grupo_id
) gs2
on gs2.Grupo_id = s.grupo_id

inner join grupos g 
on g.Grupo_id = gs.Grupo_id

left join grupos_colores gc 
on gc.Grupo_id = gs.Grupo_id


where g.datos_dashboard = 1 
order by gs.Grupo_id, s.Sesion_id, gs.socio_id
)



select 

ss.nombre_grupo,
rank() OVER ( partition by ss.grupo_id order by ss.sesion_id, ss.socio_id) AS num_sesion_socio,
CEIL((rank() OVER ( partition by ss.grupo_id order by ss.sesion_id, ss.socio_id))/ss.num_socios_grupo) AS num_sesion,
mod((rank() OVER ( partition by ss.grupo_id order by ss.sesion_id, ss.socio_id))-1,ss.num_socios_grupo)+1 as num_socio,
(rank() OVER ( partition by ss.grupo_id order by ss.sesion_id, ss.socio_id))+20000 AS Sesion_Socio_id,
ss.sesion_id AS Sesion_id,
ss.socio_id as Socio_id,
ss.Fecha,
ss.Activa,
ss.Caja,
sesion_socio_transacciones.sum_compra_acciones - sesion_socio_transacciones.sum_retiro_acciones AS Acciones_Sesion,
sesion_socio_ganancias.sum_ganancias AS Ganancias_Sesion,
ss.Fecha_prox_reunion,
ss.Lugar_prox_reunion,
ss.Tipo_sesion,
ss.Grupo_id,
ss.created_at,
sum(sesion_socio_transacciones.sum_compra_acciones) OVER ( partition by ss.grupo_id,ss.socio_id order by ss.sesion_id,ss.socio_id) 
- (ifnull(sesion_socio_transacciones.sum_compra_acciones,0) - ifnull(sesion_socio_transacciones.sum_retiro_acciones,0)) AS acciones_previas,

sum(sesion_socio_ganancias.sum_ganancias) OVER ( partition by ss.grupo_id,ss.socio_id order by ss.sesion_id,ss.socio_id)  AS gancias_acumuladas,
sum(sesion_socio_prestamos.sum_monto_prestamo) OVER ( partition by ss.grupo_id,ss.socio_id order by ss.sesion_id,ss.socio_id) AS prestamos_acum,
sum(sesion_socio_transacciones.sum_compra_acciones) OVER ( partition by ss.grupo_id,ss.socio_id order by ss.sesion_id,ss.socio_id) AS ahorro_acum,
sum(sesion_socio_transacciones.sum_retiro_acciones) OVER ( partition by ss.grupo_id,ss.socio_id order by ss.sesion_id,ss.socio_id) AS retiro_acum,
sum(sesion_socio_transacciones.sum_compra_acciones - sesion_socio_transacciones.sum_retiro_acciones) OVER ( partition by ss.grupo_id,ss.socio_id order by ss.sesion_id,ss.socio_id) AS acciones_actuales,

sesion_socio_transacciones.sum_entradas - sesion_socio_transacciones.sum_salidas AS entradas_menos_salidas_sesion,
sesion_socio_prestamos.*,
sesion_socio_multas.*,
sesion_socio_asistencias.*,
sesion_socio_transacciones.*,
sesion_socio_ganancias.*,
sesion_socio_interes_prestamo.*,
ss.Color_grupo as color_grupo

-- duracion_sesion_aprox
-- LAG( ss.caja, 1, 0 ) OVER ( partition by ss.grupo_id order by ss.sesion_id, ss.socio_id) AS caja_previa


from socio_sesion ss




left join
(
select

p.sesion_id as sesion_id_prestamos,
p.Socio_id  as socio_id_prestamos,
count(p.prestamo_id) as num_prestamos_sesion,
sum(p.monto_prestamo) as sum_monto_prestamo,
avg(p.monto_prestamo) as promedio_monto_prestamo,
sum(case when Prestamo_original_id is not null then 1 else 0 end) as ampliaciones


from railway.prestamos p 

group by sesion_id_prestamos, socio_id_prestamos
) sesion_socio_prestamos
on sesion_socio_prestamos.sesion_id_prestamos = ss.sesion_id
and sesion_socio_prestamos.socio_id_prestamos  = ss.socio_id


left join
(
select 

m.Sesion_id as sesion_id_multas,
m.socio_id as socio_id_multas,
sum(m.monto_multa) as sum_monto_multas,
count(m.multa_id) as num_multas


from railway.multas m

group by sesion_id_multas, socio_id_multas

) sesion_socio_multas
on sesion_socio_multas.sesion_id_multas = ss.sesion_id
and sesion_socio_multas.socio_id_multas = ss.socio_id


left join
(
select 

a.Sesion_id as sesion_id_asistencias,
a.socio_id as socio_id_asistencias,
case when a.presente>=1 then 1 else 0 end as socio_presente,
case when a.presente=2 then 1 else 0 end as socio_retardo

from railway.asistencias a


) sesion_socio_asistencias
on sesion_socio_asistencias.sesion_id_asistencias = ss.sesion_id
and sesion_socio_asistencias.socio_id_asistencias = ss.socio_id


left join
(
select 

t.Sesion_id  as sesion_id_transacciones,
t.socio_id  as socio_id_transacciones,
sum(case when catalogo_id = 'ABONO_PRESTAMO' then cantidad_movimiento else 0 end) as sum_abonos_prestamos,
sum(case when catalogo_id = 'ABONO_PRESTAMO' then 1 else 0 end) as count_abonos_prestamos,
sum(case when catalogo_id = 'COMPRA_ACCION' then cantidad_movimiento else 0 end) as sum_compra_acciones,
sum(case when catalogo_id = 'COMPRA_ACCION' then 1 else 0 end) as count_compra_acciones,
sum(case when catalogo_id = 'ENTREGA_PRESTAMO' then -1*cantidad_movimiento else 0 end) as sum_entrega_prestamos,
sum(case when catalogo_id = 'ENTREGA_PRESTAMO' then 1 else 0 end) as count_entrega_prestamos,
sum(case when catalogo_id = 'PAGO_MULTA' then cantidad_movimiento else 0 end) as sum_pago_multas,
sum(case when catalogo_id = 'PAGO_MULTA' then 1 else 0 end) as count_pago_multas,
sum(case when catalogo_id = 'RETIRO_ACCION' then -1*cantidad_movimiento else 0 end) as sum_retiro_acciones,
sum(case when catalogo_id = 'RETIRO_ACCION' then 1 else 0 end) as count_retiro_acciones,
sum(case when catalogo_id in ('ABONO_PRESTAMO', 'COMPRA_ACCION', 'PAGO_MULTA') then cantidad_movimiento else 0 end) as sum_entradas,
sum(case when catalogo_id in ('RETIRO_ACCION', 'ENTREGA_PRESTAMO') then -1*cantidad_movimiento else 0 end) as sum_salidas,
sum(tp.monto_abono_prestamo) as sum_monto_abono_prestamo,
sum(tp.monto_abono_interes) as sum_monto_abono_interes,
max(timestamp) as ultima_transaccion


from railway.transacciones t

left join railway.transaccion_prestamos tp
on tp.transaccion_id = t.transaccion_id

group by sesion_id_transacciones, socio_id_transacciones
) sesion_socio_transacciones
on sesion_socio_transacciones.sesion_id_transacciones = ss.Sesion_id
and sesion_socio_transacciones.socio_id_transacciones = ss.socio_id


left join
(
select

ga.sesion_id as sesion_id_ganancias,
ga.socio_id as socio_id_ganancias,
ga.Monto_ganancia as sum_ganancias,
case when ga.entregada = 1 then ga.Monto_ganancia else 0 end as sum_ganancias_entregadas,
1 as num_ganancias

from railway.ganancias ga


) sesion_socio_ganancias
on  sesion_socio_ganancias.sesion_id_ganancias = ss.sesion_id
and sesion_socio_ganancias.socio_id_ganancias = ss.socio_id



left join
(
select

ip.sesion_id as sesion_id_interes_prestamo,
p.socio_id as socio_id_interes_prestamo,
sum(ip.monto_interes) as sum_interes,
count(ip.Prestamo_id) as num_prestamos_activos,
sum(case when ip.Tipo_interes =0 then 1 else 0 end) as num_prestamos_vigentes,
sum(case when ip.Tipo_interes =1 then 1 else 0 end) as num_prestamos_morosos,
sum(case when ip.Tipo_interes =2 then 1 else 0 end) as num_prestamos_ampliados,
sum(case when ip.Tipo_interes =3 then 1 else 0 end) as num_prestamos_ampliados_morosos

from railway.interes_prestamo ip

inner join prestamos p 
on p.Prestamo_id = ip.Prestamo_id

group by sesion_id_interes_prestamo, socio_id_interes_prestamo
) sesion_socio_interes_prestamo
on  sesion_socio_interes_prestamo.sesion_id_interes_prestamo = ss.sesion_id
and  sesion_socio_interes_prestamo.socio_id_interes_prestamo = ss.socio_id



UNION 


(
select 

osssd.*,
gc.Color_grupo

from old_sesiones_socios_stats_dashboard  osssd

left join grupos_colores gc
on gc.grupo_id = osssd.grupo_id
)



order by grupo_id, num_sesion_socio
