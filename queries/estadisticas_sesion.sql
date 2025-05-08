
select 

g.nombre_grupo,
rank() OVER ( partition by g.grupo_id order by s.sesion_id) AS num_sesion,
s.*,
case when TIMESTAMPDIFF(MINUTE, s.created_at, sesion_transacciones.ultima_transaccion) > 120 or  TIMESTAMPDIFF(MINUTE, s.created_at, sesion_transacciones.ultima_transaccion) < 4 then null else TIMESTAMPDIFF(MINUTE, s.created_at, sesion_transacciones.ultima_transaccion) end  as duracion_sesion_aprox, 
LAG( s.caja, 1, 0 ) OVER ( partition by g.grupo_id order by s.sesion_id) AS caja_previa,
LAG( s.acciones, 1, 0 ) OVER ( partition by g.grupo_id order by s.sesion_id) AS acciones_previas,
sum(s.ganancias) OVER ( partition by g.grupo_id order by s.sesion_id) AS gancias_acumuladas,

sum(sesion_prestamos.sum_monto_prestamo) OVER ( partition by g.grupo_id order by s.sesion_id) AS prestamos_acum,
sum(sesion_transacciones.sum_compra_acciones) OVER ( partition by g.grupo_id order by s.sesion_id) AS ahorro_acum,


s.caja - LAG( s.caja, 1, 0 ) OVER ( partition by g.grupo_id order by s.sesion_id) AS entradas_menos_salidas_sesion,
s.acciones - LAG( s.acciones, 1, 0 ) OVER ( partition by g.grupo_id order by s.sesion_id) AS acciones_sesion,

ifnull(sesion_transacciones.sum_pago_multas,0) + ifnull(sesion_transacciones.sum_monto_abono_interes,0) - ifnull(sesion_ganancias.sum_ganancias,0) as diff_ganancias_ga_vs_transaccion,
ifnull(s.ganancias,0) - ifnull(sesion_ganancias.sum_ganancias,0) as diff_ganancias_ga_vs_sesion,

sesion_prestamos.*,
sesion_multas.*,
sesion_asistencias.*,
sesion_transacciones.*,
sesion_ganancias.*,
sesion_interes_prestamo.*

from railway.sesiones s


left join
(
select

p.sesion_id as sesion_id_prestamos,
count(p.prestamo_id) as num_prestamos_sesion,
sum(p.monto_prestamo) as sum_monto_prestamo,
avg(p.monto_prestamo) as promedio_monto_prestamo,
sum(case when Prestamo_original_id is not null then 1 else 0 end) as ampliaciones


from railway.prestamos p 

group by sesion_id_prestamos
) sesion_prestamos
on sesion_prestamos.sesion_id_prestamos = s.sesion_id


left join
(
select 

m.Sesion_id as sesion_id_multas,
sum(m.monto_multa) as sum_monto_multas,
count(m.multa_id) as num_multas


from railway.multas m

group by sesion_id_multas

) sesion_multas
on sesion_multas.sesion_id_multas = s.sesion_id

left join
(
select 

a.Sesion_id as sesion_id_asistencias,
sum(case when a.presente>=1 then 1 else 0 end) as num_socios_presente,
sum(case when a.presente>=1 then 1 else 0 end) / count(a.asistencia_id) as tasa_asistencia,
sum(case when a.presente=2 then 1 else 0 end) / count(a.asistencia_id) as tasa_retardos


from railway.asistencias a

group by sesion_id_asistencias
) sesion_asistencias
on sesion_asistencias.sesion_id_asistencias = s.sesion_id


left join
(
select 

t.Sesion_id  as sesion_id_transacciones,
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

group by sesion_id_transacciones
) sesion_transacciones
on sesion_transacciones.sesion_id_transacciones = s.Sesion_id


left join
(
select

ga.sesion_id as sesion_id_ganancias,
sum(ga.Monto_ganancia) as sum_ganancias,
sum(case when ga.entregada = 1 then ga.Monto_ganancia else 0 end) as sum_ganancias_entregadas,
count(ga.ganancias_id) as num_ganancias

from railway.ganancias ga
group by sesion_id_ganancias

) sesion_ganancias
on  sesion_ganancias.sesion_id_ganancias = s.sesion_id

left join
(
select

ip.sesion_id as sesion_id_interes_prestamo,
sum(ip.monto_interes) as sum_interes,
count(ip.Prestamo_id) as num_prestamos_activos,
sum(case when ip.Tipo_interes =0 then 1 else 0 end) as num_prestamos_vigentes,
sum(case when ip.Tipo_interes =1 then 1 else 0 end) as num_prestamos_morosos,
sum(case when ip.Tipo_interes =2 then 1 else 0 end) as num_prestamos_ampliados,
sum(case when ip.Tipo_interes =3 then 1 else 0 end) as num_prestamos_ampliados_morosos

from railway.interes_prestamo ip

group by sesion_id_interes_prestamo
) sesion_interes_prestamo
on  sesion_interes_prestamo.sesion_id_interes_prestamo = s.sesion_id


inner join grupos g
on g.grupo_id = s.grupo_id

where g.datos_dashboard = 1



union 

select 

* from old_sesiones_stats_dashboard


order by grupo_id, sesion_id

