
select 

t.*,
DATE_FORMAT(t.timestamp,'%H:%i:%s') hora_transaccion,
ct.tipo,
ct.orden_en_sesion,
g.nombre_grupo,
g.grupo_id,
rank() OVER ( partition by s.sesion_id order by t.transaccion_id) AS num_transaccion,
s.Fecha,
s.tipo_sesion,
so.nombres,
so.apellidos

from cosecha.transacciones t

inner join socios so
on so.socio_id = t.socio_id

inner join sesiones s
on t.sesion_id = s.sesion_id

inner join grupos g
on g.grupo_id = s.grupo_id

inner join catalogo_transacciones ct
on ct.catalogo_id = t.catalogo_id

where g.datos_dashboard = 1