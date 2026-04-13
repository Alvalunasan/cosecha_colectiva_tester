

select

gs.socio_id,
gs.acciones,
t2.acciones_transacciones,
gs.acciones - t2.acciones_transacciones as dif_base_acciones

from grupo_socio gs

inner join
(
select 

t.socio_id,
sum(cantidad_movimiento) as acciones_transacciones

from transacciones t


where sesion_id in (select sesion_id from sesiones where grupo_id = 2623) 
and (catalogo_id = 'COMPRA_ACCION' or catalogo_id = 'RETIRO_ACCION')

group by t.socio_id
) t2
on t2.socio_id = gs.socio_id


where grupo_id = 2623
