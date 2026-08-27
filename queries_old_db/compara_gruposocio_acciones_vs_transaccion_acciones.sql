

select * from grupo_socio gs

left join
(

select 

socio_id,
sum(cantidad_movimiento) as acciones_socio from transacciones t


where (catalogo_id = 'COMPRA_ACCION' or catalogo_id = 'RETIRO_ACCION')
and sesion_id in (select sesion_id from sesiones where grupo_id = 2641)
and sesion_id <= 1000000

group by socio_id
) as acciones_socio_hasta_sesion 
on acciones_socio_hasta_sesion.socio_id = gs.socio_id


where gs.grupo_id = 2641