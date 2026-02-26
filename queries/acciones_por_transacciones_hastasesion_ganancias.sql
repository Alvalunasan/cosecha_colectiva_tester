

select * from ganancias g


left join
(

select 

socio_id,
sum(cantidad_movimiento) as acciones_socio from transacciones t


where (catalogo_id = 'COMPRA_ACCION' or catalogo_id = 'RETIRO_ACCION')
and sesion_id in (select sesion_id from sesiones where grupo_id = 2618)
and sesion_id <= 1000000

group by socio_id
) as acciones_socio_hasta_sesion 
on acciones_socio_hasta_sesion.socio_id = g.socio_id


where g.sesion_id = 20022
