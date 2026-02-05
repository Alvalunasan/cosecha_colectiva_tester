



select 

s2.Nombres,
s2.Apellidos,
s2.Nombre_grupo,
p.interes_generado - ip2.total_interes as dif_interes,
abs(p.interes_generado - ip2.total_interes) as abs_dif_interes,
ip2.total_interes,
p.*



from prestamos p

left join (

select
ip.prestamo_id,
sum(ip.monto_interes) as total_interes

from interes_prestamo ip
group by ip.prestamo_id
) ip2
on ip2.prestamo_id = p.prestamo_id

inner join

(
select

s.socio_id,
s.nombres,
s.apellidos,
g.nombre_grupo,
g.grupo_id

from socios s

inner join grupo_socio gs 
on gs.Socio_id = s.socio_id

inner join grupos g 
on gs.grupo_id = g.grupo_id

where g.datos_dashboard = 1

) s2
on s2.socio_id = p.socio_id

where p.prestamo_id > 2500
