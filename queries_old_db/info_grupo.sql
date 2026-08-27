
select * from grupo_socio gs 


left join socios s 
on s.socio_id = gs.socio_id

left join preguntas_socios ps 
on ps.Socio_id = gs.socio_id

left join preguntas_seguridad pse 
on pse.Preguntas_seguridad_id = ps.pregunta_id


where gs.grupo_id = 2639