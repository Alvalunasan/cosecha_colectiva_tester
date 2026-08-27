

select
p.*,
g.grupo_id,
g.nombre_grupo,
s.nombres,
s.apellidos,


case when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and a.Mod_calculo_interes = 1 and p.sesiones_restantes > 0 then round(p.monto_prestamo*a.Tasa_interes*2/100)/2
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and a.Mod_calculo_interes = 1 and p.sesiones_restantes <= 0 then round(p.monto_prestamo*(a.Tasa_interes + a.interes_morosidad)*2/100)/2
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and a.Mod_calculo_interes = 1 and p.sesiones_restantes > 0 then round(p.monto_prestamo*(a.Tasa_interes + a.interes_ampliacion)*2/100)/2
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and a.Mod_calculo_interes = 1 and p.sesiones_restantes <= 0 then round(p.monto_prestamo*(a.Tasa_interes+ + a.interes_ampliacion+a.interes_morosidad)*2/100)/2
    
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and a.Mod_calculo_interes = 0 and p.sesiones_restantes > 0 then  round((p.monto_prestamo-p.monto_pagado)*a.Tasa_interes*2/100)/2
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and a.Mod_calculo_interes = 0 and p.sesiones_restantes <= 0 then round((p.monto_prestamo-p.monto_pagado)*(a.Tasa_interes + a.interes_morosidad)*2/100)/2
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and a.Mod_calculo_interes = 0 and p.sesiones_restantes > 0 then  round((p.monto_prestamo-p.monto_pagado)*(a.Tasa_interes + a.interes_ampliacion)*2/100)/2
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and a.Mod_calculo_interes = 0 and p.sesiones_restantes <= 0 then round((p.monto_prestamo-p.monto_pagado)*(a.Tasa_interes+ + a.interes_ampliacion+a.interes_morosidad)*2/100)/2
     else 0 end
     as interes_futuro,
 

case when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and p.sesiones_restantes > 0 then 'VIGENTE'
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and p.sesiones_restantes <= 0 then 'MOROSIDAD'
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and p.sesiones_restantes > 0 then 'AMPLIACIÓN'
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and p.sesiones_restantes <= 0 then 'MOROSIDAD Y AMPLIACIÓN'
     else 'SALDADO' end 
     as estatus_prestamo_letra,
     
 case when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and p.sesiones_restantes > 0 then a.Tasa_interes
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 0 and p.sesiones_restantes <= 0 then a.Tasa_interes + a.interes_morosidad
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and p.sesiones_restantes > 0 then a.Tasa_interes + a.interes_ampliacion
     when p.estatus_prestamo = 0 and p.estatus_ampliacion = 1 and p.sesiones_restantes <= 0 then a.Tasa_interes+ + a.interes_ampliacion+a.interes_morosidad
     else 0 end 
     as tasa_interes_actual,    
     
a.Tasa_interes as tasa_interes_original




from railway.prestamos p 


inner join socios s
on s.socio_id = p.socio_id

inner join acuerdos a
on a.acuerdo_id = p.acuerdos_id

inner join grupos g
on a.grupo_id = g.grupo_id
