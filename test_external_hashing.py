from external_hashing import external_hash_group_by

def normalizar_fecha(fecha):
    return str(fecha)[0:4]+"-"+str(fecha)[4:6]+"-"+str(fecha)[6:8]

PAGE_SIZE = 4096
RAM = 64
BUFFER_SIZE = RAM * 1024 

ruta_bin_dept = 'department_employee.bin'

print(f"Iniciando External Hashing con RAM de {RAM} KB...")

metricas = external_hash_group_by(
    heap_path=ruta_bin_dept, 
    page_size=PAGE_SIZE, 
    buffer_size=BUFFER_SIZE, 
    group_key="from_date"
)

resultado_agrupado = metricas.pop('result')

print("\nMetricas resultantes:")
print(metricas)

resultados_ordenados = sorted(resultado_agrupado.items(), key=lambda x: x[0])

print("\nVerificando los 3 primeros grupos (por fecha):")
primeros = resultados_ordenados[0:3]
for fecha_int, conteo in primeros:
    fecha = normalizar_fecha(fecha_int)
    print(f"Fecha: {fecha}, Total empleados asignados: {conteo}")

print("\nVerificando los ultimos grupos (por fecha):")
ultimos = resultados_ordenados[-3:]
for fecha_int, conteo in ultimos:
    fecha = normalizar_fecha(fecha_int)
    print(f"Fecha: {fecha}, Total empleados asignados: {conteo}")