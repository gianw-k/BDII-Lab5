from external_sort import external_sort
from heap_file import read_page, count_pages, EMPLOYEE_FORMAT


def normalizar_fecha(fecha):
    return str(fecha)[0:4]+"-"+str(fecha)[4:6]+"-"+str(fecha)[6:8]

PAGE_SIZE = 4096
RAM = 64
BUFFER_SIZE = RAM * 1024 

ruta_bin_employee = 'employee.bin'
ruta_output_sorted = 'employee_sorted.bin'

print(f"Iniciando External Sort con RAM de {RAM} KB...")

metricas = external_sort(
    heap_path=ruta_bin_employee, 
    output_path=ruta_output_sorted, 
    page_size=PAGE_SIZE, 
    buffer_size=BUFFER_SIZE, 
    sort_key="hire_date"
)

print("\nMetricas resultantes:")
print(metricas)

print("\nVerificando los 3 primeros registros ordenados:")
primeros = read_page(ruta_output_sorted, 0, PAGE_SIZE, EMPLOYEE_FORMAT)[0:3]
for p in primeros:
    id_emp = p[0]
    fecha_int = p[5]
    fecha = normalizar_fecha(fecha_int)
    print(f"ID: {id_emp}, Fecha: {fecha}")

print("\nVerificando los últimos registros:")
total_paginas_ordenadas = count_pages(ruta_output_sorted, PAGE_SIZE)
ultima_pagina = total_paginas_ordenadas - 1

ultimos = read_page(ruta_output_sorted, ultima_pagina, PAGE_SIZE, EMPLOYEE_FORMAT)[-3:]
for p in ultimos:
    id_emp = p[0]
    fecha_int = p[5]
    fecha = normalizar_fecha(fecha_int)
    print(f"ID: {id_emp}, Fecha: {fecha}")