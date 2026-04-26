import os
from heap_file import export_to_heap, read_page, write_page, count_pages, EMPLOYEE_FORMAT

PAGE_SIZE = 4096
csv_test = 'employee.csv'
test_bin = 'test.bin'

# 1. Función: export_to_heap
print("1. Probando export_to_heap...")
if os.path.exists(test_bin): os.remove(test_bin)
export_to_heap(csv_test, test_bin, EMPLOYEE_FORMAT, PAGE_SIZE)
print("Archivo .bin creado desde el CSV exitosamente.")

# 2. Función: count_pages
print("\n2. Probando count_pages...")
total_paginas = count_pages(test_bin, PAGE_SIZE)
print(f"El archivo tiene {total_paginas} paginas.")

# 3. Función: read_page
print("\n3. Probando read_page...")
registros = read_page(test_bin, 0, PAGE_SIZE, EMPLOYEE_FORMAT)
print(f"Se leyeron {len(registros)} registros de la página 0. Primer registro:")
print(f"{registros[0]}")

# 4. Función: write_page
print("\n4. Probando write_page...")
pagina_nueva = total_paginas
write_page(test_bin, pagina_nueva, registros, EMPLOYEE_FORMAT, PAGE_SIZE)
print(f"Se escribieron los registros en la página {pagina_nueva}.")
print(f"Nuevo total de páginas: {count_pages(test_bin, PAGE_SIZE)}")

if os.path.exists(test_bin): os.remove(test_bin)