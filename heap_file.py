import struct
import csv
import os
import math
from datetime import datetime


PAGE_HEADER_FMT = ">I"
PAGE_HEADER_SIZE = struct.calcsize(PAGE_HEADER_FMT)


EMPLOYEE_FORMAT = ">ii14s16s1si"
DEPT_EMPLOYEE_FORMAT = ">i4sii"


def date_to_int(s):
   return int(datetime.strptime(s, "%Y-%m-%d").strftime("%Y%m%d"))




def pack_employee(row):
   return struct.pack(
       EMPLOYEE_FORMAT,
       int(row[0]),
       date_to_int(row[1]),
       row[2].encode()[:14].ljust(14, b"\x00"),
       row[3].encode()[:16].ljust(16, b"\x00"),
       row[4].encode()[:1].ljust(1, b"\x00"),
       date_to_int(row[5])
   )




def pack_dept(row):
   return struct.pack(
       DEPT_EMPLOYEE_FORMAT,
       int(row[0]),
       row[1].encode()[:4].ljust(4, b"\x00"),
       date_to_int(row[2]),
       date_to_int(row[3])
   )






def export_to_heap(csv_path, heap_path, record_format, page_size):


   if record_format == EMPLOYEE_FORMAT:
       pack = pack_employee
   else:
       pack = pack_dept


   record_size = struct.calcsize(record_format)
   rpp = (page_size - PAGE_HEADER_SIZE) // record_size


   buffer = []
   pages = 0


   open(heap_path, "wb").close()


   def flush(buf):
       nonlocal pages


       header = struct.pack(PAGE_HEADER_FMT, len(buf))
       content = b"".join(buf)
       padding = bytes(page_size - PAGE_HEADER_SIZE - len(content))


       with open(heap_path, "ab") as f:
           f.write(header + content + padding)


       pages += 1


   with open(csv_path, newline="", encoding="utf-8") as f:
       reader = csv.reader(f)


       for row in reader:
           buffer.append(pack(row))


           if len(buffer) == rpp:
               flush(buffer)
               buffer = []


       if buffer:
           flush(buffer)


   return pages






def read_page(heap_path, page_id, page_size, record_format):


   offset = page_id * page_size


   if offset >= os.path.getsize(heap_path):
       return []


   rec_size = struct.calcsize(record_format)


   with open(heap_path, "rb") as f:
       f.seek(offset)
       data = f.read(page_size)


   (n,) = struct.unpack(PAGE_HEADER_FMT, data[:PAGE_HEADER_SIZE])


   records = []
   pos = PAGE_HEADER_SIZE


   for _ in range(n):
       records.append(struct.unpack(record_format, data[pos:pos+rec_size]))
       pos += rec_size


   return records






def write_page(heap_path, page_id, records, record_format, page_size):


   content = b"".join(struct.pack(record_format, *r) for r in records)
   header = struct.pack(PAGE_HEADER_FMT, len(records))
   padding = bytes(page_size - PAGE_HEADER_SIZE - len(content))


   page = header + content + padding


   mode = "r+b" if os.path.exists(heap_path) else "wb"


   with open(heap_path, mode) as f:
       f.seek(0, 2)
       size = f.tell()


       if size < page_id * page_size:
           f.write(bytes(page_id * page_size - size))


       f.seek(page_id * page_size)
       f.write(page)






def count_pages(heap_path, page_size):


   if not os.path.exists(heap_path):
       return 0  #porsiacaso


   return math.ceil(os.path.getsize(heap_path) / page_size)
