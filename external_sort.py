import os
import struct
import heapq
import time
import tempfile

from heap_file import read_page, write_page, count_pages, EMPLOYEE_FORMAT, PAGE_HEADER_SIZE

def get_key_index(sort_key: str) -> int:
    if sort_key == "hire_date":
        return 5
    return 0 

def generate_runs(heap_path: str, page_size: int, buffer_size: int, sort_key: str) -> list[str]:
    record_format = EMPLOYEE_FORMAT
    rec_size = struct.calcsize(record_format)
    rpp = (page_size - PAGE_HEADER_SIZE) // rec_size
    
    B = buffer_size // page_size
    if B < 1:
        raise ValueError("BUFFER_SIZE insuficiente")
        
    total_pages = count_pages(heap_path, page_size)
    runs = []
    tmp_dir = tempfile.mkdtemp()
    
    page_id = 0
    run_id = 0
    key_idx = get_key_index(sort_key)
    
    while page_id < total_pages:
        records = []
        for _ in range(B):
            if page_id >= total_pages:
                break
            records.extend(read_page(heap_path, page_id, page_size, record_format))
            page_id += 1
            
        if not records:
            break
            
        records.sort(key=lambda r: r[key_idx])
        
        run_path = os.path.join(tmp_dir, f"run_{run_id}.bin")
        runs.append(run_path)
        run_id += 1
        
        open(run_path, "wb").close()
        
        pid = 0
        for i in range(0, len(records), rpp):
            chunk = records[i:i+rpp]
            write_page(run_path, pid, chunk, record_format, page_size)
            pid += 1
            
    return runs

def multiway_merge(run_paths: list[str], output_path: str, page_size: int, buffer_size: int, sort_key: str):
    record_format = EMPLOYEE_FORMAT
    rec_size = struct.calcsize(record_format)
    rpp = (page_size - PAGE_HEADER_SIZE) // rec_size
    key_idx = get_key_index(sort_key)
    
    class RunReader:
        def __init__(self, path):
            self.path = path
            self.page = 0
            self.buffer = []
            self.idx = 0
            self.total_pages = count_pages(path, page_size)
            
        def load(self):
            if self.page >= self.total_pages:
                return False
            self.buffer = read_page(self.path, self.page, page_size, record_format)
            self.idx = 0
            self.page += 1
            return True
            
        def next(self):
            if self.idx >= len(self.buffer):
                if not self.load():
                    return None
            val = self.buffer[self.idx]
            self.idx += 1
            return val

    readers = [RunReader(p) for p in run_paths]
    heap = []
    
    for i, r in enumerate(readers):
        rec = r.next()
        if rec:
            heapq.heappush(heap, (rec[key_idx], i, rec))
            
    open(output_path, "wb").close()
    out_buffer = []
    out_page = 0
    
    def flush():
        nonlocal out_page
        if out_buffer:
            write_page(output_path, out_page, out_buffer[:], record_format, page_size)
            out_page += 1
            out_buffer.clear()
            
    while heap:
        _, i, rec = heapq.heappop(heap)
        out_buffer.append(rec)
        
        if len(out_buffer) == rpp:
            flush()
            
        nxt = readers[i].next()
        if nxt:
            heapq.heappush(heap, (nxt[key_idx], i, nxt))
            
    flush()

def external_sort(heap_path: str, output_path: str, page_size: int, buffer_size: int, sort_key: str) -> dict:
    t_start = time.time()
    
    t1_start = time.time()
    runs = generate_runs(heap_path, page_size, buffer_size, sort_key)
    t1_end = time.time()
    
    t2_start = time.time()
    multiway_merge(runs, output_path, page_size, buffer_size, sort_key)
    t2_end = time.time()
    
    total_pages = count_pages(heap_path, page_size)
    
    return {
        'runs_generated': len(runs),
        'pages_read': total_pages * 2,
        'pages_written': total_pages * 2,
        'time_phase1_sec': round(t1_end - t1_start, 4),
        'time_phase2_sec': round(t2_end - t2_start, 4),
        'time_total_sec': round(t2_end - t_start, 4)
    }