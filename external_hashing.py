import os
import struct
import time
import tempfile

from heap_file import read_page, write_page, count_pages, DEPT_EMPLOYEE_FORMAT, PAGE_HEADER_SIZE

def get_group_key_index(group_key: str) -> int:
    if group_key == "from_date":
        return 2 
    return 0

def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> list[str]:
    record_format = DEPT_EMPLOYEE_FORMAT
    rec_size = struct.calcsize(record_format)
    rpp = (page_size - PAGE_HEADER_SIZE) // rec_size
    
    B = buffer_size // page_size
    if B < 2:
        raise ValueError("BUFFER_SIZE insuficiente (Minimo 2)")
        
    k = B - 1  
    total_pages = count_pages(heap_path, page_size)
    
    tmp_dir = tempfile.mkdtemp()
    partition_paths = [os.path.join(tmp_dir, f"part_{i}.bin") for i in range(k)]
    
    for p in partition_paths:
        open(p, "wb").close()
        
    part_buffers = [[] for _ in range(k)]
    part_page_counts = [0] * k
    key_idx = get_group_key_index(group_key)
    
    for page_id in range(total_pages):
        records = read_page(heap_path, page_id, page_size, record_format)
        for rec in records:
            val = rec[key_idx]
            part_idx = hash(val) % k
            part_buffers[part_idx].append(rec)
            
            if len(part_buffers[part_idx]) == rpp:
                write_page(partition_paths[part_idx], part_page_counts[part_idx], 
                           part_buffers[part_idx], record_format, page_size)
                part_page_counts[part_idx] += 1
                part_buffers[part_idx].clear()
                
    for i in range(k):
        if part_buffers[i]:
            write_page(partition_paths[i], part_page_counts[i], 
                       part_buffers[i], record_format, page_size)
            
    return partition_paths

def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int, group_key: str) -> dict:
    record_format = DEPT_EMPLOYEE_FORMAT
    final_result = {}
    key_idx = get_group_key_index(group_key)
    
    for path in partition_paths:
        part_pages = count_pages(path, page_size)
        part_dict = {}
        
        for page_id in range(part_pages):
            records = read_page(path, page_id, page_size, record_format)
            for rec in records:
                val = rec[key_idx]
                part_dict[val] = part_dict.get(val, 0) + 1
                
        for k_val, count in part_dict.items():
            final_result[k_val] = final_result.get(k_val, 0) + count
            
    return final_result

def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> dict:
    t_start = time.time()
    
    t1_start = time.time()
    partitions = partition_data(heap_path, page_size, buffer_size, group_key)
    t1_end = time.time()
    
    t2_start = time.time()
    result_dict = aggregate_partitions(partitions, page_size, buffer_size, group_key)
    t2_end = time.time()
    
    original_pages = count_pages(heap_path, page_size)
    partition_pages = sum(count_pages(p, page_size) for p in partitions)
    
    return {
        'result': result_dict,
        'partitions_created': len(partitions),
        'pages_read': original_pages + partition_pages,
        'pages_written': partition_pages,
        'time_phase1_sec': round(t1_end - t1_start, 4),
        'time_phase2_sec': round(t2_end - t2_start, 4),
        'time_total_sec': round(t2_end - t_start, 4)
    }