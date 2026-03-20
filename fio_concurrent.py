import subprocess
import json
import time
import os
import sys
import threading
import queue
import shutil

class Config:
    fio_bin = "fio"
    runtime = "15s"
    size = "1G"
    base_dir = "/tmp/fio_generic_framework"
    ioengine = "libaio"

class FioJobRunner:
    def __init__(self, profile, results_queue):
        self.profile = profile
        self.results_queue = results_queue

    def run(self):
        job_name = self.profile['name']
        filename = os.path.join(Config.base_dir, f"{job_name}.bin")
        
        cmd = [
            Config.fio_bin,
            f"--name={job_name}",
            f"--filename={filename}",
            f"--rw={self.profile['rw']}",
            f"--bs={self.profile['bs']}",
            f"--iodepth={self.profile['iodepth']}",
            f"--size={Config.size}",
            f"--runtime={Config.runtime}",
            "--time_based",
            "--direct=1",
            f"--ioengine={Config.ioengine}",
            "--output-format=json",
            "--group_reporting"
        ]

        if 'rwmixread' in self.profile:
            cmd.append(f"--rwmixread={self.profile['rwmixread']}")

        print("[{}] Starting Job: {} ({})".format(
            time.strftime('%H:%M:%S'), job_name, self.profile['rw']))
        
        try:
            proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
            data = json.loads(proc.stdout)
            
            if not data.get('jobs'):
                print("[ERROR] Job '{}' returned no data.".format(job_name))
                return

            job_data = data['jobs'][0]
            
            self.results_queue.put({
                'name': job_name,
                'read_stats': job_data['read'],
                'write_stats': job_data['write']
            })
            print("[{}] Completed Job: {}".format(time.strftime('%H:%M:%S'), job_name))

        except subprocess.CalledProcessError as e:
            print("[ERROR] Job '{}' failed (Exit Code {}).".format(job_name, e.returncode))
        except json.JSONDecodeError:
            print("[ERROR] Job '{}' returned invalid JSON.".format(job_name))
        except Exception as e:
            print("[CRITICAL] Job '{}' crashed: {}".format(job_name, str(e)))

def safe_get_percentile(stat_block, key):
    if not isinstance(stat_block.get('clat_ns'), dict):
        return 0.0
    percentiles = stat_block['clat_ns'].get('percentile')
    if not isinstance(percentiles, dict):
        return 0.0
    return float(percentiles.get(key, 0.0))

def generate_system_report(results):
    print("\n" + "="*95)
    print("{:^95}".format("AGGREGATED I/O PERFORMANCE REPORT"))
    print("="*95)
    
    header = "{:<22} | {:>12} | {:>18} | {:>16} | {:>16}".format(
        "Job Profile", "Total IOPS", "Throughput (MiB/s)", "P95 Latency (us)", "P99 Latency (us)"
    )
    print(header)
    print("-" * len(header))

    total_system_iops = 0
    total_system_bw_mib = 0

    for res in results:
        r = res['read_stats']
        w = res['write_stats']
        
        cur_iops = r['iops'] + w['iops']
        cur_bw_mib = (r['bw'] + w['bw']) / 1024.0
        
        r_p95 = safe_get_percentile(r, '95.000000')
        w_p95 = safe_get_percentile(w, '95.000000')
        p95_us = max(r_p95, w_p95) / 1000.0
        
        r_p99 = safe_get_percentile(r, '99.000000')
        w_p99 = safe_get_percentile(w, '99.000000')
        p99_us = max(r_p99, w_p99) / 1000.0

        row_fmt = "{:<22} | {:>12.0f} | {:>18.2f} | {:>16.2f} | {:>16.2f}"
        print(row_fmt.format(res['name'], cur_iops, cur_bw_mib, p95_us, p99_us))
        
        total_system_iops += cur_iops
        total_system_bw_mib += cur_bw_mib

    print("-" * len(header))
    print("{:<22} | {:>12.0f} | {:>18.2f}".format("SYSTEM WIDE TOTAL", total_system_iops, total_system_bw_mib))
    print("="*95)

def main():
    if not shutil.which(Config.fio_bin):
        print("[FATAL] 'fio' binary not found in PATH.")
        sys.exit(1)

    if not os.path.exists(Config.base_dir):
        os.makedirs(Config.base_dir)

    profiles = [
        {
            "name": "Random_4k_Mixed",
            "rw": "randrw",
            "bs": "4k",
            "iodepth": 32,
            "rwmixread": 70
        },
        {
            "name": "Sequential_1M_Read",
            "rw": "read",
            "bs": "1M",
            "iodepth": 16
        }
    ]

    results_queue = queue.Queue()
    threads = []

    for p in profiles:
        runner = FioJobRunner(p, results_queue)
        t = threading.Thread(target=runner.run)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    final_results = []
    while not results_queue.empty():
        final_results.append(results_queue.get())
    
    if not final_results:
        print("\n[ERROR] No results collected.")
        sys.exit(1)

    final_results.sort(key=lambda x: x['name'])
    generate_system_report(final_results)

if __name__ == "__main__":
    main()