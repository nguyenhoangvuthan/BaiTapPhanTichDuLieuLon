"""Chạy lời giải bằng kernel của interpreter hiện tại; chỉ lưu đè khi thành công."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timezone
from time import perf_counter

import nbformat
from nbclient import NotebookClient

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS = {
    'sql': '02_Lab1_SparkSQL.ipynb',
    'ml': '03_Lab2_ML.ipynb',
    'aml': '04_Lab4_GianLan.ipynb',
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--only', choices=['all', 'mapreduce', *NOTEBOOKS], default='all')
    parser.add_argument('--skip-tests', action='store_true')
    args = parser.parse_args()
    os.chdir(ROOT)
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    os.environ.setdefault('SPARK_LOCAL_DIRS', str(Path.home() / '.cache/bigdata-labs/spark'))
    for directory in os.environ['SPARK_LOCAL_DIRS'].split(','):
        Path(directory).mkdir(parents=True, exist_ok=True)
    work = ROOT / '.work' / 'runs'
    work.mkdir(parents=True, exist_ok=True)
    records = []

    def command(label, command_args):
        print(f'>>> {label}', flush=True)
        start = perf_counter()
        result = subprocess.run(command_args, cwd=ROOT, text=True, encoding='utf-8',
                                errors='replace', stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (work / f'{label}.log').write_text(result.stdout, encoding='utf-8')
        print(result.stdout[-6000:], flush=True)
        records.append({'task': label, 'ok': result.returncode == 0,
                        'seconds': round(perf_counter() - start, 2)})
        result.check_returncode()

    try:
        if args.only in ('all', 'mapreduce'):
            command('mapreduce', [sys.executable, 'loi_giai/mapreduce.py'])
        if not args.skip_tests:
            command('tests', [sys.executable, '-m', 'unittest', 'discover', '-s', 'tests', '-v'])
        if args.only != 'mapreduce':
            subprocess.run([sys.executable, '-m', 'ipykernel', 'install', '--user',
                            '--name', 'labs', '--display-name', 'Python (labs)',
                            '--env', 'JAVA_HOME', os.environ.get('JAVA_HOME', ''),
                            '--env', 'PYSPARK_PYTHON', sys.executable,
                            '--env', 'SPARK_LOCAL_IP', os.environ['SPARK_LOCAL_IP'],
                            '--env', 'SPARK_LOCAL_DIRS', os.environ['SPARK_LOCAL_DIRS']], check=True)
        for key, name in NOTEBOOKS.items():
            if args.only not in ('all', key):
                continue
            path = ROOT / 'loi_giai' / name
            nb = nbformat.read(path, as_version=4)
            for cell in nb.cells:
                if cell.cell_type == 'code':
                    cell.outputs = []
                    cell.execution_count = None
            start = perf_counter()
            print(f'>>> {name}', flush=True)

            def progress(cell, cell_index, **kwargs):
                if cell.cell_type == 'code':
                    print(f'  cell {cell_index}: {cell.source.splitlines()[0][:90]}', flush=True)

            client = NotebookClient(nb, timeout=3600, kernel_name='labs',
                                    resources={'metadata': {'path': str(ROOT)}},
                                    on_cell_start=progress)
            try:
                client.execute()
                nb.metadata['execution_run'] = {
                    'completed_utc': datetime.now(timezone.utc).isoformat(),
                    'python': sys.version.split()[0],
                    'ml_fraction': float(os.environ.get('ML_FRACTION', '1')),
                    'aml_fraction': float(os.environ.get('AML_FRACTION', '1')),
                }
                nbformat.validate(nb)
                nbformat.write(nb, path)
                records.append({'task': key, 'ok': True, 'seconds': round(perf_counter()-start, 2)})
                print(f'OK: {name} ({perf_counter()-start:.1f}s)', flush=True)
            except Exception:
                nbformat.write(nb, work / f'{path.stem}.failed.ipynb')
                records.append({'task': key, 'ok': False, 'seconds': round(perf_counter()-start, 2)})
                raise
    finally:
        (work / f'result-{args.only}.json').write_text(
            json.dumps(records, ensure_ascii=False, indent=2), encoding='utf-8')


if __name__ == '__main__':
    main()
