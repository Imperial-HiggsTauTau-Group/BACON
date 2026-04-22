import yaml
import argparse
import subprocess
import json
import os
import concurrent.futures
from create_json_utils.create_json import create_json

def get_size(url):
    r = subprocess.run(['gfal-stat', url], capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"gfal-stat failed for {url}: {r.stderr.strip()}")
    for line in r.stdout.splitlines():
        if line.strip().startswith('Size:'):
            try:
                return int(line.split()[1])
            except Exception:
                break
    raise RuntimeError(f"Could not parse size from gfal-stat output for {url}")


def convert_bytes(n):
    units = ['B', 'kB', 'MB', 'GB', 'TB', 'PB']  # SI units (1000-based), TB = 10^12 bytes
    x = float(n)
    i = 0
    while x >= 1000 and i < len(units) - 1:
        x /= 1000.0
        i += 1
    return f'{x:.2f} {units[i]}'


def create_jsons(args):
    with open(f'samples/{args.year}.yaml') as f:
        this_years_samples = yaml.load(f, Loader=yaml.FullLoader)

    if args.specify_samples:
        with open('specify_samples.yaml') as f:
            specified_samples = yaml.load(f, Loader=yaml.FullLoader)['samples']
        samples = [s for s in this_years_samples if s in specified_samples]
    else:
        samples = this_years_samples

    subprocess.run(['gfal-mkdir', f'{args.destination_path}/{args.year}'])
    for sample in samples:
        subprocess.run(['gfal-mkdir', f'{args.destination_path}/{args.year}/{sample}'])

    for sample in samples:
        print(f'Creating json for {sample}...')
        create_json(sample, args)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create JSONs from YAMLs')
    parser.add_argument('--year', required=True, help='Year to process')
    parser.add_argument('--destination_path', required=False, help='Path to create directories on dcache', default='davs://gfe02.grid.hep.ph.ic.ac.uk:2880/pnfs/hep.ph.ic.ac.uk/data/cms/store/user/irandreo')  # For Higgs samples: davs://gfe02.grid.hep.ph.ic.ac.uk:2880/pnfs/hep.ph.ic.ac.uk/data/cms/store/user/irandreo/HiggsSamples/
    parser.add_argument('--source_path', required=False, help='Path to sample root files', default='davs://eoscms.cern.ch/eos/cms/store')  # For Higgs samples: /eos/cms/store/group/phys_higgs/HLepRare/skim_2025_v1/
    parser.add_argument('--get_size', action='store_true', help='Get total size of all unique source files across all jsons')
    parser.add_argument('--specify_samples', action='store_true', help='Specify samples to process in specify_samples.yaml')
    args = parser.parse_args()

    create_jsons(args)

    # Check if any jsons are empty    
    json_dir = f'jsons/{args.year}/'
    for json_file in os.listdir(json_dir):
        with open(os.path.join(json_dir, json_file)) as f:
            data = json.load(f)
            if len(data['files']) == 0:
                print(f'Warning: jsons/{args.year}/{json_file} is empty!')

    # Sum total size of all unique source files
    if args.get_size:
        unique_sources = set()
        total_size = 0
        for json_file in os.listdir(json_dir):
            with open(os.path.join(json_dir, json_file)) as f:
                data = json.load(f)
                for entry in data.get('files', []):
                    src = entry['sources'][0]
                    if src in unique_sources:
                        continue
                    unique_sources.add(src)
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as ex:
            futures = {ex.submit(get_size, src): src for src in unique_sources}
            for fut in concurrent.futures.as_completed(futures):
                src = futures[fut]
                size = fut.result()
                if size is None:
                    print(f'Warning: could not stat {src}')
                else:
                    total_size += size
                    print(f'Size of {src}: {size} bytes')
        print(f'Total size across all jsons: {total_size} bytes ({convert_bytes(total_size)})')
