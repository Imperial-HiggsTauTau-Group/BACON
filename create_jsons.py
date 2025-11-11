import yaml
import argparse
import subprocess
import json
import os
import concurrent.futures

DASGOCLIENT = "/cvmfs/cms.cern.ch/common/dasgoclient"

REDIRECTORS = [
    "root://cmsxrootd.fnal.gov/",
    "root://xrootd-cms.infn.it/",
    "root://cms-xrd-global.cern.ch/",
]

sample_dir_2024 = {
    'Tau_Run2024C': ('Run2024C', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024D': ('Run2024D', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024E': ('Run2024E', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024F': ('Run2024F', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024G': ('Run2024G', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024H': ('Run2024H', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024I_v1': ('Run2024I', 'Tau', 'MINIv6NANOv15-v1'),
    'Tau_Run2024I_v2': ('Run2024I', 'Tau', 'MINIv6NANOv15_v2-v1'),

    'EGamma0_Run2024C': ('Run2024C', 'EGamma0', 'MINIv6NANOv15-v1'),
    'EGamma0_Run2024D': ('Run2024D', 'EGamma0', 'MINIv6NANOv15-v1'),
    'EGamma0_Run2024E': ('Run2024E', 'EGamma0', 'MINIv6NANOv15-v1'),
    'EGamma0_Run2024F': ('Run2024F', 'EGamma0', 'MINIv6NANOv15-v1'),
    'EGamma0_Run2024G': ('Run2024G', 'EGamma0', 'MINIv6NANOv15-v2'),
    'EGamma0_Run2024H': ('Run2024H', 'EGamma0', 'MINIv6NANOv15-v2'),
    'EGamma0_Run2024I_v1': ('Run2024I', 'EGamma0', 'MINIv6NANOv15-v1'),
    'EGamma0_Run2024I_v2': ('Run2024I', 'EGamma0', 'MINIv6NANOv15_v2-v1'),

    'EGamma1_Run2024C': ('Run2024C', 'EGamma1', 'MINIv6NANOv15-v1'),
    'EGamma1_Run2024D': ('Run2024D', 'EGamma1', 'MINIv6NANOv15-v1'),
    'EGamma1_Run2024E': ('Run2024E', 'EGamma1', 'MINIv6NANOv15-v1'),
    'EGamma1_Run2024F': ('Run2024F', 'EGamma1', 'MINIv6NANOv15-v1'),
    'EGamma1_Run2024G': ('Run2024G', 'EGamma1', 'MINIv6NANOv15-v2'),
    'EGamma1_Run2024H': ('Run2024H', 'EGamma1', 'MINIv6NANOv15-v1'),
    'EGamma1_Run2024I_v1': ('Run2024I', 'EGamma1', 'MINIv6NANOv15-v1'),
    'EGamma1_Run2024I_v2': ('Run2024I', 'EGamma1', 'MINIv6NANOv15_v2-v1'),

    'Muon0_Run2024C': ('Run2024C', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024D': ('Run2024D', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024E': ('Run2024E', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024F': ('Run2024F', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024G': ('Run2024G', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024H': ('Run2024H', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024I_v1': ('Run2024I', 'Muon0', 'MINIv6NANOv15-v1'),
    'Muon0_Run2024I_v2': ('Run2024I', 'Muon0', 'MINIv6NANOv15_v2-v1'),

    'Muon1_Run2024C': ('Run2024C', 'Muon1', 'MINIv6NANOv15-v1'),
    'Muon1_Run2024D': ('Run2024D', 'Muon1', 'MINIv6NANOv15-v1'),
    'Muon1_Run2024E': ('Run2024E', 'Muon1', 'MINIv6NANOv15-v1'),
    'Muon1_Run2024F': ('Run2024F', 'Muon1', 'MINIv6NANOv15-v1'),
    'Muon1_Run2024G': ('Run2024G', 'Muon1', 'MINIv6NANOv15-v2'),
    'Muon1_Run2024H': ('Run2024H', 'Muon1', 'MINIv6NANOv15-v2'),
    'Muon1_Run2024I_v1': ('Run2024I', 'Muon1', 'MINIv6NANOv15-v1'),
    'Muon1_Run2024I_v2': ('Run2024I', 'Muon1', 'MINIv6NANOv15_v2-v1'),

    'MuonEG_Run2024C': ('Run2024C', 'MuonEG', 'MINIv6NANOv15-v1'),
    'MuonEG_Run2024D': ('Run2024D', 'MuonEG', 'MINIv6NANOv15-v1'),
    'MuonEG_Run2024E': ('Run2024E', 'MuonEG', 'MINIv6NANOv15-v1'),
    'MuonEG_Run2024F': ('Run2024F', 'MuonEG', 'MINIv6NANOv15-v2'),
    'MuonEG_Run2024G': ('Run2024G', 'MuonEG', 'MINIv6NANOv15-v3'),
    'MuonEG_Run2024H': ('Run2024H', 'MuonEG', 'MINIv6NANOv15-v2'),
    'MuonEG_Run2024I_v1': ('Run2024I', 'MuonEG', 'MINIv6NANOv15-v2'),
    'MuonEG_Run2024I_v2': ('Run2024I', 'MuonEG', 'MINIv6NANOv15_v2-v2'),

    'DYto2E_MLL_50_amcatnloFXFX': ('RunIII2024Summer24NanoAODv15', 'DYto2Mu-2Jets_Bin-MLL-50_TuneCP5_13p6TeV_amcatnloFXFX-pythia8', '150X_mcRun3_2024_realistic_v2-v6'),
    'DYto2Mu_MLL_50_amcatnloFXFX': ('RunIII2024Summer24NanoAODv15', 'DYto2E-2Jets_Bin-MLL-50_TuneCP5_13p6TeV_amcatnloFXFX-pythia8', '150X_mcRun3_2024_realistic_v2-v4'),
    'DYto2Tau_MLL_50_amcatnloFXFX': ('RunIII2024Summer24NanoAODv15', 'DYto2Tau-2Jets_Bin-MLL-50_TuneCP5_13p6TeV_amcatnloFXFX-pythia8','150X_mcRun3_2024_realistic_v2-v7'),

    
    'TTto2L2Nu': ('RunIII2024Summer24NanoAODv15', 'TTto2L2Nu_TuneCP5_13p6TeV_powheg-pythia8', '150X_mcRun3_2024_realistic_v2-v3' ),
    'TTtoLNu2Q': ('RunIII2024Summer24NanoAODv15', 'TTtoLNu2Q_TuneCP5_13p6TeV_powheg-pythia8', '150X_mcRun3_2024_realistic_v2-v2'),
    'TTto4Q': ('RunIII2024Summer24NanoAODv15', 'TTto4Q_TuneCP5_13p6TeV_powheg-pythia8', '150X_mcRun3_2024_realistic_v2-v2' ),
    
    'WtoENu_madgraphMLM': ('RunIII2024Summer24NanoAODv15', 'WtoENu-4Jets_TuneCP5_13p6TeV_madgraphMLM-pythia8', '150X_mcRun3_2024_realistic_v2-v2'),
    'WtoMuNu_madgraphMLM': ('RunIII2024Summer24NanoAODv15', 'WtoMuNu-4Jets_TuneCP5_13p6TeV_madgraphMLM-pythia8', '150X_mcRun3_2024_realistic_v2-v2'),
    'WtoTauNu_madgraphMLM': ('RunIII2024Summer24NanoAODv15', 'WtoTauNu-4Jets_TuneCP5_13p6TeV_madgraphMLM-pythia8', '150X_mcRun3_2024_realistic_v2-v2'),
}


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
        samples = yaml.load(f, Loader=yaml.FullLoader)

    subprocess.run(['gfal-mkdir', f'{args.destination_path}/{args.year}'])
    for sample in samples:
        subprocess.run(['gfal-mkdir', f'{args.destination_path}/{args.year}/{sample}'])

    for sample in samples:
        if args.year == 'Run3_2024' and  sample.startswith(('Tau', 'EGamma', 'Muon', 'MuonEG')):
            era, primary, tag = sample_dir_2024[sample]
            dataset = f'/{primary}/{era}-{tag}/NANOAOD'
        elif args.year == 'Run3_2024' :
            era, process, tag = sample_dir_2024[sample]
            dataset = f'/{process}/{era}-{tag}/NANOAODSIM' 
        else:
            source_sample_path = f'{args.source_path}/{args.year}/{sample}'

        if args.year == 'Run3_2024':
            # Query DAS to get list of files
            das_query = f'{DASGOCLIENT} --query="file dataset={dataset}" --limit=0 --format=plain'
            result = subprocess.run(das_query, shell=True, capture_output=True, text=True, check=True)
            lfns = [line.strip() for line in result.stdout.splitlines() if line.strip()]
            if not lfns:
                raise RuntimeError(f"DAS returned no files for dataset {dataset}")

            # Try redirectors; accept the first that passes a quick probe on one PFN
            for xr in REDIRECTORS:
                cand = [xr + lfn.lstrip('/') for lfn in lfns]
                if subprocess.run(['gfal-stat', cand[0]], capture_output=True).returncode == 0:
                    files = cand
                    break
            else:
                raise RuntimeError(f"None of the redirectors worked for {dataset}: {REDIRECTORS}")
        else:
            result = subprocess.run(['gfal-ls', source_sample_path], capture_output=True, text=True, check=True)
            files = result.stdout.splitlines()

        data = {
            "files": []
        }

        for file in files:
            if args.year == 'Run3_2024':
                index = files.index(file)
                dest_name = f'nano_{index}.root'
                file_entry = {
                    "sources": [file],
                    "destinations": [f'{args.destination_path}/{args.year}/{sample}/{dest_name}']
                }
            else:
                file_entry = {
                "sources": [f'{source_sample_path}/{file}'],
                "destinations": [f'{args.destination_path}/{args.year}/{sample}/{file}']
            }
            data["files"].append(file_entry)

        
        with open(f'jsons/{args.year}/{sample}.json', 'w') as f:
            json.dump(data, f, indent=4)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create JSONs from YAMLs')
    parser.add_argument('--year', required=True, help='Year to process')
    parser.add_argument('--destination_path', required=False, help='Path to create directories on dcache', default='davs://gfe02.grid.hep.ph.ic.ac.uk:2880/pnfs/hep.ph.ic.ac.uk/data/cms/store/user/irandreo/')
    parser.add_argument('--source_path', required=False, help='Path to sample root files', default='davs://eoscms.cern.ch/eos/cms/store')
    parser.add_argument('--get_size', action='store_true', help='Get total size of all unique source files across all jsons')
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
