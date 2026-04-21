import subprocess
import json

DASGOCLIENT = "/cvmfs/cms.cern.ch/common/dasgoclient"

REDIRECTORS = [
    "root://cmsxrootd.fnal.gov/",
    "root://xrootd-cms.infn.it/",
    "root://cms-xrd-global.cern.ch/",
]

with open('create_json_utils/sample_dir_2024.json', 'r') as f:
    sample_dir_2024 = json.load(f)

def create_json(sample, args):
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