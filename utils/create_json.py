import subprocess
import json
from utils.das import DASQuery

REDIRECTORS = [
    "root://cmsxrootd.fnal.gov/",
    "root://xrootd-cms.infn.it/",
    "root://cms-xrd-global.cern.ch/",
]


def create_json(sample, args):
    # 2024 samples - query DAS for file list and use redirectors
    if args.year == "Run3_2024":  
        das_query = DASQuery(sample)
        dataset = das_query.dataset

        # Query DAS to get list of files
        lfns = das_query.get_file_list()
        if not lfns:
            raise RuntimeError(f"DAS returned no files for dataset {dataset}")

        # Try redirectors; accept the first that passes a quick probe on one PFN
        for xr in REDIRECTORS:
            cand = [xr + lfn.lstrip("/") for lfn in lfns]
            if (
                subprocess.run(
                    ["gfal-stat", cand[0]], capture_output=True
                ).returncode
                == 0
            ):
                files = cand
                break
        else:
            raise RuntimeError(
                f"None of the redirectors worked for {dataset}: {REDIRECTORS}"
            )
    
    # early Run 3 samples - just read from source directory
    else:  
        source_sample_path = f"{args.source_path}/{args.year}/{sample}"
        result = subprocess.run(
            ["gfal-ls", source_sample_path],
            capture_output=True,
            text=True,
            check=True,
        )
        files = result.stdout.splitlines()

    data = {"files": []}

    for file in files:
        if args.year == "Run3_2024":
            index = files.index(file)
            dest_name = f"nano_{index}.root"
            file_entry = {
                "sources": [file],
                "destinations": [
                    f"{args.destination_path}/{args.year}/{sample}/{dest_name}"
                ],
            }
        else:
            file_entry = {
                "sources": [f"{source_sample_path}/{file}"],
                "destinations": [
                    f"{args.destination_path}/{args.year}/{sample}/{file}"
                ],
            }
        data["files"].append(file_entry)

    with open(f"jsons/{args.year}/{sample}.json", "w") as f:
        json.dump(data, f, indent=4)
