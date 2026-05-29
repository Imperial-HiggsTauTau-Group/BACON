import subprocess
import json
import yaml
import argparse
from utils.das import DASGOCLIENT


def process_duplicates(sample_names):
    tracker = {}
    for i, name in enumerate(sample_names):
        if name not in tracker:
            tracker[name] = [i]
        else:
            tracker[name].append(i)

    processed_names = sample_names.copy()
    for name, indices in tracker.items():
        if len(indices) > 1:
            for j, idx in enumerate(indices):
                processed_names[idx] = f"{name}_v{j+1}"
    
    return processed_names


def build_dict(datasets):
    sample_names = []
    sample_dirs = {}
    
    for dataset in datasets:
        primary, era_and_tag, data_tier = dataset[1:].split('/')
        era = era_and_tag.split('-')[0]
        sample_name = f"{primary}_{era}"
        sample_names.append(sample_name)
    
    sample_names = process_duplicates(sample_names)
    for sample_name, dataset in zip(sample_names, datasets):
        sample_dirs[sample_name] = dataset

    return sample_dirs


def add_to_json(sample_dirs, filename):
    try:
        with open(filename, "r") as f:
            existing_data = json.load(f)
    except FileNotFoundError:
        existing_data = {}
    
    existing_data.update(sample_dirs)
    with open(filename, "w") as f:
        json.dump(existing_data, f, indent=4)

    return list(existing_data.keys())


def main(args):
    result = subprocess.run(
        f'{DASGOCLIENT} --query={args.query}',
        shell=True,
        capture_output=True,
        text=True,
        check=True,
    )
    datasets = result.stdout.splitlines()
    sample_dirs = build_dict(datasets)

    sample_names = add_to_json(
        sample_dirs,
        f"utils/sample_dirs/{args.era}.json"
    )
    
    with open(f"samples/{args.era}.yaml", "w") as f:
        yaml.dump(sample_names, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Query DAS for sample information"
    )
    parser.add_argument(
        "--era",
        type=str,
        required=True,
        help="Era of the samples, e.g. Run3_2025"
    )
    parser.add_argument(
        "--query",
        type=str,
        required=True,
        help='DAS query string, e.g. "Tau/Run2025*PromptReco*/NANOAOD"',
    )
    args = parser.parse_args()
    
    main(args)

