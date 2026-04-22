import subprocess
import argparse
import yaml


def submit_jsons(args):

    if args.year == 'Run3_2024':
        print('\033[4m\033[91mABORTED: fts-rest-transfer not supported for this year\033[0m')
        return

    results = subprocess.run(['ls', f'jsons/{args.year}'], capture_output=True, text=True, check=True)
    jsons = results.stdout.splitlines()

    if args.specify_samples:
        with open('specify_samples.yaml') as f:
            specified_samples = yaml.load(f, Loader=yaml.FullLoader)['samples']
        jsons_to_submit = []
        for specified_sample in specified_samples:
            if f'{specified_sample}.json' in jsons:
                jsons_to_submit.append(f'{specified_sample}.json')
            else:
                print(f"Warning: JSON for specified sample '{specified_sample}' not found in jsons/{args.year}")
        jsons = jsons_to_submit
            
    yaml_dict = {}
    for json_file in jsons:

        submission_results = subprocess.run(f'fts-rest-transfer-submit -s https://fts00.grid.hep.ph.ic.ac.uk:8446 -f jsons/{args.year}/{json_file}', shell=True, capture_output=True, text=True, check=True)
        print(submission_results.args)
        yaml_dict[json_file] = submission_results.stdout

    with open(f'submissions/{args.year}.yaml', 'w') as f:
        yaml.dump(yaml_dict, f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Submit Transfer')
    parser.add_argument('--year', required=True, help='Year to process')
    parser.add_argument('--specify_samples', action='store_true', help='Specify samples to process in specify_samples.yaml')
    args = parser.parse_args()

    submit_jsons(args)