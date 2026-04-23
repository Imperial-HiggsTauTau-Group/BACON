import prettytable
import yaml
import argparse
import subprocess

def check_progress(args):
    with open(f'submissions/{args.year}.yaml', 'r') as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)

    R = "\033[0;31;40m" #RED
    G = "\033[0;32;40m" # GREEN
    Y = "\033[0;33;40m" # Yellow
    B = "\033[0;34;40m" # Blue
    N = "\033[0m" # Reset

    table = prettytable.PrettyTable()
    table.field_names = ['File', 'Status']

    for key, value in yaml_dict.items():
        job_id = value.split('\n')[1].split(': ')[1]
        results = subprocess.run(['fts-rest-transfer-status', '-s', 'https://fts00.grid.hep.ph.ic.ac.uk:8446', job_id], capture_output=True, text=True, check=True)
        job_status = results.stdout.split('\n')[1].split(': ')[1]

        if job_status == 'FINISHED':
            table.add_row([key, B+job_status+N], divider=True)
        else:
            table.add_row([key, R+job_status+N], divider=True)

    print(table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create JSONs from YAMLs')
    parser.add_argument('--year', required=True, help='Year to process')
    args = parser.parse_args()

    check_progress(args) 