import os
import prettytable
import yaml
import argparse
import subprocess
from utils.gfal import Submission
from utils.das import DASQuery
from create_jsons import convert_bytes

def check_progress(args):
    with open(f'submissions/{args.year}.yaml', 'r') as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)

    R = "\033[0;31;40m" #RED
    G = "\033[0;32;40m" # GREEN
    Y = "\033[0;33;40m" # Yellow
    B = "\033[0;34;40m" # Blue
    N = "\033[0m" # Reset

    table = prettytable.PrettyTable()

    if args.year == 'Run3_2024':
        table.field_names = ['Sample', 'Files', 'Transferred']
        for json_filename, value in yaml_dict.items():
            sample_name = json_filename.replace('.json', '')
            print(B + f'Checking {sample_name}...' + N)
            sub = Submission(args.year, json_filename)
            das_query = DASQuery(sample_name)
            
            if sub.file_count_check(): # check number of files in target directory 
                source_sizes = das_query.get_file_sizes() # get file sizes from source with DAS
                total_source_size = convert_bytes(sum(source_sizes.values()))
                destination_sizes = sub.destination_file_sizes() # get file sizes from target with gfal
                total_destination_size = convert_bytes(sum(destination_sizes.values()))
                mismatch = False

                for i in range(len(sub.json_dict)):
                    source_file = sub.json_dict[i]['sources'][0]
                    destination_file = sub.json_dict[i]['destinations'][0]
                    source_size = source_sizes[os.path.basename(source_file)]
                    destination_size = destination_sizes[os.path.basename(destination_file)]
                    if source_size == destination_size: # check if file sizes match
                        continue
                    else:
                        mismatch = True
                        print(
                            '\033[1;91m' +
                            f'File size mismatch for {destination_file}!' +
                            '\033[0m'
                        )
                        print(f'Source file: {source_file},\n size: {source_size} bytes')
                        print(f'Destination file: {destination_file},\n size: {destination_size} bytes\n')

                if mismatch:
                    status = Y
                else:
                    status = G
                    
            else: # if file count check fails, skip file size check and just report missing files
                status = R 

            table.add_row([
                           status + sample_name + N,
                           f'{sub.file_count}/{sub.expected_file_count}',
                           f'{total_destination_size} / {total_source_size}'
                          ], divider=True)

    else:
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
 