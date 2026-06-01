import os
import prettytable
import yaml
import argparse
import subprocess
from utils.gfal import Submission
from utils.das import DASQuery
from utils.create_json import early_run_3
from create_jsons import convert_bytes


def check_progress(args):
    with open(f"submissions/{args.year}.yaml", "r") as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)

    R = "\033[0;31;40m"  # RED
    G = "\033[0;32;40m"  # GREEN
    Y = "\033[0;33;40m"  # Yellow
    B = "\033[0;34;40m"  # Blue
    N = "\033[0m"  # Reset

    table = prettytable.PrettyTable()

    if args.year not in early_run_3:
        table.field_names = ["Sample", "Files", "Transferred"]
        for json_filename in yaml_dict.keys():
            print(B + f"Checking {json_filename}..." + N)
            sub = Submission(args.year, json_filename)
            das_query = DASQuery(args.year, sub.sample_name)

            # check number of files in target directory
            if sub.file_count_check():
                # get file sizes from source with DAS
                source_sizes = das_query.get_file_sizes()
                total_source_size = convert_bytes(sum(source_sizes.values()))
                # get file sizes from target with gfal
                destination_sizes = sub.destination_file_sizes()
                total_destination_size = convert_bytes(sum(destination_sizes.values()))
                mismatch = False

                # check if file sizes match for each file
                for i in range(sub.n_mappings):
                    source_file_path = sub.mappings[i]["sources"][0]
                    destination_file_path = sub.mappings[i]["destinations"][0]
                    source_file = os.path.basename(source_file_path)
                    destination_file = os.path.basename(destination_file_path)
                    source_size = source_sizes[source_file]
                    destination_size = destination_sizes[destination_file]

                    # check if file sizes match
                    if source_size == destination_size:
                        continue
                    else:
                        mismatch = True
                        print(
                            "\033[1;91m"
                            + f"File size mismatch for {destination_file}!"
                            + "\033[0m"
                        )
                        print(
                            f"Source file: {source_file},\n size: {source_size} bytes"
                        )
                        print(
                            f"Destination file: {destination_file},\n size: {destination_size} bytes\n"
                        )

                if mismatch:
                    status = Y
                else:
                    status = G
            # if file count check fails, skip file size check and just report missing files
            else:
                status = R

            table.add_row(
                [
                    status + sub.sample_name + N,
                    f"{sub.destination_file_count}/{sub.n_mappings}",
                    f"{total_destination_size} / {total_source_size}",
                ],
                divider=True,
            )

    # for Early Run 3 years, just check FTS job status
    else:
        table.field_names = ["File", "Status"]
        for key, value in yaml_dict.items():
            job_id = value.split("\n")[1].split(": ")[1]
            results = subprocess.run(
                [
                    "fts-rest-transfer-status",
                    "-s",
                    "https://fts00.grid.hep.ph.ic.ac.uk:8446",
                    job_id,
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            job_status = results.stdout.split("\n")[1].split(": ")[1]

            if job_status == "FINISHED":
                table.add_row([key, B + job_status + N], divider=True)
            else:
                table.add_row([key, R + job_status + N], divider=True)

    print(table)


def resubmit(args):
    if args.year in early_run_3:
        print(
            "\033[1;91mResubmission is not implemented for Early Run 3 years yet!\033[0m"
        )
        return
    
    with open(f"submissions/{args.year}.yaml", "r") as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
    
    for json_filename in yaml_dict.keys():
        sub = Submission(args.year, json_filename)
        if not sub.file_count_check():
            sub.resubmit_missing_files()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create JSONs from YAMLs")
    parser.add_argument("--year", required=True, help="Year to process")
    parser.add_argument("--resubmit", action="store_true", help="Whether to resubmit missing files")
    args = parser.parse_args()

    if args.resubmit:
        resubmit(args)
    else:
        check_progress(args)
