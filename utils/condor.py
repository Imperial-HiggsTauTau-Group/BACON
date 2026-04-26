import os
import json
import subprocess


def prepare_submission(directory, filename, mappings_slice):
    executable = f"{directory}/{filename}.sh"

    with open(executable, "w") as f:
        for mapping in mappings_slice:
            if (
                len(mapping["sources"]) == 1
                and len(mapping["destinations"]) == 1
            ):
                f.write(
                    f"gfal-copy -v -p {mapping['sources'][0]}"
                    + f" {mapping['destinations'][0]}\n\n"
                )
            else:
                print(
                    "\033[91mError: More than one source or destination"
                    + f" found for mapping: {mapping}\033[0m"
                )

    with open(f"{directory}/{filename}.sub", "w") as f:
        f.write(
            f"""
executable = {executable}
getenv = True

output = {directory}/{filename}.out
error = {directory}/{filename}.err
log = {directory}/{filename}.log

request_cpus = 1
request_memory = 1024M

+MaxRuntime = 3600
queue
"""
        )

    subprocess.run(["chmod", "+x", executable], check=True)


def make_submission(directory, filename):
    subprocess.run(
        ["condor_submit", f"{directory}/{filename}.sub"], check=True
    )


def tests():
    with open("condor/test/test.json", "r") as f:
        mappings = json.load(f)["files"]

    prepare_submission("condor/test", "test", mappings[0:2])
    # make_submission("condor/test", "test")


if __name__ == "__main__":
    tests()
