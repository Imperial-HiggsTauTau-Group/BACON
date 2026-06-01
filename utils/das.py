import os
import json
import subprocess

DASGOCLIENT = "/cvmfs/cms.cern.ch/common/dasgoclient"

class DASQuery:
    def __init__(self, year, sample):

        with open(f"utils/sample_dirs/{year}.json", "r") as f:
            self.sample_dir = json.load(f)

        self.dataset = self.sample_dir[sample]

    def get_file_list(self):
        """
        Query DAS to get list of files
        """
        query = f'{DASGOCLIENT} --query="file dataset={self.dataset}" --limit=0 --format=plain'
        result = subprocess.run(
            query, shell=True, capture_output=True, text=True, check=True
        )
        lfns = [
            line.strip() for line in result.stdout.splitlines() if line.strip()
        ]
        return lfns

    def get_file_sizes(self):
        """
        Get the sizes of all files in the dataset.
        """
        file_sizes = {}
        query = f'{DASGOCLIENT} --query="file dataset={self.dataset} | grep file.name,file.size"'
        result = subprocess.run(
            query, shell=True, capture_output=True, text=True, check=True
        )
        lines = result.stdout.splitlines()

        for line in lines:
            filename = os.path.basename(line.split()[0])
            size = int(line.split()[1])
            file_sizes[filename] = size
        return file_sizes


def tests():
    test_sample = "DYto2Mu_MLL_50_1J_amcatnloFXFX"
    das_query = DASQuery("Run3_2024", test_sample)
    print(f"Dataset for {test_sample}: {das_query.dataset}")
    lfns = das_query.get_file_list()
    print(f"Number of files for {test_sample}: {len(lfns)}")
    print(f"First 2 files for {test_sample}:")
    for lfn in lfns[:2]:
        print(lfn)
    file_sizes = das_query.get_file_sizes()
    print(len(file_sizes))


if __name__ == "__main__":
    tests()

