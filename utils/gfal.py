import os
import json
import subprocess

def get_file_size(file_path):
    """
    Returns the size of the file in bytes.
    """
    results = subprocess.run(['gfal-stat', file_path], capture_output=True, text=True, check=True)
    stats = results.stdout.splitlines()
    size = stats[1].split()[1]
    return int(size)


def get_file_list(dir):
    """
    Returns a list of files in the directory.
    """    
    results = subprocess.run(['gfal-ls', dir], capture_output=True, text=True, check=True)
    file_list = results.stdout.splitlines()
    return file_list


def get_file_sizes(dir):
    """
    Returns a list of file sizes for all files in the directory (in bytes).
    """
    results = subprocess.run(['gfal-ls', '-l', dir], capture_output=True, text=True, check=True)
    ls = results.stdout.splitlines()
    file_sizes = {}
    for line in ls:
        filename = line.split()[-1]
        size = int(line.split()[4])
        file_sizes[filename] = size
    return file_sizes


class Submission:
    def __init__(self, year, json_filename):        
        with open(f'jsons/{year}/{json_filename}', 'r') as f:
            self.json_dict = json.load(f)['files']
        first_source = self.json_dict[0]['sources'][0]
        first_destination = self.json_dict[0]['destinations'][0]
        
        self.year = year
        self.json_filename = json_filename
        self.source_dir = os.path.dirname(first_source)
        self.target_dir = os.path.dirname(first_destination)
        self.file_list = get_file_list(self.target_dir)
        self.file_count = len(self.file_list)
        self.expected_file_count = len(self.json_dict)

    def file_count_check(self):   
        if self.file_count == self.expected_file_count:
            return True
        else:
            print(
                  '\033[1;91m' +
                  f'Expected {self.expected_file_count} files, but ' +
                  f'found {self.file_count} files in {self.target_dir}' +
                  '\033[0m'
                 )
            for i in range(len(self.json_dict)):
                expected_file = self.json_dict[i]['destinations'][0].split('/')[-1]
                if expected_file not in self.file_list:
                    print('\033[93m' + f"Missing file: {expected_file}" + '\033[0m')
                    source_file = self.json_dict[i]['sources'][0]
                    print('\033[93m' + f"Source file: {source_file}" + '\033[0m')
            return False

    def gfal_stat_file_size(self, i):
        """
        Prints the size of the i^th source and destination file in bytes.
        Really slow - deprecated for now, but could be useful for debugging in the future.
        """
        source_size = get_file_size(self.json_dict[i]['sources'][0])
        destination_size = get_file_size(self.json_dict[i]['destinations'][0])

        print('Size of ' + self.json_dict[i]['sources'][0] + ':')
        print(f'\033[1;94m{source_size} bytes\033[0m\n')
        print('Size of ' + self.json_dict[i]['destinations'][0] + ':')
        print(f'\033[1;94m{destination_size} bytes\033[0m\n')

    def destination_file_sizes(self):
        """
        Returns a dictionary of file sizes for all files in the target directory (in bytes).
        """
        return get_file_sizes(self.target_dir)


def tests():
    test_sub = Submission('Run3_2024', 'DYto2Mu_MLL_50_1J_amcatnloFXFX.json')
    print('All files in directory:', test_sub.file_count_check())
    print('Expected file count:', test_sub.expected_file_count)
    print('Actual file count:', test_sub.file_count)
    print(len(get_file_sizes(test_sub.target_dir)))


if __name__ == "__main__":
    tests()