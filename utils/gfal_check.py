import json
import subprocess

def dir_check(year, json_filename):
    with open(f'jsons/{year}/{json_filename}', 'r') as f:
        json_dict = json.load(f)['files']
    first_destination = json_dict[0]['destinations'][0] 
    target_dir = first_destination.split('/nano')[0]
    
    results = subprocess.run(['gfal-ls', target_dir], capture_output=True, text=True, check=True)
    file_list = results.stdout.split('\n')[:-1] # Remove the last empty element from the split
    if len(file_list) == len(json_dict):
        return True
    else:
        print('\033[91m' + f"Expected {len(json_dict)} files, but found {len(file_list)} files in {target_dir}" + '\033[0m')
        return False


def tests():
    print('All files in directory:', dir_check('Run3_2024', 'DYto2Mu_MLL_50_1J_amcatnloFXFX.json'))


if __name__ == "__main__":
    tests()