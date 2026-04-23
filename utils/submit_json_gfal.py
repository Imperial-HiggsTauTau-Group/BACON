import os
import json
import subprocess

def prepare_submission(dir, filename, mappings_slice):
    executable = f'{dir}/{filename}.sh'

    with open(executable, 'w') as f:
        for mapping in mappings_slice:
            if len(mapping['sources']) == 1 and len(mapping['destinations']) == 1:                
                f.write(f'gfal-copy -p {mapping["sources"][0]} {mapping["destinations"][0]}\n\n')            
            else:                
                print('\033[91m' + f'Error: More than one source or destination found for mapping: {mapping}' + '\033[0m')

    with open(f'{dir}/{filename}.sub', 'w') as f:        
        f.write(
f"""      
executable = {executable}
getenv = True

output = {dir}/{filename}.out
error = {dir}/{filename}.err
log = {dir}/{filename}.log

request_cpus = 1
request_memory = 1024M

+MaxRuntime = 3600
queue
"""
                )
        
    subprocess.run(['chmod', '+x', executable], check=True)
        

def make_submission(dir, filename):   
    subprocess.run(['condor_submit', f'{dir}/{filename}.sub'], check=True)


def submit_json_gfal(year, json_file, chunk_size = 5):  
    sample_name = json_file.replace('.json', '')
    os.makedirs(f'condor/{year}/{sample_name}', exist_ok=True)
    json_filepath = f'jsons/{year}/{json_file}'
    print('\033[94m' + f'Submitting {json_file} using gfal-copy...' + '\033[0m')

    with open(json_filepath, 'r') as f:        
        mappings = json.load(f)['files']

    json_length = len(mappings)
    print('\033[94m' + f'Preparing to submit {json_length} transfers in chunks of {chunk_size}...' + '\033[0m')

    for i in range(0, json_length, chunk_size):        
        chunk = mappings[i: min(i + chunk_size, json_length)]
        prepare_submission(f'condor/{year}/{sample_name}', f'{sample_name}_chunk_{i // chunk_size}', chunk)
        make_submission(f'condor/{year}/{sample_name}', f'{sample_name}_chunk_{i // chunk_size}')

def tests():
    with open('condor/test/test.json', 'r') as f:        
        mappings = json.load(f)['files']

    prepare_submission('condor/test', 'test', mappings[0:2])
    make_submission('condor/test', 'test')

if __name__ == "__main__": 
    tests()