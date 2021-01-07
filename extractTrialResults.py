import os
import re

query_range = range(4, 8)
trial_nums = range(6)

def check_line_for_time(line):
    #sample line- 2 function calls in 0.000 seconds
    pattern = re.compile(r'(.*) function calls in (.*) seconds')
    searched = pattern.search(line)
    if searched:
        print(searched.group(2).strip())
        runtime = float(searched.group(2).strip())
        return runtime
    else:
        return None

#check_line_for_time('\t\t\t2 function calls in 204.512345678 seconds       \n')

def extract_time_from_file(fname):
    with open(fname) as fh:
        for line in fh: #the runtime we're looking for shows up only once
            runtime = check_line_for_time(line)
            if runtime != None:
                return runtime

#extract_time_from_file('q4_nsql_test_0.txt')

def extract_times_to_csv(dirname):
    resfile = open(os.path.join(dirname, 'all_runtimes.csv'), 'w+')
    resfile.write('Trial,Runtime (s)')
    for file in os.listdir(dirname):
        if file.endswith(".txt"): #this should be enough of a screen
            fpath = os.path.join(dirname, file)
            print("fpath is: " + fpath)
            runtime = extract_time_from_file(fpath)
            print(file[-5])
            trial_num = int(file[-5])
            resfile.write(str(trial_num) + ',' + str(runtime))
    
    resfile.close()
    
extract_times_to_csv('/home/pranav/catalog_experiments/nsql1con')
