import os
import sys

password = sys.argv[1]

files = ['./lib/01_demographic_mappings.py', './lib/02_Geocode_Consolidation_v2_0.py', './lib/03_Generate_Metrics.py']

for idx, fil in enumerate(files):
	print ("Running {}/{}".format(idx,len(files)))
	os.system('python {} {}'.format(fil, password))

