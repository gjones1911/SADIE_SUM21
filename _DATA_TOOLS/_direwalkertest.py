from _DATA_TOOLS._PARSING_TOOLS import *
from _DATA_TOOLS._DATES_TIMES import *

rootpath =  r'C:\\Users\\gjone\\__SADIE_WRK\\serf_041821_042621'
print("rootpath: {}".format(rootpath))
data_range =["01.2021", "01.02.2021", "02.2021"]
data_range =["01.2021", "02.2021"]
data_range =["02.2021", "01.2021"]
dirwalker = DIR_PARSER(root_dir=rootpath, data_range=data_range)
dirwalker.root_walker(rootpath=rootpath, walkerFunc=None, )

serftools = SERFTOOLS(root=rootpath)

startInfo, endInfo = serftools.directoryparsingtools.processDateRange(data_range)

print("Start Info: {}".format(startInfo))
print("End Info: {}".format(endInfo))

# now get the stuff from the root that meets the date reqs