from datetime import datetime, timedelta, timezone
import datetime
from time import time
import time
import os
import sys
def getDirContents(directory):
    return os.listdir(directory)

def getROOT():
    return os.path.dirname(os.path.abspath(__file__))


def combineRootContent(root, content, get_true_root=False):
    bslash = r'\\'
    if bslash[0] in root:
        if bslash == content[0]:
            content = content[1:]
    elif '/' in root:
        if '/' == content[0]:
            content = content[1:]
    elif bslash != content[0] and '/' != content[0]:
        return root + '/' + content

    return root + content


def getDirsFromRootContents(root, contents):
    dirlist = list()
    for item in contents:
        root_item = combineRootContent(root, item)
        if os.path.isdir(root_item):
            dirlist.append(root_item)
    return dirlist

class SERFTOOLS:
    def __init__(self, root=None, **kwargs):
        self.directoryparsingtools = self.DirectoryParsingTools(root, **kwargs)
    class DirectoryParsingTools:
        def __init__(self, root=None, **kwargs):
            self.root=root
        def process_folder(self, dateRange, retDirs):
            if dateRange.count(".") == 1:
                # get month based on current set format
                # 5/23/21
                startD, startT = self.process_contentobject(dateRange, isdir=True)
                retDirs["months"][startD]["orig"] = dateRange
                retDirs["months"][startD]["days"] = {}
                retDirs["months"][startD]["time"] = startT
            # otherwise get a day folder
            elif dateRange.count(".") == 2:
                startD, startT = self.process_contentobject(dateRange, isdir=True)
                dateList = dateRange.split(".")
                monthyr = "-".join(dateList[0:2] + dateList[-1:])

                retDirs["months"][monthyr] = {}
                retDirs["months"][monthyr]["orig"] = ".".join(dateList[0:1] + dateList[-1:])
                retDirs["months"][monthyr]["time"] = {startT}
                retDirs["months"][monthyr]["days"] = {startD: {"time": startT,
                                                               "orig": dateRange,
                                                               "files": [],
                                                               }
                                                      }
                return startD, startT, retDirs

        def processDateRange(self, dateRange=[]):
            if len(dateRange) <=2:
                endD, endT = self.process_contentobject("01.99", isdir=True)
                retDirs = {"months":{}}

                if len(dateRange) == 0:
                    startD, startT = self.process_contentobject("01.00", isdir=True)

                    retDirs["months"][startD] = {}
                    retDirs["months"][startD]["orig"] = "01.00"
                    retDirs["months"][startD]["days"] = {}
                    retDirs["months"][startD]["time"] = startT
                    retDirs["months"][endD] = {}
                    retDirs["months"][endD]["orig"] = "01.99"
                    retDirs["months"][endD]["days"] = {}
                    retDirs["months"][endD]["time"] = endT
                    rettuple = [(startD, startT), (endD, endT)]
                    rettuple = sorted(rettuple, key=lambda x: x[0])
                    return rettuple, retDirs
                if len(dateRange) > 0:
                    # get start time for a month folder
                    if dateRange[0].count(".") == 1:
                        # get month based on current set format
                        # 5/23/21
                        startD, startT = self.process_contentobject(dateRange[0], isdir=True)
                        retDirs["months"][startD]["orig"]= dateRange[0]
                        retDirs["months"][startD]["days"]={}
                        retDirs["months"][startD]["time"]=startT
                    # otherwise get a day folder
                    elif dateRange[0].count(".") == 2:
                        startD, startT = self.process_contentobject(dateRange[0], isdir=True)
                        dateList = dateRange[0].split(".")
                        monthyr = "-".join( dateList[0:2] + dateList[-1:])

                        retDirs["months"][monthyr]={}
                        retDirs["months"][monthyr]["orig"]=".".join( dateList[0:1] + dateList[-1:])
                        retDirs["months"][monthyr]["time"]={startT}
                        retDirs["months"][monthyr]["days"]={startD:{"time":startT,
                                                                    "orig": dateRange[0],
                                                                     "files":[],
                                                                    }
                                                            }

                startD, startT, retDirs = self.process_folder(dateRange[0], retDirs=retDirs)
                if len(dateRange) == 2:
                    endD, endT = self.process_contentobject(dateRange[1], isdir=True)
                rettuple = [(startD, startT), (endD, endT)]
                rettuple = sorted(rettuple, key=lambda x:x[0])
                return rettuple
            else:
                rettuple = []
                retDic = {"dirs": {},"dirFiles":[] }
                for date in dateRange:
                    if date.count(".") ==1:
                        if date.split(".")[0] not in retDic['dirs']:
                            retDic["dirs"][date.split(".")[0]] = []
                        rettuple.append(self.process_contentobject(date, isdir=True))
                    else:
                        retDic["dirs"][date.split(".")[0]].append(date)
                        retDic["dirFiles"].append(self.process_contentobject(date, isdir=True))
                        rettuple.append(self.process_contentobject(date, isdir=True))

                rettuple = sorted(rettuple, key=lambda x:x[0])
                return rettuple, retDic

        def getTrueRoot(self, root):
            if self.root is None:
                self.root= root
            self.root = combineRootContent(getROOT(), self.root)
        def convert_dirDate_To_Epoch(self, dirname):
            if dirname.count(".") == 1:
                year = dirname.split(".")[1]
                month = dirname.split(".")[0]
                formatstr = "%m-%y"
                dirname2 = "{}-{}".format(month, year[-2:])
                dirname = "{}-{}".format(month, year)
            else:
                month = dirname.split(".")[0]
                day = dirname.split(".")[1]
                year = dirname.split(".")[2]
                formatstr = "%y-%m-%d"
                #print("{}".format(year[-2:]))
                dirname2 = "{2}-{0}-{1}".format(month, day, year[-2:])
                dirname = "{2}-{0}-{1}".format(month, day, year)
            return convertDateTimeToEpoch(dirname2, formatstr=formatstr), dirname

        def convert_fileDate_To_Epoch(self, filename, ):
            formatstr = "%{}-%{}-%{} %{}:%{}:%{}"
            filenamedate = self.processSerfFileName(filename, formatstr)
            return convertDateTimeToEpoch(filename, formatstr=formatstr), filenamedate
        def processSerfFileName(self, filename, formatstr="%{}.%{}.%{} %{}:%{}:%{}:%{}"):
            # rmv file type
            # 0123456789AEFGH
            # yyyymmdd_hhmmss.tdms
            # A=10, E=11, F=12, G=13, H=14
            year = filename[0:4]
            month = filename[4:6]
            day = filename[6:8]
            hour = filename[9:11]
            min = filename[11:13]
            sec = filename[13:15]
            datetimeStr = formatstr.format(year, month, day, hour, min, sec)
            return datetimeStr

        def process_contentobject(self, content, isdir=True):
            if isdir:
                return self.convert_dirDate_To_Epoch(content,)
            else:
                return self.convert_fileDate_To_Epoch(content,)





# convert epoch time to some other time

def convertDateTimeToEpoch(datetimeStr, formatstr ="%m/%d/%y" ):
    return datetime.datetime.strptime(datetimeStr, formatstr).timestamp()

def convertDateTimetoEpoch(datetimeStr, formatstr):
    return


def convertEpochToDateTime(epochT, formatstr="%m-%d-%y %H:%M:%S"):
    return time.strftime(formatstr, time.localtime(epochT))

def convertEpochToDateTimeDelm(epochT, formatstr="%m{}%d{}%y %H{}%M{}%S", delim=("-", ":")):
    formatstr = formatstr.format(delim[0], delim[1])
    return time.strftime(formatstr, time.localtime(epochT))

def convertEpochToTime(epochT, formatstr="%H:%M:%S"):
    return convertEpochToDateTime(epochT, formatstr)

def convertEpochToDate(epochT, formatstr="%m-%d-%y"):
    return convertEpochToDateTime(epochT, formatstr)

def convertEpochToDateHyph(epochT, formatstr="%m-%d-%y"):
    return convertEpochToDateTime(epochT, formatstr)

def convertEpochToDateSlsh(epochT, formatstr="%m/%d/%y"):
    return convertEpochToDateTime(epochT, formatstr)

def convertEpochToDateDot(epochT, formatstr="%m.%d.%y"):
    return convertEpochToDateTime(epochT, formatstr)

def process_range(inputslist, format_str = "%m.%d.%y", verbose=False,
                  past="01.01.00", future="12.01.50"):
    # get the first month to look for
    if len(inputslist) > 0:
        dstart = inputslist[0].split(".")
        m1 = dstart[0]
        d1 = dstart[1]
        y1 = dstart[2][-2:]
        if verbose:
            print("m1: {}, d1: {}, y1: {}".format(m1, d1, y1))
        date_strt = "{}.{}.{}".format(m1, d1, y1)
        #s_epoch = datetime.datetime.strptime(date_strt, "%m.%d.%y").timestamp()
        s_epoch = datetime.datetime.strptime(date_strt, format_str).timestamp()
    else:
        s_epoch = datetime.datetime.strptime(past, format_str).timestamp()
    if len(inputslist) == 1:
        return s_epoch
    if len(inputslist) > 1:
        dend = inputslist[1].split(".")
        m2 = dend[0]
        d2 = dend[1]
        y2 = dend[2][-2:]
        if verbose:
            print("m2: {}, d2: {}, y2: {}".format(m2, d2, y2))
        date_end = "{}.{}.{}".format(m2, d2, y2)
        e_epoch = datetime.datetime.strptime(date_end, format_str).timestamp()
    else:
        e_epoch = datetime.datetime.strptime(future, format_str).timestamp()
    # get the first set of data
    return s_epoch, e_epoch