import time
import datetime
import re
# Time conversion tools



###########################################################
#     Convert Epoch to DATETIMES/DATES
##########################################################
def check_dates(d1, d2, cmp='ge', division=1, formatstr1=None, formatstr2=None):
    if cmp == "ge":
        return d1 > d2
    elif cmp == "geq":
        return d1 >= d2
    elif cmp == "le":
        return d1 < d2
    elif cmp == "leq":
        return d1 <= d2
    elif cmp == "eq":
        return d1 == d2
    elif cmp == "neq":
        return d1 != d2
    else:
        return d1 <= d2



def get_delim(datestr, ):
    ch = datestr[0]
    idx = 1
    while ch.isalnum() and idx < len(datestr):
        ch = datestr[idx]
        idx += 1
    #print("delim1:{}".format(ch))
    return ch


def convert_datestr_datetime(datestr, form=None, delim=None, getdelim=False, delims=("-", ':')):
    bb = False
    # get the delimeter
    if datestr.find(" ") > 0:
        datestr, form = adjust_mmddyy_hms(datestr, delims=delims, returnform=False )
        bb = True
        if "f" in form:
            print(form)
            return datetime.datetime.strptime(datestr, form.format(delim))
    elif delim is None and getdelim:
        delim = get_delim(datestr)
        # adjust datestr if needed
    if form is None and not bb:
        #print("here")
        #print("datestr, ", datestr)
        #print("Form")
        datestr, form = adjust_mmddyy(datestr, delim=delim, returnform=True)
        #print(datestr, form)
    elif not bb:
        datestr= adjust_mmddyy(datestr, delim=delim, returnform=False)
    return datetime.datetime.strptime(datestr, form.format(delim))

def get_date_str_dif(datestr1, datestr2, division=1, format1=None, format2=None,
                     delim1=None, delim2=None,
                     getdelim1=False, getdelim2=False):

    d1 = convert_datestr_datetime(datestr1, form=format1, delim=delim1, getdelim=True)
    d2 = convert_datestr_datetime(datestr2, form=format2, delim=delim2, getdelim=True)
    print("d1: {}, d2: {}".format(d1, d2))
    return (d1-d2).total_seconds()/division

def get_datestr(dateObj, formatstr="%y-%m-%d %H:%M:%S:%f"):
    print()
    return datetime.datetime.strftime(dateObj, formatstr)


def get_datastr_from_filename(tdmsname, splitval='_', filetype=".tdms", yearspos=(0, 4), monthpos=(4, 6),
                              daypos=(6, 20),
                              hourpos=(0, 2), minpos=(2, 4), secspos=(4, 6), msecs=None):
    # make sure this is what we need
    if not tdmsname.endswith(".tdms"):
        print("not a tdms file")
        return None
    span_group = re.search(".tdms$", tdmsname)
    found = span_group.group()
    span_g = span_group.span()
    name_part = tdmsname[:span_g[0]]
    # print("found: {}".format(found))
    # print("span: {}".format(span_g))
    # print("the name: {}".format(name_part))
    span_group2 = re.search("^\d.*_", name_part)
    dateN = name_part.split("_")[0]
    dateT = name_part.split("_")[1]
    # print("date: {}".format(dateN))
    # print("time: {}".format(dateT))
    dateDate = dateN[yearspos[0]:yearspos[1]] + "-" + dateN[monthpos[0]:monthpos[1]] + "-" + dateN[daypos[0]:daypos[1]]
    # print(dateDate)
    dateTime = dateT[hourpos[0]:hourpos[1]] + ":" + dateT[minpos[0]:minpos[1]] + ":" + dateT[secspos[0]:secspos[1]]
    if len(dateT) >= 8:
        dateTime += ":" + dateT[6:8]
    if len(dateT) >= 10:
        dateTime += ":" + dateT[8:10]
    # print(dateTime)

    dateDate_dateTime = dateDate + " " + dateTime
    # print(dateDate_DateTime)
    return dateDate_dateTime

def adjust_mmddyy_hms(mmddyy_hms, delims=None, returnform=False):
    # get and and time parts seperately
    Date = mmddyy_hms.split(" ")[0].strip()
    Time = mmddyy_hms.split(" ")[1].strip()

    TimeDelim = get_delim(Time)
    DateDelim = get_delim(Date)

    print("Time delim: {}, date delim: {}".format(TimeDelim, DateDelim))
    Date, dateform = adjust_mmddyy(Date, delim=DateDelim, returnform=True)
    newTime, timeform = adjust_mmddyy(Time, delim=TimeDelim, returnform=True, )
    print(dateform)
    print()
    return Date + " " + Time, dateform.format(DateDelim) + " " + timeform.format(TimeDelim)

def adjust_mmddyy(mmddyystr, delim=".", returnform=False):
    """
        This method just reformats the date given to
        be read by datetime.datetime.strptime()
    :param mmddyystr: string of the form mm"delim"dd"delim"yy or yy"delim"mm"delim"dd
    :param delim: delimeter used to seperate the different parts of the data, i.e. mm-dd-yy has
                  a delimeter of "-"
    :return:
    """
    form = ''
    ret_str = ""
    #print(delim)
    #print('datestr ',mmddyystr)
    splitDate = mmddyystr.split(delim)
    year = splitDate[-1]
    if len(splitDate) == 2:
        if len(splitDate[0]) == 4:
            year = splitDate[0][-2:]
            ret_str = "{}".format(delim).join([year] + splitDate[-1:])
            form = "%y{}%m"
        elif len(splitDate[-1]) == 4:
            year = splitDate[-1][-2:]
            ret_str = "{}".format(delim).join(splitDate[0:-1] + [year])
            form = "%m{}%y"
    elif len(splitDate[-1]) == 4:
        year = year[-2:]
        ret_str = "{}".format(delim).join(splitDate[0:-1] + [year])
        form = "%m{0}%d{0}%y"
    elif len(splitDate[0]) == 4:
        year = splitDate[0][-2:]
        ret_str = "{}".format(delim).join([year] + splitDate[1:])
        form = "%y{0}%m{0}%d"
    elif delim == ":":
        #print(mmddyystr)
        if len(mmddyystr)==8:
            ret_str = mmddyystr
            form = "%H{0}%M{0}%S"
        elif len(mmddyystr) == 11:
            ret_str = mmddyystr
            form = "%H{0}%M{0}%S:%f"
    else:
        ret_str = mmddyystr
    if returnform:
        return ret_str, form
    return ret_str

# converts a given string into an epoch time
def convert_mdy_epoch(mdy_str, frmt_str="%m{0}%d{0}%y", delim=None):
    mdy_str = mdy_str.strip()
    # check for needed format
    if delim is None:
        delim = get_delim(mdy_str)
    mdy_str = adjust_mmddyy(mdy_str, delim=delim, returnform=False)
    return datetime.datetime.strptime(mdy_str, frmt_str.format(delim)).timestamp()

def convert_ymd_epoch(ymd_str, frmt_str="%y{0}%m{0}%d", delim=None):
    ymd_str = ymd_str.strip()
    # check for needed format
    if delim is None:
        delim = get_delim(ymd_str)
    print("delim:{}".format(delim))
    ymd_str = adjust_mmddyy(ymd_str, delim=delim, returnform=False)
    print('ymd_str:{}::'.format(ymd_str))
    print("{}::".format(frmt_str.format(delim)))
    return datetime.datetime.strptime(ymd_str, frmt_str.format(delim)).timestamp()

def convert_epoch_datetime(epochtime, formatstr="%m{0}%d{0}%"):
    pass

###########################################################
#     Convert DATETIME/DATES to epoch/mins/sec/hours/days
###########################################################
###########################################################
#
###########################################################
###########################################################
#
###########################################################
###########################################################
#
###########################################################
