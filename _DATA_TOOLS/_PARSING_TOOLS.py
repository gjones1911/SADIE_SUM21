from ._TDMS_TOOLS import TdmsFile, tdmsUseColDictBoolean
from ._DATES_TIMES import *
from ._WAVEFORM_FEARTURE_CALCULATIONS import *
from ._TIME_DATE_TOOLS import *
import traceback

class TDMS_DIR_PARSER:
    def __init__(self, **kwargs):
        self.initArgsCheck(key_word="keys",
                           keys=['root_dir', 'data_range'])
        self.root_path = kwargs['root_dir']
        #print('root: ',self.root_path)
        self.date_range = self.process_date_range(kwargs['date_range'])
        #print(self.date_range)
        self.args_last_checked = False
        # This will keep track of which method called which arge checker
        # last. If it was it will not check it again to avoid unneeded
        # method calls
        self.ArgfussyFuncDict = {}

    def print_error(self, msg="!!!!!!  ERROR  !!!!!", traceback=""):
        print("{}\n\t\t--------------\n{}".format(msg, traceback))

    def process_date_range(self, date_ranges):
        #print("date_ranges__: ", date_ranges)
        dstart = convert_datestr_datetime(date_ranges[0], getdelim=True)
        dend = convert_datestr_datetime(date_ranges[1], getdelim=True)
        return dstart, dend

    def initArgsCheck(self,key_word="keys", endHere=True, **kwargs):
        if key_word not in kwargs:
            if endHere:
                self.print_error("Developer error!!!! we need {} in kwargs".format(key_word),
                                  traceback.format_exc())
                quit()
            return False
        return True

    def persnicky_check(self, fussyFunc,key_word, endHere=True, **kwargs):
            if  str(fussyFunc) not in self.ArgfussyFuncDict or not self.ArgfussyFuncDict[str(fussyFunc)]:
                self.ArgfussyFuncDict[str(fussyFunc)] = True
            if self.ArgfussyFuncDict[str(fussyFunc)]:
                return True
            self.ArgfussyFuncDict[str(fussyFunc)] = True
            if not fussyFunc(key_word, endHere=endHere, **kwargs):
                quit(-11)
            return True


    def root_walkerArgsCheck(self,reqs=("datetime_range",) ,endHere=True, **kwargs):

        if self.ArgfussyFuncDict:
            return True
        for k in reqs:
            if k not in kwargs:
                if endHere:
                    self.print_error("root_walkerArgscheck missing required argument: {}".format(k),
                                     traceback=traceback.format_exc())
                    quit()
        return True

    def get_dirs_in_range(self, dirlist):
        ret_list = list()
        for directory in dirlist:
            ord = directory
            directory =convert_datestr_datetime(directory, getdelim=True)
            #print("converted: {}/{}".format(ord, directory))
            if check_dates(d1=self.date_range[0], cmp='leq', d2=directory) and check_dates(d1=self.date_range[1], cmp='geq', d2=directory):
                ret_list.append(ord)
        return ret_list


    def root_walker_daily_features(self, rootpath=None, walkerFunc=None, filetype=".tdms", select_group="Waveform",
                    **kwargs):
                #keys=[""], date_T=None, verbose=True, retDF_dict={}, ret_dates=False,
                #usecols=None, groups=None, usecolsdict=None, df_groupDF_dict=None,
                #dates_loaded = [], lengthS=None,
        """
               This will walk through the root directory looking
               for paths or directories of a given type/name and perform
               some method on them passed by users or declared elsewhere
        :param :
        :param rootpath:
        :param walkerFunc: method used during the walk on found files/dir
        :param argchecker: method used to check this methods arguments
        :param keys: the required arguments that will be used to check
                     this methods arguments
                     These include:
                        *
        :param kwargs: the arg_name=?, arguments passed to this method
        :return:
        """
        #self.root_walkerArgsCheck(reqs=tuple(), **kwargs)

        if rootpath is None:
            rootpath = self.root_path

        # get the directories to search files
        # these represent months
        roots_directories = getDirContents(rootpath)
        # process the date range into epoch times for comparison
        roots_directories = self.get_dirs_in_range(roots_directories)
        #print("Directories in range: {}".format(roots_directories))
        # now go through each directory getting its day files and loading
        # and processing them for their features
        # for each day get all available files, merge, and calculate the needed features
        new_df = None
        for rdir in roots_directories:
            # get this directories folders(days)
            day_folders = getDirContents(rootpath + '/' + rdir + '/')
            day_folders = self.get_dirs_in_range(day_folders)
            #print('day folders: ',day_folders)
            # now go through the day folders, get the files of the needed type, and
            # process the files into a dataframe and store the features as a day based
            # dataframe
            for day in day_folders:
                #print("\t\t\t\t{}".format(day))
                possible_tdms = getDirContents("/".join([rootpath, rdir,day]))
                #print(possible_tdms)
                filecnt = 0
                # for every sample for this day
                for fileP in possible_tdms:
                    if fileP.endswith(filetype):
                        #print('\t\tthing: ', fileP)
                        filecnt += 1
                        #print("the {} file ".format(filecnt))
                        # open this day
                        group_meta, retDfDict, properties = smartDfOpener("/".join([rootpath, rdir, day, fileP]))
                        # if this is the first run
                        # create new running data frame for this day
                        if new_df is None:
                            #print("new df")
                            new_df = pd.DataFrame()
                            file_datestr = get_datastr_from_filename(fileP)
                            #print("those things: {}".format(file_datestr))
                            #ddt =convert_datestr_datetime(file_datestr, getdelim=False, delim='', form="%Y-%m-%d %H:%M:%S")
                            #print("ddt: {}".format(ddt))
                            new_df['datetime'] = [file_datestr]
                            colname = group_meta[select_group]['channel_names']
                            tmp_df = group_meta[select_group]["DF"][colname]

                            #new_df['RMS'] = mCalculateCrestFactor(tmp_df, colname)
                            # get the crest factor
                            crestF_DF = BCalculateCrestFactor(tmp_df, colname)
                            for c in crestF_DF:
                                new_df[c] = crestF_DF[c]
                            # get the Derived peak
                            derived_peakDF = derived_peak(tmp_df, colname)
                            for c in derived_peakDF:
                                new_df[c+"_derived_Peak"] = derived_peakDF[c]

                            # get the High frequency RMS
                            rms_df = calculate_rms(tmp_df, col=colname, rename=True)
                            for c in rms_df:
                                new_df[c] = rms_df[c]

                            # get the peak-peak
                            pkpk_df = peak_peak_df(tmp_df, colname)
                            for c in pkpk_df:
                                new_df[c + "_PKPK"] = pkpk_df[c]


                            # get the RMS

                            # get the True peak
                            tpk_df = get_peaks(tmp_df, colname)
                            for c in tpk_df:
                                new_df[c+"_TruePk"] = tpk_df[c]

                            # get the min
                            min_df = get_minDF(tmp_df[colname])
                            for c in min_df:
                                new_df[c+"_MIN"] = min_df[c]

                            # get the max
                            max_df = get_maxDF(tmp_df[colname])
                            for c in max_df:
                                new_df[c+"_MAX"] = max_df[c]


                            #print(new_df)
                        else:
                            filecnt += 1
                            #print("the-- {} file ".format(filecnt))
                            to_addDF = pd.DataFrame()
                            file_datestr = get_datastr_from_filename(fileP)
                            # print("those things: {}".format(file_datestr))
                            #ddt = convert_datestr_datetime(file_datestr, getdelim=False, delim='',
                            #                               form="%Y-%m-%d %H:%M:%S")
                            # print("ddt: {}".format(ddt))
                            to_addDF['datetime'] = [file_datestr]
                            colname = group_meta[select_group]['channel_names']
                            tmp_df = group_meta[select_group]["DF"][colname]

                            # new_df['RMS'] = mCalculateCrestFactor(tmp_df, colname)
                            crestF_DF = BCalculateCrestFactor(tmp_df, colname)
                            for c in crestF_DF:
                                to_addDF[c] = crestF_DF[c]
                            #print(to_addDF)

                            # get the Derived peak
                            derived_peakDF = derived_peak(tmp_df, colname)
                            for c in derived_peakDF:
                                to_addDF[c+"_derived_Peak"] = derived_peakDF[c]

                            # get the High frequency RMS
                            rms_df = calculate_rms(tmp_df, col=colname, rename=True)
                            for c in rms_df:
                                to_addDF[c] = rms_df[c]

                            # get the peak-peak
                            pkpk_df = peak_peak_df(tmp_df, colname)
                            for c in pkpk_df:
                                to_addDF[c + "_PKPK"] = pkpk_df[c]

                            # get the RMS

                            # get the True peak
                            tpk_df = get_peaks(tmp_df, colname)
                            for c in tpk_df:
                                to_addDF[c + "_TruePk"] = tpk_df[c]

                            # get the min
                            min_df = get_minDF(tmp_df[colname])
                            for c in min_df:
                                to_addDF[c + "_MIN"] = min_df[c]

                            # get the max
                            max_df = get_maxDF(tmp_df[colname])
                            for c in max_df:
                                to_addDF[c + "_MAX"] = max_df[c]



                            new_df = pd.concat([new_df, to_addDF], ignore_index=True)
                            #print(new_df)
                        """
                        try:
                            #display(group_meta)
                            display(group_meta['channel_names'])
                            display(group_meta[select_group]["DF"][group_meta['channel_names']].head())
                            #display(group_meta)
                            #print()
                            #display(retDfDict)
                            #print(properties)
                        except Exception as ex:
                            pass
                            #print(ex)
                            #print(group_meta[select_group]['channel_names'])
                            #print(group_meta[select_group]["DF"][group_meta[select_group]['channel_names']].head())
                            #print(group_meta[select_group].keys())
                            #print(retDfDict)
                            #print(properties)
                        """
        #print(new_df)
        return new_df



def smartDfOpener(filename, **kwargs,):
    """
    Can be used to open, csv, excel, hd5, or tdms files
    :param filename: the pathto/filename of the file you want to open
    :param kwargs:
            usecols: list of columns of data you want to use if opening a csv/excel
            usecolsdict: dictionary of form: group=key, usecols=val for the different groups
                         in the .tdms file
            groups: if only seeking to get a specific group give it's name
            low_memory: option for opening a csv so that it does not cast the data into
                        a lower memory representation
    :return: a pandas dataframe containing the data from the file
    """
    usecols = None
    # return print("we have a .csv {}\nand kwargs:\n{}".format(filename, kwargs))
    if "loader" in kwargs:
        loader = kwargs["loader"]

    if "usecols" in kwargs:
        usecols = kwargs["usecols"]
    #if "" in kwargs:
    #    usecols = kwargs[""]
    if "low_memory" in kwargs:
        low_memory = kwargs["low_memory"]

    if str(filename).endswith(".csv"):
        low_memory=False
        return pd.read_csv(filename, usecols=usecols, low_memory=low_memory)
    elif str(filename).endswith(".xlsx"):
        #return print("we have a .xlsx {}\nand kwargs:\n{}".format(filename, kwargs))
        return pd.read_excel(filename, usecols=usecols, )
    elif str(filename).endswith(".tdms"):
        #pd.read_csv(filename, kwargs)
        # return print("we have a .tdms {}\nand kwargs:\n{}".format(filename, kwargs))
        return tdmsDataFrame(filename, **kwargs)
    elif str(filename).endswith(".hdf"):
        #return pd.read_csv(filename, kwargs)
        return print("we have a .hdf {}\nand kwargs:\n{}".format(filename, kwargs))



def tdmsDataFrame(filename, cnt=0, **kwargs):
    # https://nptdms.readthedocs.io/en/stable/reading.html
    groups = None
    group_meta= {}
    retDfDict = {}

    #tdmsArgChecker(**kwargs)

    verbose=True
    usecolsdict=None
    #if "" in kwargs:
    #    = kwargs[""]
    if "groups" in kwargs:
        groups = kwargs["groups"]
    if "usecolsdict" in kwargs:
        usecolsdict = kwargs["usecolsdict"]
    # open the file
    tdmsDF = TdmsFile.read(filename)
    properties = {}
    # Iterate over all items in the file properties and print them
    for name, value in tdmsDF.properties.items():
        #print("\t\t\tProperties: {0}: {1}".format(name, value))
        properties[name] = value

    # if given some groups to get get those, otherwise get
    # them all
    groups_list=  list(tdmsDF.groups())
    if groups is None:
        for g in groups_list:
            if g.name not in group_meta:
                group_meta[g.name]={}
            group_meta[g.name]["name"]=g.name
            group_meta[g.name]["DF"]=g.as_dataframe
            group_meta[g.name]["_channels"]=g._channels
            group_meta[g.name]["channels"]=g.channels
            group_meta[g.name]["properties"]=g.properties
            group_meta[g.name]["channel_names"] = list()
    else:
        for g in groups_list:
            if g.name in groups:
                if g.name not in group_meta:
                    group_meta[g.name] = {}
                group_meta[g.name]["name"] = g.name
                group_meta[g.name]["DF"] = g.as_dataframe()
                group_meta[g.name]["_channels"] = g._channels
                group_meta[g.name]["channels"] = g.channels()
                group_meta[g.name]["properties"] = g.properties
                group_meta[g.name]["channel_names"] = list()
    groupsnames = list(group_meta.keys())

    # now fill in the channel data with acutal data
    for g in groupsnames:
        group_meta[g]['DF'] = pd.DataFrame()
        for ch in tdmsDF[g].channels():
            #if "usecolsdict" in kwargs and kwargs["usecolsdict"] is not None and g in kwargs["usecolsdict"] and ch.name not in kwargs["usecolsdict"][g]:
            if tdmsUseColDictBoolean(g, ch.name, **kwargs):
                continue
            # get the channels for this group
            if verbose:
                pass
                #print("for channel: {} of group: {}".format(ch.name, g))
            group_meta[g][ch.name] = {}
            channelName = ch.name
            # group_meta[g]["channel_names"]
            group_meta[g]["channel_names"].append(channelName)
            group_meta[g][ch.name]['channel'] = ch
            group_meta[g][ch.name]['Length'] = ch._length
            #print("\n\n\nThe length : {}\n\n\n".format(ch._length))
            group_meta[g][ch.name]["dtype"] = ch.dtype
            #group_meta[g][ch.name]["DF"] = ch.as_dataframe()
            '''
            print("\n{:-3}".format(5))
            print(ch.as_dataframe().)
            print(ch.as_dataframe().values.flatten())
            print("{:-3}\n".format(5))
            '''
            group_meta[g]["DF"][ch.name] = ch.as_dataframe().values.flatten()
            try:
                group_meta[g][ch.name]["ttrack"] = ch.time_track()
            except Exception as ex:
                group_meta[g][ch.name]["ttrack"] = None
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                #print('the Exception: {}'.format(message))
                #print(traceback.format_exc())
            # Iterate through and store the channels
            group_meta[g][ch.name]["data"] = ch.data
        # store the new data frame for this group
        retDfDict[g] = pd.DataFrame(group_meta[g]["DF"])
        #print(retDfDict[g].shape)
        for p in properties:
            retDfDict[g][p] = list([properties[p]]*len(retDfDict[g]))

    return group_meta, retDfDict, properties