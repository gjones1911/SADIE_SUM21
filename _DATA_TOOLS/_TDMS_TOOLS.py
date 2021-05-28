from nptdms import TdmsFile
import numpy as np
import pandas as pd
import plotly_express
import traceback
channel_dirs = ['__class__', '__delattr__', '__dict__', '__dir__',
                '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__',
                '__getitem__', '__gt__', '__hash__', '__init__', '__init_subclass__',
                '__iter__', '__le__', '__len__', '__lt__', '__module__', '__ne__',
                '__new__', '__reduce__', '__reduce_ex__', '__repr__',
                '__setattr__', '__sizeof__', '__str__', '__subclasshook__',
                '__weakref__', '_cached_chunk', '_cached_chunk_bounds',
                '_file_properties', '_group_properties',
                '_length', '_memmap_dir', '_path', '_raw_data',
                '_raw_data_dtype', '_raw_timestamps', '_read_at_index',
                '_read_channel_data', '_read_channel_data_chunk_for_index',
                '_read_channel_data_chunks', '_read_data_values',
                '_read_slice', '_reader', '_scale_data', '_scaling',
                '_set_raw_data', 'as_dataframe', 'data', 'data_chunks',
                'data_type', 'dtype', 'group_name', 'name', 'path', 'properties',
                'raw_data', 'raw_scaler_data',
                'read_data', 'scaler_data_types', 'time_track']

root_cols = [
'Root Name',
'Title',
'Author',
'Date/Time',
'Groups',
'Description',
'NI_CM_AssetName',
'NI_CM_AssetNodeId',
'NI_CM_DataGroup',
'NI_CM_DeviceName',
'NI_CM_OpState',
'NI_CM_Reason',
'NI_CM_TdmsFormatVersion',
'',
'',
'',
'',
'',
'',
'',
'',
]

def smartDfOpener(filename, **kwargs,):
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


def tdmsArgChecker(**kwargs):
    pass
    #print(kwargs)

def tdmsUseColDictBoolean(g, chname, **kwargs):
    col="usecolsdict"
    if "col" in kwargs:
        col = kwargs["col"]
    return col in kwargs and kwargs[col] is not None and g in kwargs[col] and chname not in kwargs[col][g]

def funcBoolean(bfunc, **kwargs):
    return bfunc(kwargs)

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
        print("\t\t\tProperties: {0}: {1}".format(name, value))
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
                print('the Exception: {}'.format(message))
                print(traceback.format_exc())
            # Iterate through and store the channels
            group_meta[g][ch.name]["data"] = ch.data
        # store the new data frame for this group
        retDfDict[g] = pd.DataFrame(group_meta[g]["DF"])
        print(retDfDict[g].shape)
        for p in properties:
            retDfDict[g][p] = list([properties[p]]*len(retDfDict[g]))


    return group_meta, retDfDict, properties

class Walker:
    """
    Base class for a class of directory search and retrieval
    Tools
    """
    def __init__(self, root=None, dirkeys=None, ):
        self.rootdir = root
        self.dirkeys = dirkeys
        self.dataBundle=None

    def load_file_into_df(self, filename, **kwargs):
        if "getterfunc" in kwargs and kwargs["kwargs"] is not None:
            return kwargs["getterfunc"](filename, kwargs["kwargs1"])
        else:
            return smartDfOpener(filename)


class TDMSDF:
    def __init__(self, filepath=None, groups=None, verbose=False):
        self.tdms_file = None
        self.groups = groups
        self.group_dict = {}
        self.group_channels = {}

    def groups_to_list(self, groupsRet):
        retl = list()
        for g in groupsRet:
            #print('dir g:\n{}'.format(dir(g)))

            print("group name: {}".format(g.name))
            print("group as dataframe: {}".format(g.as_dataframe))
            print("group channels: {}".format(g.channels))
            print("group _channels: {}".format(g._channels))
            print("group properties: {}".format(g.properties))
            print("group properties: {}".format(g.properties['description']))


            #g = str(g)
            retl.append(g.name)
        return retl

    def load_groups_dict(self, ):
        for g in self.groups:
            print("Inside group: {}".format(g))
            self.get_group(g)

    def loadTdms(self, filepath, verbose=False):
        self.tdms_file = TdmsFile.read(filepath)
        if self.groups is None:
            self.groups = self.groups_to_list(list(self.tdms_file.groups()))
        self.load_groups_dict()
        ["{}: {}".format(k, self.group_dict[k]) for k in self.group_dict]

    def get_group(self, groupName='Waveform', verbose=False):
        if verbose:
            print("\n\n\t\tGetting the group: {}".format(groupName))
        self.group_dict[groupName] = self.tdms_file[groupName]
        if verbose:
            print(self.group_dict[groupName])
            print("Grabbing group {} channels".format(groupName))
        self.get_GroupChannels(groupName)


    def get_GroupChannels(self, groupName, verbose=False):
        # get the channels for this group
        self.group_channels[groupName] = {}
        for ch in self.group_dict[groupName].channels():
            if verbose:
                print("for channel: {} of group: {}".format(ch.name, groupName))
            self.group_channels[groupName][ch.name] = {}
            self.group_channels[groupName][ch.name]['channel'] = ch
            self.group_channels[groupName][ch.name]['Length'] = ch._length
            print("\n\n\nThe length : {}\n\n\n".format(ch._length))
            self.group_channels[groupName][ch.name]["dtype"] =ch.dtype
            self.group_channels[groupName][ch.name]["DF"] = ch.as_dataframe()
            if len(self.group_channels[groupName][ch.name]["DF"]) > 0:
                self.group_channels[groupName][ch.name]["MAX"] = np.max(np.abs(ch.as_dataframe().values))
                self.group_channels[groupName][ch.name]["MIN"] = np.min(np.abs(ch.as_dataframe().values))
                self.group_channels[groupName][ch.name]["RMS"] = np.sqrt(np.mean(ch.as_dataframe().values **2))
                self.group_channels[groupName][ch.name]["CF"] = self.group_channels[groupName][ch.name]["MAX"] / self.group_channels[groupName][ch.name]["RMS"]
            else:
                self.group_channels[groupName][ch.name]["MAX"] = np.nan
                self.group_channels[groupName][ch.name]["MIN"] = np.nan
                self.group_channels[groupName][ch.name]["RMS"] = np.nan
                self.group_channels[groupName][ch.name]["CF"] = np.nan

            self.group_channels[groupName][ch.name]["ttrack"] = ch.time_track
            #Iterate through and store the channels
            self.group_channels[groupName][ch.name]["data"] = ch.data
            if verbose:
                print("Name: {}".format(ch.name))
                print("Length: {}".format(self.group_channels[groupName][ch.name]["size"]))
                print("MAX: {}".format(self.group_channels[groupName][ch.name]["MAX"]))
                print("MIN: {}".format(self.group_channels[groupName][ch.name]["MIN"]))
                print("RMS: {}".format(self.group_channels[groupName][ch.name]["RMS"]))
                print("CF: {}".format(self.group_channels[groupName][ch.name]["CF"]))
                print("dtype: {}".format(self.group_channels[groupName][ch.name]["dtype"]))
                print("+++++++\nDF.head():\n{}\n==========\n".format(self.group_channels[groupName][ch.name]["DF"].head()))
                print("ttrack: {}\n++++++++++++++\n".format(self.group_channels[groupName][ch.name]["ttrack"]))

        for ch in self.group_channels[groupName]:
            print("channel: {} complete".format(ch))
            #print(dir(ch))
        #print(dir(self.group_channels[groupName]))

def calculate_rms(df, col=None):
    if col is None:
        col = df.columns.tolist()
    return pd.DataFrame({c:[x] for x, c in zip(np.sqrt(np.mean(df[col].values**2, axis=0)), col)})


def get_peaks(df, cols):
    return pd.DataFrame({c:[x] for x, c in zip(df.abs().max().values.T, cols)} )

def calculate_crestFactor(df, col=None):
    rms = calculate_rms(df, col)
    peak = np.abs(df[col].values).max()
    print("{}: Peak: {}, rms: {}".format(col, peak, rms))
    crestfactor = peak/rms
    return crestfactor
def get_minDF(df):
    return pd.DataFrame({c:[x] for x, c in zip(df.min().values.T, df.columns)} )
def get_maxDF(df):
    return pd.DataFrame({c:[x] for x, c in zip(df.max().values.T, df.columns)} )

def peak_peak_df(df, cols):
    print(cols)
    min_df = get_minDF(df[cols])
    print(min_df)
    max_df = get_maxDF(df[cols])
    print(max_df)
    return max_df - min_df

def mCalculateCrestFactor(df, cols):
    rms = calculate_rms(df, cols)
    peak = np.abs(df[cols].values).max(axis=0)
    try:
        print("peak:\n")
        display(peak)
        print()
    except:
        print("Peak:\n{}".format(peak))
        print()

    #print("Peak: {}\n, rms: {}\n".format(peak, rms))
    crestfactor = peak/rms
    print("crest factor:")
    print(crestfactor)
    print()
    crest_factor = {c:[crestfactor[c][0]] for c in crestfactor}
    return pd.DataFrame(crest_factor)
