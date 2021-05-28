import pandas as pd
import numpy as np
pd.options.mode.use_inf_as_na = True



# TODO: Pandas manipulation tools
def basic_drop_impute(df, replace=('', np.inf, -999), inplace=True):
    """
        This method is designed to remove the nan and the passed replace values
    :param df:
    :param replace:
    :param inplace:
    :return:
    """
    for to_replace in replace:
        df.replace(to_replace, np.nan, inplace=True)
    if inplace:
        df.dropna(inplace=True)
        return
    else:
        return df.dropna()

def drop_cols(df, drops, inplace=True, verbose=False):
    if inplace:
        print('Droping the columns: {}'.format(drops))
        df.drop(columns=drops)
        return
    else:
        return df.drop(columns=drops, inplace=False)


def check_table_type(table_name):
    """
    This will return what type of file name you have given it in the form of a string
    that will be either csv for a CSV file, or xlsx for an excel file. This method is used
    by other methods to know what type of file it is working with. If the type of file is
    not one of the two the method terminates you program with a explanation
    :param table_name:  Name of data file to open.
    :return: 'csv' for a CSV, and 'xlsx' for an excel workbook
    """
    if table_name[-3:] == 'csv':
        return 'csv'
    elif table_name[-4:] == 'xlsx':
        return 'xlsx'
    else:
        print('Unknown Table storage type for file: {}'.format(table_name))
        print('Terminating program')
        quit()

def show_missing(df, ):
    missingsO = df.isna().sum()
    for cc in sorted(missingsO.index.tolist()):
        if missingsO[cc] > 0:
            print('{}: {}'.format(cc, missingsO[cc]))
    return

def smart_table_opener(table_file, usecols=None,):
    """
        This will open a dataframe from the given file as long as it is a csv or excel file
    :param table_file: name of table to open
    :param usecols: (optional) the columns of the table you want to load
    :return:
    """
    table_type_options = ['xlsx', 'csv', ]
    if check_table_type(table_file) == table_type_options[0]:
        return pd.read_excel(table_file, usecols=usecols, )
    elif check_table_type(table_file) == table_type_options[1]:
        return pd.read_csv(table_file, usecols=usecols, low_memory=False,)

def data_merger(data_sets, joins=('fips', 'FIPS', 'geoid'), target=None, verbose=False, drop_joins=False, ):
    """This method can be used to merge a set of data frames using a shared
       data column. the first argument is a list of the dataframes to merge
       and the second argument is a list of the column labels used to perform the merge
       TODO: some work needs to be done for error checking
       TODO: add more flexibility in how the merge is perfomed
       TODO: make sure the copy rows are removed
    :param data_sets: a list of data frames of the data sets that are to be joined
    :param joins: a list of the column labels used to merge, the labels should be in the s
                  same order as the data frames for the method to work. Right now this works
                  best if the label used is the same for all. This makes sured the duplicate
                   columns are not created.
    :param verbose: at this point does nothing but can be used to inform user of what
                    has occured
    :return: a reference to the new merged dataframe
    """
    cnt = 0
    if len(data_sets) == 1:
        return data_sets[0]
    for df in range(1, len(data_sets)):
        data_sets[0] = data_sets[0].merge(data_sets[df], left_on=joins[0], right_on=joins[df], how='left')
        if verbose:
            print(data_sets[0].columns)

        if (joins[0] + '_x') in data_sets[0].columns.values.tolist() or (
                (joins[0] + '_y') in data_sets[0].columns.values.tolist()):
            data_sets[0].drop(columns=[(joins[0] + '_x'), (joins[1] + '_y')], inplace=True)
        if target is not None and ((target + '_x') in data_sets[0].columns.values.tolist() or (
                (target + '_y') in data_sets[0].columns.values.tolist())):
            data_sets[0][target] = data_sets[0].loc[:, target + '_x']
            data_sets[0].drop(columns=[(target + '_x'), (target + '_y')], inplace=True)
    if drop_joins:
        data_sets[0].drop(columns=list(joins), inplace=True)
    return data_sets[0]