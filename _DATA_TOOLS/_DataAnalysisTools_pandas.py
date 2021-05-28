import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler, StandardScaler
import numpy as np
from re import sub
from decimal import Decimal


def sort_dict(d, kv=0, reverse=False):
    return dict(sorted(d.items(), key=lambda x: x[kv], reverse=reverse))


def stat_table_maker(df, new_table, threshold=None, verbose=0, dropOT=False):
    desc = df.describe()
    vars = desc.columns.tolist()
    #print('desc')
    #print(desc)
    #print("vars: {}".format(vars))
    drp = list()
    # go through description getting neccesary values
    rd = {
        'Variables':[],
        'Mean': [],
        'std': [],
        'missing':[],
        'Min': [],
        'Max': [],
        'Range': [],
    }
    for v in vars:
        missing =np.around((len(df) - desc.loc['count', v])/len(df), 2)

        rd['% missing'].append(missing)
        rd['Variables'].append(v)
        rd['Mean'].append(desc.loc['mean', v])
        rd['std'].append(desc.loc['std', v])
        # get the number of missing (total - counted) and divide py total to get percentage missing
        rd['Min'].append(desc.loc['min', v])
        rd['Max'].append(desc.loc['max', v])
        rd['Range'].append([desc.loc['min', v], desc.loc['max', v]])
        if threshold is not None and missing > threshold:
            if verbose:
                print('-------------------------------------------------------------------')
                print("             dropping variable: {}, overthreshold: {:.3f}/{:.3f}".format(v, threshold, missing))
                print('-------------------------------------------------------------------\n')
            drp.append(v)
    #pd.DataFrame(rd).sort_values(by=['missing'], ascending=True).to_excel(new_table, index=False)
    rdf =pd.DataFrame(rd)
    rdf = rdf.sort_values(by=['missing'], ascending=True)
    if new_table is not None:
        rdf.to_excel(new_table, index=False)
    if dropOT:
        print("Dropping: {}".format(drp))
        df.drop(columns=drp, inplace=True)
    return rdf

def read_stat_table_min_max_dict(tableName):
    df = pd.read_excel(tableName, index_col='Variables')
    rd = {}
    #print(df.index.tolist())
    for v in df.index.tolist():
        #print('v: {}'.format(v))
        rd[v] = {}
        rd[v]['miss'] = df.loc[v, 'missing']
        rd[v]['min'] = df.loc[v, 'Min']
        rd[v]['max'] = df.loc[v, 'Max']
        rd[v]['std'] = df.loc[v, 'std']
        rd[v]['mean'] = df.loc[v, 'Mean']

    return rd

def calculateMissingPerc(df, verbose=True):
    df_count = df.count()
    dflen = df.shape[0]
    ret_count = {}
    for c in df_count.index.tolist():
        mper = (1 -(df_count[c]/dflen)) * 100
        if verbose:
            print("{}: % {:.3}".format(c, mper ))
        ret_count[c] = [mper]
    return pd.DataFrame(ret_count)

def convertMonetaryToFloat(df, colname, newcol_name=None):
    ret_l = list()
    if newcol_name is None:
        newcol_name = colname
    for v in df[colname].values:
        if v is not np.nan and v is not None:
            ret_l.append(float(Decimal(sub(r'[^\d\-.]', '', v))))
        else:
            ret_l.append(np.nan)
    df[newcol_name] = ret_l

def countmissing(df, reverse=False, verbose=True, retd=True):
    missingsO = df.isna().sum()
    rd = {}
    rdpct = {}
    N = df.shape[0]
    for cc in missingsO.index.tolist():
        if missingsO[cc] > 0:
            if verbose:
                print('{}: {}'.format(cc, missingsO[cc]))
            rd[cc] = missingsO[cc]
            rdpct[cc] = np.around(missingsO[cc]/N, 4)
    rd =sort_dict(rd, reverse=reverse)
    rdpct = sort_dict(rdpct, reverse=reverse)
    if retd:
        return rd, rdpct
    return


def generate_droplist(missingcountD, pctThresh=.21):
    rl = list()
    for v in missingcountD:
        if missingcountD[v] >= pctThresh:
            rl.append(v)
    return rl

def generate_binaryCol(df, col, new_col=None, checkfunc=None, verbose=True):
    """

    :param df:
    :param col:
    :param checkfunc:
    :param verbose:
    :return:
    """
    if new_col is None:
        pass

    if checkfunc is None:
        df[col] = np.zeros(len(df))
        df.loc[[not nv for nv in df["Finish_Tech"].isna()], "Finished"] = int(1)


def filterMissing(df, pctThresh=.20):
    mdic = countmissing(df)
    drops = generate_droplist(mdic, pctThresh=pctThresh)
    df.drop(columns=drops, inplace=True)
    return
def string_check(df):
    to_remove = list()
    for c in df.columns.tolist():
        if isinstance(df[c].values.tolist()[0], type('_ESRD')):
            print('"{}",'.format(c))
            to_remove.append(c)
    return to_remove

def remove_outliers3(df, threshold=3, vars_to_check=None, verbose=True):
    no = df.shape[0]
    z_scores = pd.DataFrame(np.abs(StandardScaler().fit_transform(df.filter(items=vars_to_check, axis=1))), columns=vars_to_check, index=df.index)
    mark_d = pd.DataFrame(df.copy())
    for v in z_scores.columns.tolist():
        print('V: {}'.format(v))
        tng =z_scores[v] > threshold
        print("bool vals: {}".format(tng.values))
        mark_d[v] = np.zeros(len(z_scores[v]))
        count = 0
        outcount = 0
        for bl in tng:
            if bl:
                mark_d.loc[count, v] = 1
                outcount += 1
            count += 1

    pct_loss_d = {}
    print('Out count: {}'.format(outcount))
    for v in vars_to_check:
        print('\n==================================')
        print('checking {}'.format(v))
        #print('the og head {}'.format(df[v].head()))
        ss = df.shape[0]
        print('before removal shape {}'.format(df.shape[0]))
        #dfv = pd.DataFrame()
        #dfv[v] = np.abs(stats.zscore(df[v]))
        #dfv[v] = np.abs((df[v] - df[v].mean())/df[v].std())
        #dfv.index = df.index
        #print('Zhead:\n', dfv.head())
        #print('number not nan ', dfv[v].head())
        #z = np.abs(stats.zscore(df[v]))
        #df = df.loc[dfv[v] <= threshold, :]
        #df = df.loc[z <= threshold, :]
        #df = df.loc[df[v] <= highr, :]
        df = df.loc[mark_d[v] != 1, :]
        mark_d = mark_d.loc[mark_d[v] != 1, :]
        nn= df.shape[0]
        print('Now it is {}'.format(df.shape[0]))
        print('a {} % loss'.format(((ss - nn)/ss)*100))
        pct_loss_d[v] =((ss - nn)/ss)*100
        print('---------------------------------\n')
    if verbose:
        nc = df.shape[0]
        loss = ((no - nc) / no) * 100
        print('11N: {}, cleaned: {}, {:.3f}% data loss'.format(no, nc, loss))

    pct_loss_d = sort_dict(pct_loss_d, reverse=True)
    for v in pct_loss_d:
        print('{} lost {} %'.format(v, pct_loss_d[v]))

    return df


