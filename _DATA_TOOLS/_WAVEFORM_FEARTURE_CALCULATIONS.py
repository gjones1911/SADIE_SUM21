import pandas as pd
import numpy as np

def calculate_rms(df, col=None, rename=False):
    if col is None:
        col = df.columns.tolist()
    if rename:
        rdD = {c: c + "_RMS" for c in col}
        return pd.DataFrame({c:[x] for x, c in zip(np.sqrt(np.mean(df[col].values**2, axis=0)), col)}).rename(rdD)
    return pd.DataFrame({c:[x] for x, c in zip(np.sqrt(np.mean(df[col].values**2, axis=0)), col)})

def highFrequencyRMS(df, cols=None):
    if cols is None:
        col = df.columns.tolist()

def derived_peak(df, cols=None):
    if cols is None:
        cols = df.columns.tolist()
    rms = calculate_rms(df, cols, rename=False)
    #print("rms: {}".format(rms))
    #rdD = {c:c+"_derived_PK" for c in cols}
    for c in rms:
        rms[c] = rms[c] *np.sqrt(2)
        #print(rms[c])
    return rms

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
    #print(cols)
    min_df = get_minDF(df[cols])
    #print(min_df)
    max_df = get_maxDF(df[cols])
    #print(max_df)
    return max_df - min_df

def BCalculateCrestFactor(df, cols):
    rms = calculate_rms(df, cols)
    peak = np.abs(df[cols].values).max(axis=0)
    try:
        #print("peak:")
        #display(peak)
        pass
    except:
        pass
        #print("Peak:\n{}".format(peak))

    #print("Peak:\n{}\n,rms:\n {}\n".format(peak, rms))
    crestfactor = peak/rms
    #print('cF:\n{}\n'.format(crestfactor))
    crest_factor = {c+"_CF":[crestfactor[c][0]] for c in crestfactor}
    return pd.DataFrame(crest_factor)