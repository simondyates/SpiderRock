import pandas as pd
import os
import re

def filter_cols(df):
    # srtrade009.msgsprdparentexecution has 244 columns.  Let's filter to just the ones we need
    keep_cols = ['parentNumber', 'baseParentNumber', 'clOrdId', 'secKey_tk', 'secKey_yr',
            'secKey_mn', 'secKey_dy', 'secKey_xx', 'secKey_cp', 'secType',
            'orderSide', 'childSize', 'childPrice', 'childDttm', 'childMakerTaker',
            'childUBid', 'childUAsk', 'childBid', 'childAsk', 'childMark',
            'childVol', 'childProb', 'childMktStance', 'childMethod',
            'fillTransactDttm', 'fillExchFee', 'fillPrice', 'fillQuantity', 'fillBid',
            'fillAsk', 'fillMark', 'fillUMark', 'fillUBid', 'fillUAsk',
            'fillVolAtm', 'fillMark1M', 'fillMark10M', 'fillBid1M', 'fillAsk1M',
            'fillBid10M', 'fillAsk10M', 'fillUMark1M', 'fillUMark10M', 'fillVolAtm1M',
            'fillVolAtm10M', 'fillVol', 'fillProb', 'fillLimitRefUPrc', 'fillVe',
            'fillGa', 'fillDe', 'fillTh', 'parentDttm', 'parentUBid',
            'parentUAsk', 'parentUMark', 'parentBid', 'parentAsk', 'parentMark',
            'autoHedge']
    df.drop([c for c in df.columns if c not in keep_cols], axis=1, inplace=True)


def round_price_cols(df):
    # Deal with apparent float precision issues in SR prices to return true penny prices
    round_cols = [col for col in df.columns if df[col].dtype=='float64']
    round_cols = [col for col in round_cols if ('Vol' not in col) and ('Prob' not in col) and ('Mark' not in col)]
    for col in round_cols:
        df[col] = df[col].apply(lambda x: round(x, 2))


def process_time_cols(df):
    # df is a query from srtrade009.msgsrparentexecution
    # Convert SR's string time fields and convert to tz-aware Timestamps, adding in micros if available

    time_cols = [col for col in df.columns if ('Dttm' in col) and ('_us' not in col)]
    for col in time_cols:
        df[col] = df[col].apply(pd.to_datetime)
        df[col] = df[col].dt.tz_localize('America/Chicago').dt.tz_convert('America/New_York')
        try:
            s = df[col + '_us'].apply(pd.Timedelta, unit='micros')
            df[col] = df[col] + s
        except:
            pass


def format_df(df, format_dict, axis=0, drop_Nan=True):
    # Converts a dataframe with numeric types to all strings using supplied format codes
    # Format codes are applied along rows (default) or columns (axis=1)
    # Rows (or cols) with any Nan's are deleted unless drop_Nan = False
    out_df = pd.DataFrame(index=df.index, columns=df.columns)
    for row in df.index:
        for col in df.columns:
            if (type(df.loc[row, col]) is str) or (pd.isna(df.loc[row, col])):
                out_df.loc[row, col] = df.loc[row, col]
            else:
                if axis == 0:
                    out_df.loc[row, col] = format_dict[row].format(df.loc[row, col])
                else:
                    out_df.loc[row, col] = format_dict[col].format(df.loc[row, col])
    if drop_Nan:
        out_df = out_df[out_df.notna().all(axis=(1-axis))]
    return out_df


def make_title(df):
    # df is a query from srtrade009.msgsrparentexecution, filtered to a single execution
    # Returns a descriptive title for the order
    title1 = f"{df['orderSide'].iloc[0]} {df['fillQuantity'].sum()} {df['secKey_tk'].iloc[0]} "
    title2 = f"{df['secKey_yr'].iloc[0]}{df['secKey_mn'].iloc[0]:02}{df['secKey_dy'].iloc[0]} "
    strike = df['secKey_xx'].iloc[0]
    if strike == int(strike):
        title3 = f"{df['secKey_xx'].iloc[0]:.0f} "
    else:
        title3 = f"{df['secKey_xx'].iloc[0]:.2f} "
    title4 = f"{df['secKey_cp'].iloc[0]} "
    title = title1
    if df['secKey_mn'].iloc[0] > 0:
        title += title2 + title3 + title4
    return title


def find_first_file(dt, fStart='BrkrState'):
    """Returns a dataframe for the first file dated after dt starting with fStart

    The function is hard-coded to look in the directory FillData which is assumed to be
    in the current working directory.  Files are assumed to be of the format
    fStartyyyymmdd.csv

    Parameters
    ----------
    dt: datetime.date (or anything richer)
        Filter filenames to be equal to or after this date
    fStart: string, optional
        The initial text in the filename (default='BrkrState')

    Returns
    -------
    pandas.core.frame.DataFrame or None
        The result of applying pd.read_csv to the first filename, or None
        if no appropriate filename found
    """

    dirPath = os.path.join(os.getcwd(), 'FillData')
    contents = [f for f in os.listdir(dirPath) if fStart in f]
    dateStrs = [re.findall('\d{8}', c) for c in contents]
    dateStrs = [l[0] for l in dateStrs if len(l) > 0]
    dates = [pd.to_datetime(s) for s in dateStrs]
    validDates = sorted([d for d in dates if d >= dt])
    if len(validDates) == 0:
        return None
    else:
        firstFile = f'{fStart}{validDates[0]:%Y%m%d}.csv'
        return pd.read_csv(os.path.join(dirPath, firstFile))
