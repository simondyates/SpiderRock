import pandas as pd

def convert_time_cols(df):
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

def filter_cols(df):
    # msgsprdparentexecution has 244 columns.  Let's filter to just the ones we need
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
    # Deal with apparent float precision issues to return true penny prices
    round_cols = [col for col in df.columns if df[col].dtype=='float64']
    round_cols = [col for col in round_cols if ('Vol' not in col) and ('Prob' not in col) and ('Mark' not in col)]
    for col in round_cols:
        df[col] = df[col].apply(lambda x: round(x, 2))

def calc_dajd_fills(df):
    # Adds a column to df representing the fill price delta-adjusted back to arrival_ul_mid
    arrival_ul_mid = (df.loc[df.index[0], 'parentUBid'] + df.loc[df.index[0], 'parentUAsk']) / 2
    fills_df = df[df['fillQuantity'] > 0]
    delta = fills_df.loc[fills_df.index[0], 'fillDe']
    df['fillDAdjPrice'] = df['fillPrice'] - delta * (df['fillLimitRefUPrc'] - arrival_ul_mid)

def calc_option_TCA_metrics(df):
    rows = ['Child Orders', 'Avg Child Size', 'Filled Contracts', 'Contract Fill Rate',
            'Exec Px', 'Arr Slip Mid Px', 'Arr Slip Mid USD', 'Arr Slip Mark Px', 'Arr Slip Mark USD',
            'Exec DAdj Px', 'Arr Slip Mid DAdj Px', 'Arr Slip Mid DAdj USD', 'Arr Slip Mark DAdj Px', 'Arr Slip Mark DAdj USD',
            'Exec Vol', 'Arr Slip Mid Vol', 'Arr Slip Mark Vol',
            'Fill Pct Spread']
    cols = ['Maker', 'Taker', 'Total', 'Desc']
    results = pd.DataFrame(index=rows, columns= cols)
    make_df = df[df['childMakerTaker']=='Maker']
    take_df = df[df['childMakerTaker'] == 'Taker']

    if df.loc[df.index[0], 'orderSide'] == 'Buy':
        side = 1
    else:
        side = -1

    arrival_mid = (df.loc[df.index[0], 'parentBid'] + df.loc[df.index[0], 'parentAsk']) / 2
    arrival_mark = df.loc[df.index[0], 'parentMark']
    first_fill_vol = df[df['fillQuantity']>0]['fillVol'][0]
    first_fill_adj_px = df[df['fillQuantity']>0]['fillDAdjPrice'][0]
    vega = df[df['fillQuantity']>0]['fillVe'][0]
    arrival_mid_vol = first_fill_vol + (arrival_mid - first_fill_adj_px) / (100 * vega)
    arrival_mark_vol = first_fill_vol + (arrival_mark - first_fill_adj_px) / (100 * vega)

    def calc_fill_pctSpd(df):
        # Returns average fill as % of spread at arrival time.  0 = Bid, 0.5 = Mid, 1 = Ask
        fill_df = df[df['fillQuantity'] > 0]
        s = (fill_df['fillPrice'] - fill_df['fillBid']) / (fill_df['fillAsk'] - fill_df['fillBid']) * fill_df['fillQuantity']
        return s.sum() / fill_df['fillQuantity'].sum()

    def populate_rows(df, col):
        results.loc['Child Orders', col] = df['clOrdId'].unique().shape[0]
        results.loc['Avg Child Size', col] = df.groupby('clOrdId').first()['childSize'].sum() / results.loc['Child Orders', col]
        results.loc['Filled Contracts', col] = df['fillQuantity'].sum()
        results.loc['Contract Fill Rate', col] = results.loc['Filled Contracts', col] / \
                                                     (results.loc['Avg Child Size', col] * results.loc['Child Orders', col])
        results.loc['Exec Px', col] = (df['fillPrice'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid Px', col] = side * (arrival_mid - results.loc['Exec Px', col])
        results.loc['Arr Slip Mid USD', col] = side * results.loc['Arr Slip Mid Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Arr Slip Mark Px', col] = side * (arrival_mark - results.loc['Exec Px', col])
        results.loc['Arr Slip Mark USD', col] = side * results.loc['Arr Slip Mark Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Exec DAdj Px', col] = (df['fillDAdjPrice'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid DAdj Px', col] = side * (arrival_mid - results.loc['Exec DAdj Px', col])
        results.loc['Arr Slip Mid DAdj USD', col] = side * results.loc['Arr Slip Mid DAdj Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Arr Slip Mark DAdj Px', col] = side * (arrival_mark - results.loc['Exec DAdj Px', col])
        results.loc['Arr Slip Mark DAdj USD', col] = side * results.loc['Arr Slip Mark DAdj Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Exec Vol', col] = (df['fillVol'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid Vol', col] = side * (arrival_mid_vol - results.loc['Exec Vol', col])
        results.loc['Arr Slip Mark Vol', col] = side * (arrival_mark_vol - results.loc['Exec Vol', col])

        results.loc['Fill Pct Spread', col] = calc_fill_pctSpd(df)

    if make_df['childSize'].sum() > 0:
        populate_rows(make_df, 'Maker')
    else:
        results['Maker'] = 0

    if take_df['childSize'].sum() > 0:
        populate_rows(take_df, 'Taker')
    else:
        results['Taker'] = 0

    if df['childSize'].sum() > 0:
        populate_rows(df, 'Total')
    else:
        results['Total'] = 0

    # Add descriptions where appropriate
    results.loc['Child Orders', 'Desc'] = 'Number of child orders that resulted in a fill'
    results.loc['Exec Px', 'Desc'] = 'Ctr-weighted avg fill price'
    results.loc['Fill Pct Spread', 'Desc'] = 'Ctr-weighted avg of [fill vs spread] at fill time.  0 means on bid, 1 means on offer'
    # Fill empty descriptions
    results.loc[results['Desc'].isna(), 'Desc'] = ''
    return results

if __name__ == '__main__':
    execs = pd.read_csv('~/Dropbox/Element/Phase2/TradesJan25.csv')
    parents = execs['baseParentNumber'].unique()
    execs = execs[execs['baseParentNumber'] == parents[0]]
    convert_time_cols(execs)
    filter_cols(execs)
    round_price_cols(execs)
    calc_dajd_fills(execs)
    results = calc_option_TCA_metrics(execs)