import pandas as pd

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
        

def calc_option_TCA_metrics(df):
    # Returns a dataframe of TCA metrics for making and taking algos separately
    # This **only** handles df's representing a single underlying with a single trade side

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

    # Add a column to df representing the fill price delta-adjusted back to arrival_ul_mid
    # This could be improved by incorporating a gamma adjustment
    arrival_ul_mid = (df.loc[df.index[0], 'parentUBid'] + df.loc[df.index[0], 'parentUAsk']) / 2
    delta = df[df['fillQuantity']>0]['fillDe'][0] # This implicitly assumes all our executions are in the same contract
    df['fillUMid'] = (df['fillUBid'] + df['fillUBid'])/2
    df['fillDAdjPrice'] = df['fillPrice'] - delta * (df['fillUMid'] - arrival_ul_mid)
    df['fillDAdjBid'] = df['fillBid'] - delta * (df['fillUMid'] - arrival_ul_mid)
    df['fillDAdjAsk'] = df['fillAsk'] - delta * (df['fillUMid'] - arrival_ul_mid)
    df['fillDAdjMark'] = df['fillMark'] - delta * (df['fillUMid'] - arrival_ul_mid)

    # Add a column to df converting the d-adj fill to a vol
    arrival_mid = (df.loc[df.index[0], 'parentBid'] + df.loc[df.index[0], 'parentAsk']) / 2
    arrival_mark = df.loc[df.index[0], 'parentMark']
    first_fill_vol = df[df['fillQuantity'] > 0]['fillVol'][0]
    first_fill_adj_px = df[df['fillQuantity'] > 0]['fillDAdjPrice'][0]
    vega = df[df['fillQuantity']>0]['fillVe'][0]  # Again, not true if we have multiple contracts in the same order
    arrival_mid_vol = first_fill_vol + (arrival_mid - first_fill_adj_px) / (100 * vega)
    arrival_mark_vol = first_fill_vol + (arrival_mark - first_fill_adj_px) / (100 * vega)
    df['fillCalcVol'] = arrival_mid_vol + (df['fillDAdjPrice'] - arrival_mid) / (100 * vega)
    df['fillCalcVolBid'] = arrival_mid_vol + (df['fillDAdjBid'] - arrival_mid) / (100 * vega)
    df['fillCalcVolAsk'] = arrival_mid_vol + (df['fillDAdjAsk'] - arrival_mid) / (100 * vega)
    df['fillCalcVolMark'] = arrival_mid_vol + (df['fillDAdjMark'] - arrival_mid) / (100 * vega)
    fills_df = df[df['fillQuantity'] > 0]

    # Prepare to populate TCA results
    cols = ['Maker', 'Taker', 'Total', 'Desc']
    results = pd.DataFrame(columns= cols)
    make_df = df[df['childMakerTaker']=='Maker']
    take_df = df[df['childMakerTaker'] == 'Taker']

    if df.loc[df.index[0], 'orderSide'] == 'Buy':
        side = 1
    else:
        side = -1

    def populate_rows(df, col):
        results.loc['Child Orders', col] = df['clOrdId'].unique().shape[0]
        results.loc['Avg Child Size', col] = df.groupby('clOrdId').first()['childSize'].sum() / results.loc['Child Orders', col]
        results.loc['Filled Contracts', col] = df['fillQuantity'].sum()
        results.loc['Contract Fill Rate', col] = results.loc['Filled Contracts', col] / \
                                                     (results.loc['Avg Child Size', col] * results.loc['Child Orders', col])
        results.loc['Px Range', col] = df['fillPrice'].max() - df['fillPrice'].min()
        results.loc['Exec Px', col] = (df['fillPrice'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid Px', col] = side * (arrival_mid - results.loc['Exec Px', col])
        results.loc['Arr Slip Mid USD', col] = results.loc['Arr Slip Mid Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Arr Slip Mark Px', col] = side * (arrival_mark - results.loc['Exec Px', col])
        results.loc['Arr Slip Mark USD', col] = results.loc['Arr Slip Mark Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Eq Weighted Mid Px', col] = ((fills_df['fillBid'] + fills_df['fillAsk'])/2).mean()
        # This is a *VERY* poor proxy for VWAP since it's sampled on fills.  I need to get access to historical time&sales data ...
        results.loc['Slip to EqW Mid Px', col] = side * (results.loc['Eq Weighted Mid Px', col] - results.loc['Exec Px', col])
        results.loc['Slip to EqW Mid USD', col] = results.loc['Slip to EqW Mid Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Eq Weighted Mark Px', col] = fills_df['fillMark'].mean()
        results.loc['Slip to EqW Mark Px', col] = side * (results.loc['Eq Weighted Mark Px', col] - results.loc['Exec Px', col])
        results.loc['Slip to EqW Mark USD', col] = results.loc['Slip to EqW Mark Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['DAdj Px Range', col] = fills_df['fillDAdjPrice'].max() - fills_df['fillDAdjPrice'].min()
        results.loc['Exec DAdj Px', col] = (df['fillDAdjPrice'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid DAdj Px', col] = side * (arrival_mid - results.loc['Exec DAdj Px', col])
        results.loc['Arr Slip Mid DAdj USD', col] = side * results.loc['Arr Slip Mid DAdj Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Arr Slip Mark DAdj Px', col] = side * (arrival_mark - results.loc['Exec DAdj Px', col])
        results.loc['Arr Slip Mark DAdj USD', col] = side * results.loc['Arr Slip Mark DAdj Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Eq Weighted Mid DAdj Px', col] = ((fills_df['fillDAdjBid'] + fills_df['fillDAdjAsk'])/2).mean()
        results.loc['Slip to EqW Mid DAdj Px', col] = side * (results.loc['Eq Weighted Mid DAdj Px', col] - results.loc['Exec DAdj Px', col])
        results.loc['Slip to EqW Mid DAdj USD', col] = results.loc['Slip to EqW Mid DAdj Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Eq Weighted Mark DAdj Px', col] = fills_df['fillDAdjMark'].mean()
        results.loc['Slip to EqW Mark DAdj Px', col] = side * (results.loc['Eq Weighted Mark DAdj Px', col] - results.loc['Exec DAdj Px', col])
        results.loc['Slip to EqW Mark DAdj USD', col] = results.loc['Slip to EqW Mark DAdj Px', col] * results.loc['Filled Contracts', col] * 100
        results.loc['Vol Range', col] = fills_df['fillCalcVol'].max() - fills_df['fillCalcVol'].min()
        results.loc['Exec Vol', col] = (df['fillCalcVol'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid Vol', col] = side * (arrival_mid_vol - results.loc['Exec Vol', col])
        results.loc['Arr Slip Mid Vol USD', col] = results.loc['Arr Slip Mid Vol', col] * vega * results.loc['Filled Contracts', col] * 10000
        # Vol USD fields will differ slightly from DAdjUSD due to gamma if using SR Vols but will exactly match with CalcVol
        results.loc['Arr Slip Mark Vol', col] = side * (arrival_mark_vol - results.loc['Exec Vol', col])
        results.loc['Arr Slip Mark Vol USD', col] = results.loc['Arr Slip Mark Vol', col] * vega * results.loc['Filled Contracts', col] * 10000
        results.loc['Eq Weighted Mid Vol', col] = ((fills_df['fillCalcVolBid'] + fills_df['fillCalcVolAsk'])/2).mean()
        results.loc['Slip to EqW Mid Vol', col] = side * (results.loc['Eq Weighted Mid Vol', col] - results.loc['Exec Vol', col])
        results.loc['Slip to EqW Mid Vol USD', col] = results.loc['Slip to EqW Mid Vol', col] * vega * results.loc['Filled Contracts', col] * 10000
        results.loc['Eq Weighted Mark Vol', col] = fills_df['fillCalcVolMark'].mean()
        results.loc['Slip to EqW Mark Vol', col] = side * (results.loc['Eq Weighted Mark Vol', col] - results.loc['Exec Vol', col])
        results.loc['Slip to EqW Mark Vol USD', col] = results.loc['Slip to EqW Mark Vol', col] * vega * results.loc['Filled Contracts', col] * 10000


        results.loc['Fill Pct Spread', col] = ((fills_df['fillPrice'] - fills_df['fillBid']) / (fills_df['fillAsk'] - fills_df['fillBid'])
                                              * fills_df['fillQuantity']).sum() / fills_df['fillQuantity'].sum()

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
    results = calc_option_TCA_metrics(execs)