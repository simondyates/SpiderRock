# Really should refactor my variable names to make them more consistent

import pandas as pd
from collections import namedtuple
from SRUtils import process_time_cols, format_df, make_title


def calc_option_TCA_metrics(df, qwap_mark=None, qwap_Umark=None, actualUvsAvg=None, formatted=True):
    # Returns a dataframe of TCA metrics for making and taking algos separately
    # This **only** handles a df representing a single underlying with a single trade side
    # qwap_mark is for primary instrument and qwap_Umark is for underlying

    # Add a column to df representing the fill price delta-adjusted back to arrival_ul_mid
    # This could be improved by incorporating a gamma adjustment
    arrival_ul_mid = (df.loc[df.index[0], 'parentUBid'] + df.loc[df.index[0], 'parentUAsk']) / 2
    delta = df[df['fillQuantity'] > 0]['fillDe'].iloc[
        0]  # This implicitly assumes all our executions are in the same contract
    if delta != 0:
        df['fillUMid'] = (df['fillUBid'] + df['fillUAsk']) / 2
        df['fillDAdjPrice'] = df['fillPrice'] - delta * (df['fillUMid'] - arrival_ul_mid)
        df['fillDAdjBid'] = df['fillBid'] - delta * (df['fillUMid'] - arrival_ul_mid)
        df['fillDAdjAsk'] = df['fillAsk'] - delta * (df['fillUMid'] - arrival_ul_mid)
        df['fillDAdjMark'] = df['fillMark'] - delta * (df['fillUMid'] - arrival_ul_mid)

    # Add a column to df converting the d-adj fill to a vol
    arrival_mid = (df.loc[df.index[0], 'parentBid'] + df.loc[df.index[0], 'parentAsk']) / 2
    arrival_mark = df.loc[df.index[0], 'parentMark']
    if delta != 0:
        first_fill_vol = df[df['fillQuantity'] > 0]['fillVol'].iloc[0]
        first_fill_adj_px = df[df['fillQuantity'] > 0]['fillDAdjPrice'].iloc[0]
        vega = df[df['fillQuantity'] > 0]['fillVe'].iloc[
            0]  # Again, not true if we have multiple contracts in the same order
        arrival_mid_vol = first_fill_vol + (arrival_mid - first_fill_adj_px) / (100 * vega)
        arrival_mark_vol = first_fill_vol + (arrival_mark - first_fill_adj_px) / (100 * vega)
        df['fillCalcVol'] = arrival_mid_vol + (df['fillDAdjPrice'] - arrival_mid) / (100 * vega)
        df['fillCalcVolBid'] = arrival_mid_vol + (df['fillDAdjBid'] - arrival_mid) / (100 * vega)
        df['fillCalcVolAsk'] = arrival_mid_vol + (df['fillDAdjAsk'] - arrival_mid) / (100 * vega)
        df['fillCalcVolMark'] = arrival_mid_vol + (df['fillDAdjMark'] - arrival_mid) / (100 * vega)

    # Fudge the contract multiplier rather than looking it up properly
    if df['secType'].iloc[0] == 'Option':
        mult = 100
    else:
        mult = 1

    # Prepare to populate TCA results
    cols = ['Maker', 'Taker', 'Total', 'Desc']
    field = namedtuple('field', ('format', 'desc'))
    comma = '{:>10,.0f}'
    price = '{:>10.2f}'
    pct0 = '{:>10.0%}'
    pct2 = '{:>10.2%}'
    rows_dict = {'Child Orders': field(comma, 'Number of child orders which had fills'),
                 'Avg Child Size': field(comma, 'Avg size of child orders which had fills'),
                 'Filled Contracts': field(comma, 'Total number of contracts filled'),
                 'Contract Fill Rate': field(pct0,
                                             'Filled Contracts divided by total size sent by child orders which had fills'),
                 'Fill Pct Spread': field(pct2,
                                          'Quantity-weighted average of fill price in spread units at time of fill: 0 = on bid; 1 = on offer'),
                 'Arrival Mid': field(price, 'Mid at order creation'),
                 'Arrival Mark': field(price, 'SR Mark at order creation'),
                 'Arrival U/L Mid': field(price, 'Mid of underlying at order creation'),
                 'Exec Px': field(price, 'Average filled price'),
                 'Px Range': field(price, 'High minus low fill price'),
                 'Arr Slip Mid Px': field(price,
                                          'Amount by which Exec Px was more favorable than mid at order creation'),
                 'Arr Slip Mid USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Arr Slip Mark Px': field(price,
                                           'Amount by which Exec Px was more favorable than SR mark at order creation'),
                 'Arr Slip Mark USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'QWAP Px': field(price, 'SR-calculated QWAP'),
                 'QWAP U/L Px': field(price, 'SR-calculated QWAP for underlying price'),
                 'Slip to QWAP Px': field(price, 'Amount by which Exec Px was more favorable than the above price'),
                 'Slip to QWAP USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Exec DAdj Theo Arr Px': field(price,
                                            'Average of option fills delta-adjusted from fill-time mid back to arrival mid price'),
                 'DAdj Theo Arr Px Range': field(price, 'High minus low delta-adjusted fill price'),
                 'Theo U/L Px': field(price, 'The weighted average mid of underlying at fill times'),
                 'Arr Slip Mid DAdj Theo Px': field(price,
                                                    'Amount by which Exec DAdj Theo Px was more favorable than mid at order creation'),
                 'Arr Slip Mid DAdj Theo USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Arr Slip Mark DAdj Theo Px': field(price,
                                                     'Amount by which Exec DAdj Theo Px was more favorable than SR mark at order creation'),
                 'Arr Slip Mark DAdj Theo USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Exec DAdj Theo QWAP Px': field(price,
                                                 'Average of option fills delta-adjusted from fill-time mid back to QWAP u/l price'),
                 'Slip to Exec DAdj Theo QWAP Px': field(price,
                                                         'Amount by which Exec DAdj Theo QWAP Px was more favorable than the above price'),
                 'Slip to Exec DAdj Theo QWAP USD': field(comma,
                                                          'Above field * contracts filled * contract multiplier'),
                 'Exec Theo Vol': field(pct2, 'Implied volatility of Exec DAdj Theo Px at arrival mid price'),
                 'Vol Range': field(pct2,
                                    'All vol fields below correspond to the DAdj fields above, but expressed in vols'),
                 'Arr Slip Mid Theo Vol': field(pct2, ''),
                 # Commemting out the vol USD fields but leaving them here.  They should be exactly the same numbers as above
                 # So no need to display them - but they are a useful check.
                 #'Arr Slip Mid Theo Vol USD': field(comma, ''),
                 'Arr Slip Mark Theo Vol': field(pct2, ''),
                 #'Arr Slip Mark Theo Vol USD': field(comma, ''),
                 'QWAP Vol': field(pct2, ''),
                 'Theo Slip to QWAP Vol': field(pct2, ''),
                 #'Theo Slip to QWAP Vol USD': field(comma, ''),
                 'Exec DAdj Act Arr Px': field(price,
                                           'Average of option fills delta-adjusted using actual hedge fills back to arrival mid price'),
                 'Act U/L Px': field(price, 'Weighted average actual hedge price'),
                 'Arr Slip Mid DAdj Act Px': field(price,
                                                   'Amount by which Exec DAdj Act Px was more favorable than mid at order creation'),
                 'Arr Slip Mid DAdj Act USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Arr Slip Mark DAdj Act Px': field(price,
                                                    'Amount by which Exec DAdj Act Px was more favorable than SR mark at order creation'),
                 'Arr Slip Mark DAdj Act USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Exec DAdj Act QWAP Px': field(price, 'Average of option fills delta-adjusted from fill-time mid back to QWAP u/l price'),
                 'Slip to Exec DAdj Act QWAP Px': field(price,
                                                        'Amount by which Exec DAdj Act QWAP Px was more favorable than the above price'),
                 'Slip to Exec DAdj Act QWAP USD': field(comma, 'Above field * contracts filled * contract multiplier'),
                 'Exec Act Vol': field(pct2, 'Implied volatility of Exec DAdj Act Px at arrival mid price'),
                 'Arr Slip Mid Act Vol': field(pct2, ''),
                 #'Arr Slip Mid Act Vol USD': field(comma, ''),
                 'Arr Slip Mark Act Vol': field(pct2, ''),
                 #'Arr Slip Mark Act Vol USD': field(comma, ''),
                 'Act Slip to QWAP Vol': field(pct2, ''),
                 #'Act Slip to QWAP Vol USD': field(comma, '')
                 }

    results = pd.DataFrame(index=rows_dict.keys(), columns=cols)
    make_df = df[df['childMakerTaker'] == 'Maker']
    take_df = df[df['childMakerTaker'] == 'Taker']

    if df.loc[df.index[0], 'orderSide'] == 'Buy':
        side = 1
    else:
        side = -1

    def populate_rows(df, col):
        fills_df = df[df['fillQuantity'] > 0]
        # Overall stats
        results.loc['Child Orders', col] = df['clOrdId'].unique().shape[0]
        results.loc['Avg Child Size', col] = df.groupby('clOrdId').first()['childSize'].sum() / results.loc[
            'Child Orders', col]
        results.loc['Filled Contracts', col] = df['fillQuantity'].sum()
        results.loc['Contract Fill Rate', col] = results.loc['Filled Contracts', col] / \
                                                 (results.loc['Avg Child Size', col] * results.loc['Child Orders', col])
        results.loc['Fill Pct Spread', col] = ((fills_df['fillPrice'] - fills_df['fillBid']) / (
                fills_df['fillAsk'] - fills_df['fillBid'])
                                               * fills_df['fillQuantity']).sum() / fills_df['fillQuantity'].sum()
        results.loc['Arrival Mid', col] = arrival_mid
        results.loc['Arrival Mark', col] = arrival_mark
        results.loc['Arrival U/L Mid', col] = arrival_ul_mid
        # Basic arrival slippage
        results.loc['Px Range', col] = df['fillPrice'].max() - df['fillPrice'].min()
        results.loc['Exec Px', col] = (df['fillPrice'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
        results.loc['Arr Slip Mid Px', col] = side * (arrival_mid - results.loc['Exec Px', col])
        results.loc['Arr Slip Mid USD', col] = results.loc['Arr Slip Mid Px', col] * results.loc[
            'Filled Contracts', col] * mult
        if delta != 0:
            results.loc['Arr Slip Mark Px', col] = side * (arrival_mark - results.loc['Exec Px', col])
            results.loc['Arr Slip Mark USD', col] = results.loc['Arr Slip Mark Px', col] * results.loc[
                'Filled Contracts', col] * mult

        # QWAP slippage
        if qwap_mark is not None:
            results.loc['QWAP Px', col] = qwap_mark
            results.loc['QWAP U/L Px', col] = qwap_Umark
            results.loc['Slip to QWAP Px', col] = side * (results.loc['QWAP Px', col] - results.loc['Exec Px', col])
            results.loc['Slip to QWAP USD', col] = results.loc['Slip to QWAP Px', col] * results.loc[
                'Filled Contracts', col] * mult

        # Show delta-adjusted and vol versions of above
        if delta != 0:  # so vega != 0 either
            # Delta-adjusted Arrival
            results.loc['Exec DAdj Theo Arr Px', col] = (df['fillDAdjPrice'] * df['fillQuantity']).sum() / df[
                'fillQuantity'].sum()
            results.loc['DAdj Theo Arr Px Range', col] = fills_df['fillDAdjPrice'].max() - fills_df[
                'fillDAdjPrice'].min()
            fill_Umark = (df['fillUMid'] * df['fillQuantity']).sum() / df['fillQuantity'].sum()
            results.loc['Theo U/L Px', col] = fill_Umark
            results.loc['Arr Slip Mid DAdj Theo Px', col] = side * (arrival_mid - results.loc['Exec DAdj Theo Arr Px', col])
            results.loc['Arr Slip Mid DAdj Theo USD', col] = side * results.loc['Arr Slip Mid DAdj Theo Px', col] * \
                                                             results.loc['Filled Contracts', col] * mult
            results.loc['Arr Slip Mark DAdj Theo Px', col] = side * (
                        arrival_mark - results.loc['Exec DAdj Theo Arr Px', col])
            results.loc['Arr Slip Mark DAdj Theo USD', col] = side * results.loc['Arr Slip Mark DAdj Theo Px', col] * \
                                                              results.loc['Filled Contracts', col] * mult
            # Delta-adjusted QWAP
            results.loc['Exec DAdj Theo QWAP Px', col] = results.loc['Exec Px', col] - delta * (fill_Umark - qwap_Umark)
            results.loc['Slip to Exec DAdj Theo QWAP Px', col] = side * (
                        qwap_mark - results.loc['Exec DAdj Theo QWAP Px', col])
            results.loc['Slip to Exec DAdj Theo QWAP USD', col] = results.loc['Slip to Exec DAdj Theo QWAP Px', col] * \
                                                                  results.loc['Filled Contracts', col] * mult
            # Vol
            results.loc['Vol Range', col] = fills_df['fillCalcVol'].max() - fills_df['fillCalcVol'].min()
            results.loc['Exec Theo Vol', col] = (df['fillCalcVol'] * df['fillQuantity']).sum() / df[
                'fillQuantity'].sum()
            results.loc['Arr Slip Mid Theo Vol', col] = side * (arrival_mid_vol - results.loc['Exec Theo Vol', col])
            #results.loc['Arr Slip Mid Theo Vol USD', col] = results.loc['Arr Slip Mid Theo Vol', col] * vega * \
            #                                                results.loc['Filled Contracts', col] * 100 * mult
            # Vol USD fields will differ slightly from DAdjUSD due to gamma if using SR Vols but will exactly match with CalcVol
            results.loc['Arr Slip Mark Theo Vol', col] = side * (arrival_mark_vol - results.loc['Exec Theo Vol', col])
            #results.loc['Arr Slip Mark Theo Vol USD', col] = results.loc['Arr Slip Mark Theo Vol', col] * vega * \
            #                                                 results.loc['Filled Contracts', col] * 100 * mult
            results.loc['QWAP Vol', col] = arrival_mid_vol + (
                        qwap_mark - delta * (qwap_Umark - arrival_ul_mid) - arrival_mid) / (100 * vega)
            results.loc['Theo Slip to QWAP Vol', col] = side * (
                        results.loc['QWAP Vol', col] - results.loc['Exec Theo Vol', col])
            #results.loc['Theo Slip to QWAP Vol USD', col] = results.loc['Theo Slip to QWAP Vol', col] * vega * \
            #                                                results.loc['Filled Contracts', col] * 100 * mult

        if actualUvsAvg is not None:  # guarantees delta, vega != 0 as well
            # Calculate values delta-adjusted using actual execution level
            actual_ul = (fills_df.loc[fills_df.index[0], 'fillUBid'] + fills_df.loc[fills_df.index[0], 'fillUBid']) * (1 + actualUvsAvg) / 2
            # using the u/l at the time of first option fill for proxy of u/l at first stock fill
            # Delta-adjusted Arrival
            results.loc['Exec DAdj Act Arr Px', col] = results.loc['Exec Px', col] - delta * (actual_ul - arrival_ul_mid)
            results.loc['Act U/L Px', col] = actual_ul
            results.loc['Arr Slip Mid DAdj Act Px', col] = side * (arrival_mid - results.loc['Exec DAdj Act Arr Px', col])
            results.loc['Arr Slip Mid DAdj Act USD', col] = side * results.loc['Arr Slip Mid DAdj Act Px', col] * \
                                                            results.loc['Filled Contracts', col] * mult
            results.loc['Arr Slip Mark DAdj Act Px', col] = side * (arrival_mark - results.loc['Exec DAdj Act Arr Px', col])
            results.loc['Arr Slip Mark DAdj Act USD', col] = side * results.loc['Arr Slip Mark DAdj Act Px', col] * \
                                                             results.loc['Filled Contracts', col] * mult
            # Delta-adjusted QWAP
            results.loc['Exec DAdj Act QWAP Px', col] = results.loc['Exec Px', col] - delta * (actual_ul - qwap_Umark)
            results.loc['Slip to Exec DAdj Act QWAP Px', col] = side * (
                        qwap_mark - results.loc['Exec DAdj Act QWAP Px', col])
            results.loc['Slip to Exec DAdj Act QWAP USD', col] = results.loc['Slip to Exec DAdj Act QWAP Px', col] * \
                                                                 results.loc['Filled Contracts', col] * mult
            # Vol
            results.loc['Exec Act Vol', col] = arrival_mid_vol + (
                        results.loc['Exec DAdj Act Arr Px', col] - arrival_mid) / (100 * vega)
            results.loc['Arr Slip Mid Act Vol', col] = side * (arrival_mid_vol - results.loc['Exec Act Vol', col])
            #results.loc['Arr Slip Mid Act Vol USD', col] = results.loc['Arr Slip Mid Act Vol', col] * vega * \
            #                                               results.loc['Filled Contracts', col] * 100 * mult
            results.loc['Arr Slip Mark Act Vol', col] = side * (arrival_mark_vol - results.loc['Exec Act Vol', col])
            #results.loc['Arr Slip Mark Act Vol USD', col] = results.loc['Arr Slip Mark Act Vol', col] * vega * \
            #                                                results.loc['Filled Contracts', col] * 100 * mult
            results.loc['Act Slip to QWAP Vol', col] = side * (
                        results.loc['QWAP Vol', col] - results.loc['Exec Act Vol', col])
            #results.loc['Act Slip to QWAP Vol USD', col] = results.loc['Act Slip to QWAP Vol', col] * vega * \
             #                                              results.loc['Filled Contracts', col] * 100 * mult

    if make_df['fillQuantity'].sum() > 0:
        populate_rows(make_df, 'Maker')
    else:
        results['Maker'] = 0

    if take_df['fillQuantity'].sum() > 0:
        populate_rows(take_df, 'Taker')
    else:
        results['Taker'] = 0

    if df['fillQuantity'].sum() > 0:
        populate_rows(df, 'Total')
    else:
        results['Total'] = 0

    # Add descriptions
    for key in rows_dict.keys():
        results.loc[key, 'Desc'] = rows_dict[key].desc

    # Add formatting
    if formatted:
        format_dict = {key: rows_dict[key].format for key in rows_dict.keys()}
        results = format_df(results, format_dict)
    return results


if __name__ == '__main__':
    df = pd.read_csv('./FillData/Trades20210125.csv')
    process_time_cols(df)
    parents = df['baseParentNumber'].unique()
    execs = df[df['baseParentNumber'] == parents[0]].copy()
    # Calculate actual price for delta-hedge
    if len(parents) > 1:
        hedges = df[df['baseParentNumber'] == parents[1]]
        arrival_U = (hedges.loc[hedges.index[0], 'parentBid'] + hedges.loc[hedges.index[0], 'parentAsk']) / 2
        fill_U = (hedges['fillPrice'] * hedges['fillQuantity']).sum() / hedges['fillQuantity'].sum()
        actualUvsAvg = (fill_U - arrival_U) / arrival_U
    else:
        actualUvsAvg = None

    brkr = pd.read_csv('./FillData/BrkrState20210126.csv')
    brkr = brkr[brkr['baseParentNumber'] == parents[0]]
    if brkr.shape[0] > 0:
        qwap_mark = brkr.loc[brkr.index[0], 'brokerQwapMark']
        qwap_Umark = brkr.loc[brkr.index[0], 'brokerQwapUMark']
    else:
        qwap_mark = qwap_Umark = None
    results = calc_option_TCA_metrics(execs, qwap_mark, qwap_Umark, actualUvsAvg)
    # Save to csv
    title = make_title(execs)
    results.to_csv(f'./TCA/{title}.csv')
