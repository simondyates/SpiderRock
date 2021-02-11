import pandas as pd
from SRUtils import process_time_cols, format_df, make_title, find_first_file
import os

def calc_TCA_metrics(df, qwap=None, qwapU=None, arrActSlipPct=None, formatted=True):
    """Returns a dataframe of TCA metrics for an option or stock order on SpiderRock

    The are three broad classes of TCA returned.  The first is raw stats on execution price vs. arrival
    and QWAP (quote-weighted average price).  The second uses theoretical delta-adjusted values. These are
    theoretical in the sense they assume the delta-hedge was executed at mid-market at the time of each option fill.
    The third takes an actual delta execution price and uses this in place of the theoretical one.

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        A dataframe generated from SRSE Trade's msgsrparentexecution table, filtered to represent a single underlying
    qwap : float, optional
        SR's estimated QWAP for the option, from msgsrparentbrkrstate (default is None)
    qwapU : float, optional
        SR's estimated QWAP for the option underlying, from msgsrparentbrkrstate (default is None)
    arrActSlipPct: float, optional
        The % difference between the hedge's average price and its mid at the time of first fill (default is None)
    formatted: bool, optional
        Whether the dataframe returned should be converted to fixed-width formatted strings (default is True)

    Returns
    -------
    pandas.core.frame.DataFrame
        A dataframe indexed by TCA stats, separating Making and Taking trades and providing field descriptions
    """

    # Build results df so as to put variable definitions at start
    cols = ['Maker', 'Taker', 'Total', 'Desc']
    comma = '{:>10,.0f}'
    price = '{:>10.2f}'
    pct0 = '{:>10.0%}'
    pct2 = '{:>10.2%}'
    rows_dict = {
        'Arrival Mid': (price, 'Mid at order creation'),
        'Arrival Mark': (price, 'SR Mark at order creation'),
        'Arrival U Mid': (price, 'Mid of underlying at order creation'),
        'Arrival Mid Vol': (pct2, 'Implied volatility of Arrival Mid at Arrival U Mid'),
        'Arrival Mark Vol': (pct2, 'Implied volatility of Arrival Mark at Arrival U Mid'),
        'Qwap': (price, 'SR-calculated Qwap (or Vwap for a stock only order)'),
        'Qwap U': (price, 'SR-calculated Qwap for underlying price'),
        'Qwap Vol': (pct2, 'Implied volatility of Qwap at Qwap U'),
        'Delta': (pct0, 'Option Contract Delta'),
        'Vega': (price, 'Option Contract Vega'),
        'Child Orders': (comma, 'Number of child orders which had fills'),
        'Avg Child Size': (comma, 'Avg size of child orders which had fills'),
        'Filled Ctr': (comma, 'Total number of contracts filled'),
        'Ctr Fill Rate': (pct0, 'Filled Contracts divided by total size sent by child orders which had fills'),
        'Avg Fill Pct Spread': (pct2, '0% means fill is on bid at fill time; 100% means offer'),
        'Exec Px': (price, 'Average filled price'),
        'Px Range': (price, 'High minus low fill price'),
        'Slip Arr Mid Px': (price, 'Amount by which Exec Px was more favorable than mid at order creation'),
        'Slip Arr Mid USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Slip Arr Mark Px': (price, 'Amount by which Exec Px was more favorable than SR mark at order creation'),
        'Slip Arr Mark USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Slip Qwap Px': (price, 'Amount by which Exec Px was more favorable than Qwap'),
        'Slip Qwap USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Theo U Mid': (price, 'Average underlying price if hedging mid-market each fill time'),
        'Exec DTheo Arr Mid Px': (price, 'Exec Px delta-adjusted from Theo U Mid to Arrival Mid'),
        'DTheo Px Range': (price, 'High minus low delta-adjusted fill price'),
        'DTheo Slip Arr Mid Px': (price, 'Amount by which Exec DTheo Arr Mid Px was more favorable than Arrival Mid'),
        'DTheo Slip Arr Mid USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'DTheo Slip Arr Mark Px': (price, 'Amount by which Exec DTheo Arr Mid Px was more favorable than Arrival Mark'),
        'DTheo Slip Arr Mark USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Exec DTheo Qwap Px': (price, 'Exec Px delta-adjusted from Theo U Mid to Qwap U'),
        'DTheo Slip Qwap Px': (price, 'Amount by which Exec DTheo Qwap Px was more favorable than Qwap'),
        'DTheo Slip Qwap USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Exec DTheo Vol': (pct2, 'Implied volatility of Exec DTheo Arr Mid Px at Arrival Mid'),
        'DTheo Vol Range': (pct2, 'High minus low vol'),
        'DTheo Slip Arr Mid Vol': (pct2, 'Implied volatility of DTheo Slip Arr Mid Px at Arrival Mid'),
        'DTheo Slip Arr Mark Vol': (pct2, 'Implied volatility of DTheo Slip Arr Mark Px at Arrival Mid'),
        'DTheo Slip Qwap Vol': (pct2, 'Implied volatility of DTheo Slip Qwap Px at Qwap U'),
        'Act U Mid': (price, 'Actual average underlying price from executed hedge'),
        'Exec DAct Arr Mid Px': (price, 'Exec Px delta-adjusted from Act U Mid to Arrival Mid'),
        'DAct Slip Arr Mid Px': (price, 'Amount by which Exec DAct Arr Mid Px was more favorable than Arrival Mid'),
        'DAct Slip Arr Mid USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'DAct Slip Arr Mark Px': (price, 'Amount by which Exec DAct Arr Mid Px was more favorable than Arrival Mark'),
        'DAct Slip Arr Mark USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Exec DAct Qwap Px': (price, 'Exec Px delta-adjusted from Act U Mid to Qwap U'),
        'DAct Slip Qwap Px': (price, 'Amount by which Exec DAct Qwap Px was more favorable than Qwap'),
        'DActSlip Qwap USD': (comma, 'Above  * contracts filled * contract multiplier'),
        'Exec DAct Vol': (pct2, 'Implied volatility of Exec DAct Arr Mid Px at Arrival Mid'),
        'DAct Slip Arr Mid Vol': (pct2, 'Implied volatility of DTheo Slip Arr Mid Px at Arrival Mid'),
        'DAct Slip Arr Mark Vol': (pct2, 'Implied volatility of DTheo Slip Arr Mark Px at Arrival Mid'),
        'DAct Slip Qwap Vol': (pct2, 'Implied volatility of DTheo Slip Qwap Px at Qwap U')}

    results = pd.DataFrame(index=rows_dict.keys(), columns=cols)

    # Restrict calculations to positive quantity fills only
    df = df[df['fillQuantity'] > 0].copy()

    # Populate Arrival Stats and Contract Details (incl side and mult)
    # Handle Generic Metrics
    arrivalMid = (df['parentBid'].iloc[0] + df['parentAsk'].iloc[0]) / 2
    # Save to results
    results.loc['Arrival Mid'] = arrivalMid

    if df['orderSide'].iloc[0] == 'Buy':
        side = 1
    else:
        side = -1
    if df['secType'].iloc[0] == 'Option':
        mult = 100
    else:
        mult = 1

    # Handle qwap-dependent Metrics
    if qwap is not None:
        results.loc['Qwap'] = qwap
        results.loc['Qwap U'] = qwapU

    # Handle delta-dependent Metrics and data
    delta = df['fillDe'].iloc[0]
    vega = df['fillVe'].iloc[0]
    if delta != 0:
        arrivalMark = df['parentMark'].iloc[0]
        arrivalUMid = (df['parentUBid'].iloc[0] + df['parentUAsk'].iloc[0]) / 2
        # Add delta-adjusted price column
        df['fillUMid'] = (df['fillUBid'] + df['fillUAsk']) / 2
        df['fillDPrice'] = df['fillPrice'] - delta * (df['fillUMid'] - arrivalUMid)
        # Calculate arrival vols
        firstFillVol = df['fillVol'].iloc[0]
        firstFillDPx = df['fillDPrice'].iloc[0]
        arrivalMidVol = firstFillVol + (arrivalMid - firstFillDPx) / (100 * vega)
        arrivalMarkVol = firstFillVol + (arrivalMark - firstFillDPx) / (100 * vega)
        results.loc['Delta'] = delta
        results.loc['Vega'] = vega
        results.loc['Arrival Mark'] = arrivalMark
        results.loc['Arrival U Mid'] = arrivalUMid
        results.loc['Arrival Mid Vol'] = arrivalMidVol
        results.loc['Arrival Mark Vol'] = arrivalMarkVol

        # Handle qwap- and delta-dependent Metrics
        if qwap is not None:
            qwapDPx = qwap - delta * (qwapU - arrivalUMid)
            qwapVol = arrivalMidVol + (qwapDPx - arrivalMid) / (100 * vega)
            results.loc['Qwap Vol'] = qwapVol

        # Handle arrActSlipPct and delta-dependent Metrics
        if arrActSlipPct is not None:
            actUMid = (df['fillUMid'].iloc[0]) * (1 + arrActSlipPct)
            # Note that this uses Mid at the time of first option fill, rather than order arrival,
            # since my stock returns are based off the time of the first stock fill
            # (which will follow the option fill)
            results.loc['Act U Mid'] = actUMid


    # Calculate Metrics that Depend on Make/Take Classification
    def populate_rows(sdf, col):
        # sdf - subdataframe - e.g. filtered for just Make or Take trades

        # Calc metrics that require none of (delta/vega, qwap, arrActSlipPct)
        childOrders = sdf['clOrdId'].unique().shape[0]
        avgChildSize = sdf.groupby('clOrdId').first()['childSize'].sum() / childOrders
        filledCtr = sdf['fillQuantity'].sum()
        ctrFillRate = filledCtr / (childOrders * avgChildSize)
        avgFillPctSpread = ((sdf['fillPrice'] - sdf['fillBid'])
                             / (sdf['fillAsk'] - sdf['fillBid'])
                             * sdf['fillQuantity']).sum() / filledCtr
        execPx = (sdf['fillPrice'] * sdf['fillQuantity']).sum() / filledCtr
        pxRange = sdf['fillPrice'].max() - sdf['fillPrice'].min()
        slipArrMidPx = side * (arrivalMid - execPx)
        slipArrMidUSD = slipArrMidPx * filledCtr * mult
        # Save to results
        results.loc['Child Orders', col] = childOrders
        results.loc['Avg Child Size', col] = avgChildSize
        results.loc['Filled Ctr', col] = filledCtr
        results.loc['Ctr Fill Rate', col] = ctrFillRate
        results.loc['Avg Fill Pct Spread', col] = avgFillPctSpread
        results.loc['Exec Px', col] = execPx
        results.loc['Px Range', col] = pxRange
        results.loc['Slip Arr Mid Px', col] = slipArrMidPx
        results.loc['Slip Arr Mid USD', col] = slipArrMidUSD

        # Calc metrics that require only qwap
        if qwap is not None:
            slipQwapPx = side * (qwap - execPx)
            slipQwapUSD = slipQwapPx * filledCtr * mult
            results.loc['Slip Qwap Px', col] = slipQwapPx
            results.loc['Slip Qwap USD', col] = slipQwapUSD

        # Calc metrics that require only delta/vega
        if delta != 0:
            slipArrMarkPx = side * (arrivalMark - execPx) # Doesn't use delta but mark is zero for non options
            slipArrMarkUSD = slipArrMarkPx * filledCtr * mult
            theoUMid = (sdf['fillUMid'] * sdf['fillQuantity']).sum() / filledCtr
            execDTheoArrMidPx = execPx - delta * (theoUMid - arrivalUMid)
            dTheoPxRange = sdf['fillDPrice'].max() - sdf['fillDPrice'].min()
            dTheoSlipArrMidPx = side * (arrivalMid - execDTheoArrMidPx)
            dTheoSlipArrMidUSD = dTheoSlipArrMidPx * filledCtr * mult
            dTheoSlipArrMarkPx = side * (arrivalMark - execDTheoArrMidPx)
            dTheoSlipArrMarkUSD = dTheoSlipArrMarkPx * filledCtr * mult
            execDTheoVol = arrivalMidVol + (execDTheoArrMidPx - arrivalMid) / (100 * vega)
            dTheoVolRange = dTheoPxRange / (100 * vega)
            dTheoSlipArrMidVol = dTheoSlipArrMidPx / (100 * vega)
            dTheoSlipArrMarkVol = dTheoSlipArrMarkPx / (100 * vega)
            # Save to results
            results.loc['Slip Arr Mark Px', col] = slipArrMarkPx
            results.loc['Slip Arr Mark USD', col] = slipArrMarkUSD
            results.loc['Theo U Mid', col] = theoUMid
            results.loc['Exec DTheo Arr Mid Px', col] = execDTheoArrMidPx
            results.loc['DTheo Px Range', col] = dTheoPxRange
            results.loc['DTheo Slip Arr Mid Px', col] = dTheoSlipArrMidPx
            results.loc['DTheo Slip Arr Mid USD', col] = dTheoSlipArrMidUSD
            results.loc['DTheo Slip Arr Mark Px', col] = dTheoSlipArrMarkPx
            results.loc['DTheo Slip Arr Mark USD', col] = dTheoSlipArrMarkUSD
            results.loc['Exec DTheo Vol', col] = execDTheoVol
            results.loc['DTheo Vol Range', col] = dTheoVolRange
            results.loc['DTheo Slip Arr Mid Vol', col] = dTheoSlipArrMidVol
            results.loc['DTheo Slip Arr Mark Vol', col] = dTheoSlipArrMarkVol

            # Calc metrics that require both delta/vega and qwap
            if qwap is not None:
                execDTheoQwapPx = execPx - delta * (theoUMid - qwapU)
                dTheoSlipQwapPx = side * (qwap - execDTheoQwapPx)
                dTheoSlipQwapUSD = dTheoSlipQwapPx * filledCtr * mult
                dTheoSlipQwapVol = dTheoSlipQwapPx / (100 * vega)
                # Save to results
                results.loc['Exec DTheo Qwap Px', col] = execDTheoQwapPx
                results.loc['DTheo Slip Qwap Px', col] = dTheoSlipQwapPx
                results.loc['DTheo Slip Qwap USD', col] = dTheoSlipQwapUSD
                results.loc['DTheo Slip Qwap Vol', col] = dTheoSlipQwapVol

            # Calc metrics that require delta/vega and arrActSlipPct
            if arrActSlipPct is not None:
                execDActArrMidPx = execPx - delta * (actUMid - arrivalUMid)
                dActSlipArrMidPx = side * (arrivalMid - execDActArrMidPx)
                dActSlipArrMidUSD = dActSlipArrMidPx * filledCtr * mult
                dActSlipArrMarkPx = side * (arrivalMark - execDActArrMidPx)
                dActSlipArrMarkUSD = dActSlipArrMarkPx * filledCtr * mult
                execDActVol = arrivalMidVol + (execDActArrMidPx - arrivalMid) / (100 * vega)
                dActSlipArrMidVol = dActSlipArrMidPx / (100 * vega)
                dActSlipArrMarkVol = dActSlipArrMarkPx / (100 * vega)
                # Save to results
                results.loc['Exec DAct Arr Mid Px', col] = execDActArrMidPx
                results.loc['DAct Slip Arr Mid Px', col] = dActSlipArrMidPx
                results.loc['DAct Slip Arr Mid USD', col] = dActSlipArrMidUSD
                results.loc['DAct Slip Arr Mark Px', col] = dActSlipArrMarkPx
                results.loc['DAct Slip Arr Mark USD', col] = dActSlipArrMarkUSD
                results.loc['Exec DAct Vol', col] = execDActVol
                results.loc['DAct Slip Arr Mid Vol', col] = dActSlipArrMidVol
                results.loc['DAct Slip Arr Mark Vol', col] = dActSlipArrMarkVol

                # Calc metrics that require delta/vega, arrActSlipPct and qwap
                if qwap is not None:
                    execDActQwapPx = execPx - delta * (actUMid - qwapU)
                    dActSlipQwapPx = side * (qwap - execDActQwapPx)
                    dActSlipQwapUSD = dActSlipQwapPx * filledCtr * mult
                    dActSlipQwapVol = dActSlipQwapPx / (100 * vega)
                    # Save to results
                    results.loc['Exec DAct Qwap Px', col] = execDActQwapPx
                    results.loc['DAct Slip Qwap Px', col] = dActSlipQwapPx
                    results.loc['DActSlip Qwap USD', col] = dActSlipQwapUSD
                    results.loc['DAct Slip Qwap Vol', col] = dActSlipQwapVol

    # Run populate_rows for makeDf / takeDf
    makeDf = df[df['childMakerTaker'] == 'Maker']
    takeDf = df[df['childMakerTaker'] == 'Taker']

    if makeDf['fillQuantity'].sum() > 0:
        populate_rows(makeDf, 'Maker')
    else:
        results['Maker'] = 0

    if takeDf['fillQuantity'].sum() > 0:
        populate_rows(takeDf, 'Taker')
    else:
        results['Taker'] = 0

    if df['fillQuantity'].sum() > 0:
        populate_rows(df, 'Total')
    else:
        results['Total'] = 0

    # Add descriptions
    for key in rows_dict.keys():
        results.loc[key, 'Desc'] = rows_dict[key][1]

    # Add formatting and return results
    if formatted:
        format_dict = {key: rows_dict[key][0] for key in rows_dict.keys()}
        results = format_df(results, format_dict)
    return results

def process_day_TCA(dt):
    """Calls calc_option_TCA_metrics for each trade ticket found for date dt

    The function will attempt to locate the relevant files for the day and determine the number
    of tickets from the number of unique packageIds.  It will identify hedge trades which share a
    baseParentNumber with option trades and process them for delta-neutral TCA.  It will attempt to
    find Qwap (for options) or Vwap (for stock only trades) data matching each baseParentNumber

    Parameters
    ----------
    dt : datetime.date (or anything richer than that)
            The trade date to process

    Returns
    -------
    int
            The number of baseParentNumbers processed
    """

    tradeFile = os.path.join(os.getcwd(), 'FillData', f'Trades{dt:%Y%m%d}.csv')
    dayFills = pd.read_csv(tradeFile)
    process_time_cols(dayFills)
    wins = 0
    if dayFills.shape[0] == 0:
        return wins

    for pkg in dayFills['packageId'].unique():
        parents = dayFills.loc[dayFills['packageId'] == pkg, 'baseParentNumber'].unique()
        opt_parents = [p for p in parents if dayFills.loc[dayFills['baseParentNumber'] == p, 'secType'].iloc[0] == 'Option']
        stock_parents = [p for p in parents if dayFills.loc[dayFills['baseParentNumber'] == p, 'secType'].iloc[0] == 'Stock']

        if len(opt_parents) > 0:
            # Look for a delta hedge execution
            if len(stock_parents) == 1: # I think it will be either 0 or 1
                hedges = dayFills[dayFills['baseParentNumber'] == stock_parents[0]]
                actUMid = (hedges.loc[hedges.index[0], 'parentBid'] + hedges.loc[hedges.index[0], 'parentAsk']) / 2
                fillU = (hedges['fillPrice'] * hedges['fillQuantity']).sum() / hedges['fillQuantity'].sum()
                arrActSlipPct = (fillU - actUMid) / actUMid
            else:
                arrActSlipPct = None
            for opt in opt_parents:
                # Look for qwap data matching opt
                brkr = find_first_file(dt)
                if brkr is not None:
                    brkr = brkr[brkr['baseParentNumber'] == opt]
                    if brkr.shape[0] > 0:
                        qwap = brkr.loc[brkr.index[0], 'brokerQwapMark']
                        qwapU = brkr.loc[brkr.index[0], 'brokerQwapUMark']
                else:
                    qwap = qwapU = None
                fills = dayFills[dayFills['baseParentNumber'] == opt]
                results = calc_TCA_metrics(fills, qwap, qwapU, arrActSlipPct)
                fName = make_title(fills) + '.csv'
                results.to_csv(os.path.join(os.getcwd(), 'TCA', fName))
                wins += 1

        if len(opt_parents) == 0 and len(stock_parents) > 0:
            for stock in stock_parents:
                # Look for Vwap data matching stock
                # For a pure stock order, Vwap is probably a better metric than Qwap
                qwap = qwapU = None
                brkr = find_first_file(dt)
                if brkr is not None:
                    brkr = brkr[brkr['baseParentNumber'] == stock]
                    if brkr.shape[0] > 0:
                        qwap = brkr.loc[brkr.index[0], 'brokerVwapMark']
                fills = dayFills[dayFills['baseParentNumber'] == stock]
                results = calc_TCA_metrics(fills, qwap)
                fName = make_title(fills) + '.csv'
                results.to_csv(os.path.join(os.getcwd(), 'TCA', fName))
                wins += 1

    return wins


if __name__ == '__main__':
    dt = pd.to_datetime('20210122')
    wins = process_day_TCA(dt)
