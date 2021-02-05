import os
import pandas as pd
from SRUtils import process_time_cols, make_title
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as off
import plotly.io as pio
pio.renderers.default = 'browser'

def plot_fill_graph(df, save=True):
    """Generates a vizualization of a trade execution

    For stock trades, this produces a price chart showing bid/offer prices with
    arrows representing executions.  The sizing of the arrows is proportional to
    the trade size.  Overlayed on the chart is cumulative fill, and a lower chart
    shows spread width and underlying price during the time of execution.  The
    X axis starts at the time of order creation and runs until the final fill.

    Parameters
    ----------
    df : pandas.core.frame.DataFrame
        A dataframe generated from SRSE Trade's msgsrparentexecution table, filtered to represent a single underlying
    save: bool, optional
        Whether to save the html file to the TCA directory (default is True)

    Returns
    -------
    None (although it will display the graph in a browser)
    """

    df = df[df['fillQuantity'] > 0].copy()
    df['cumQuantity'] = df['fillQuantity'].cumsum()
    title = make_title(df)

    start = df.loc[df.index[0], 'parentDttm']
    arrival_bid = df.loc[df.index[0], 'parentBid']
    arrival_ask = df.loc[df.index[0], 'parentAsk']
    arrival_mid = (arrival_bid + arrival_ask) / 2
    arrival_mark = df.loc[df.index[0], 'parentMark']
    arrival_ul_mid = (df.loc[df.index[0], 'parentUBid'] + df.loc[df.index[0], 'parentUAsk']) / 2
    delta = df.loc[df.index[0], 'fillDe']
    vega = df.loc[df.index[0], 'fillVe']

    # If relevant, construct a delta-adjusted version of the data, referenced to arrival_ul_mid
    cols = ['fillTransactDttm', 'orderSide', 'fillBid', 'fillAsk',
            'fillMark', 'fillPrice', 'fillLimitRefUPrc', 'fillQuantity', 'cumQuantity']

    if delta != 0:
        df_adj = pd.DataFrame(index=df.index, columns=cols)
        for col in cols[2:6]:
            df_adj[col] = df[col] - delta * ((df['fillUBid'] + df['fillUAsk']) / 2 - arrival_ul_mid)

        for col in cols[:2] + cols[6:]:
            df_adj[col] = df[col]

        # Convert the delta-adjusted prices to vols
        first_trade_vol = df.loc[df.index[0], 'fillVol']
        first_trade_adj_px = df_adj.loc[df_adj.index[0], 'fillPrice']
        arrival_mid_vol = first_trade_vol + (arrival_mid - first_trade_adj_px) / (100 * vega)
        arrival_mark_vol = first_trade_vol + (arrival_mark - first_trade_adj_px) / (100 * vega)

        df_vol = pd.DataFrame(index = df.index, columns=cols)
        for col in cols[2:6]:
            df_vol[col] = arrival_mid_vol + (df_adj[col] - arrival_mid) / (100 * vega)

        for col in cols[:2] + cols[6:]:
            df_vol[col] = df[col]

        # df = df[cols]

    def generate_graph(df, pct_y=False):
        max_fill = df['fillQuantity'].max()
        scale_arrows = True

        fig = make_subplots(rows=2, cols=1, row_heights=[1, 0.3], vertical_spacing=0.02,
                            shared_xaxes=True, specs=[[{'secondary_y': True}],
                                                      [{'secondary_y': True}]])
        fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillBid'], name='Bid',
                                 line=dict(color='blue', width=1, dash='solid')), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillAsk'], name='Ask',
                                 line=dict(color='blue', width=1, dash='solid')), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillMark'], name='SR Mark',
                                 line=dict(color='magenta', width=1, dash='dot')), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['cumQuantity'], name='Cumulative Fills',
                                 line=dict(color='black', width=1, dash='dot')), secondary_y=True, row=1, col=1)
        fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillAsk'] - df['fillBid'], name='Spread',
                                 line=dict(color='royalblue', width=1, dash='solid')), row=2, col=1)
        fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillLimitRefUPrc'], name='Underlier',
                                 line=dict(color='black', width=1, dash='dot')), secondary_y=True, row=2, col=1)

        for t in df.index:
            if df.loc[t, 'orderSide'] == 'Buy':
                fig.add_annotation(dict(
                    x=df.loc[t, 'fillTransactDttm'],
                    y=df.loc[t, 'fillPrice'],
                    ax=0,
                    ay=(50 * df.loc[t, 'fillQuantity'] / max_fill) * scale_arrows + 25 * (1 - scale_arrows),
                    xref='x',
                    yref='y',
                    showarrow=True,
                    arrowcolor='green',
                    arrowsize=2,
                    arrowwidth=1,
                    arrowhead=1
                ))
            else:
                fig.add_annotation(dict(
                    x=df.loc[t, 'fillTransactDttm'],
                    y=df.loc[t, 'fillPrice'],
                    ax=0,
                    ay=-(50 * df.loc[t, 'fillQuantity'] / max_fill) * scale_arrows - 25 * (1 - scale_arrows),
                    xref='x',
                    yref='y',
                    showarrow=True,
                    arrowcolor='red',
                    arrowsize=2,
                    arrowwidth=1,
                    arrowhead=1
            ))
        fig.update_xaxes(range=[start, df.loc[df.index[-1], 'fillTransactDttm']])
        if pct_y:
            fig.update_yaxes(title='Vol', tickformat='.2%', secondary_y=False, row=1, col=1)
            fig.update_yaxes(title='Bid/Ask Spread', tickformat='.2%', secondary_y=False, row=2, col=1)
        else:
            fig.update_yaxes(title='Price', tickformat='.2f', secondary_y=False, row=1, col=1)
            fig.update_yaxes(title='Bid/Ask Spread', tickformat='.2f', secondary_y=True, row=2, col=1)
        fig.update_yaxes(title='Cumulative Fills', tickformat=',.0f', showgrid=False, secondary_y=True, row=1, col=1)
        if df['fillLimitRefUPrc'].iloc[0] > 1000:
            fig.update_yaxes(title='Underlier Price', tickformat=',.0f', showgrid=False, secondary_y=True, row=2, col=1)
        else:
            fig.update_yaxes(title='Underlier Price', tickformat=',.2f', showgrid=False, secondary_y=True, row=2, col=1)
        fig.update_layout(title=title, height=1000, width=1000)
        if save:
            off.plot(fig, filename=os.path.join(os.getcwd(), 'TCA', f'{title}.html'))
        else:
            fig.show()

    if delta != 0:
        generate_graph(df_vol, True)
    else:
        generate_graph(df, False)

if __name__ == '__main__':
    df = pd.read_csv(os.path.join(os.getcwd(), 'FillData', 'Trades20210122.csv'))
    process_time_cols(df)
    parents = df['baseParentNumber'].unique()
    df = df[df['baseParentNumber'] == parents[0]]
    plot_fill_graph(df, True)
