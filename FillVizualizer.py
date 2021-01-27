# TO DO: Make this into a single function

import pandas as pd
from SRUtils import process_time_cols, make_title
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.offline as off
import plotly.io as pio
pio.renderers.default = 'browser'

df = pd.read_csv('./FillData/Trades20210125.csv')
df = df[df['fillQuantity'] > 0]

parents = df['baseParentNumber'].unique()
df = df[df['baseParentNumber'] == parents[0]]

# Convert Date Cols and add in microsecond cols
process_time_cols(df)
title = make_title(df)

start = df.loc[df.index[0], 'parentDttm']
arrival_bid = df.loc[df.index[0], 'parentBid']
arrival_ask = df.loc[df.index[0], 'parentAsk']
arrival_mid = (arrival_bid + arrival_ask) / 2
arrival_mark = df.loc[df.index[0], 'parentMark']
arrival_ul_mid = (df.loc[df.index[0], 'parentUBid'] + df.loc[df.index[0], 'parentUAsk']) / 2
delta = df.loc[df.index[0], 'fillDe']
vega = df.loc[df.index[0], 'fillVe']

# Construct a delta-adjusted version of the data, referenced to arrival_ul_mid
cols = ['fillTransactDttm', 'orderSide', 'fillBid', 'fillAsk',
        'fillMark', 'fillPrice', 'fillLimitRefUPrc', 'fillQuantity']
df_adj = pd.DataFrame(index=df.index, columns=cols)

for col in cols[2:-2]:
    df_adj[col] = df[col] - delta * ((df['fillUBid'] + df['fillUAsk']) / 2 - arrival_ul_mid)

for col in cols[:2] + cols[-2:]:
    df_adj[col] = df[col]

# Convert the delta-adjusted prices to vols
first_trade_vol = df.loc[df.index[0], 'fillVol']
first_trade_adj_px = df_adj.loc[df_adj.index[0], 'fillPrice']

arrival_mid_vol = first_trade_vol + (arrival_mid - first_trade_adj_px) / (100 * vega)
arrival_mark_vol = first_trade_vol + (arrival_mark - first_trade_adj_px) / (100 * vega)

df_vol = pd.DataFrame(index = df.index, columns=cols)
for col in cols[2:-2]:
    df_vol[col] = arrival_mid_vol + (df_adj[col] - arrival_mid) / (100 * vega)

for col in cols[:2] + cols[-2:]:
    df_vol[col] = df[col]

df_vol[cols[-1]] = df[cols[-1]]

df = df[cols]
df['cumQuantity'] = df['fillQuantity'].cumsum()
df_adj['cumQuantity'] = df['cumQuantity']
df_vol['cumQuantity'] = df['cumQuantity']

def plot_fill_graph(df, pct_y=False):
    max_fill = df['fillQuantity'].max()
    scale_arrows = False

    fig = make_subplots(rows=2, cols=1, row_heights=[1, 0.3], vertical_spacing=0.02,
                        shared_xaxes=True, specs=[[{'secondary_y': True}],
                                                  [{'secondary_y': True}]])
    fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillBid'], name='Bid',
                             line=dict(color='royalblue', width=1, dash='solid')), row=1, col=1)
    fig.add_trace(go.Scatter(x=df['fillTransactDttm'], y=df['fillAsk'], name='Ask',
                             line=dict(color='green', width=1, dash='solid')), row=1, col=1)
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
    fig.update_yaxes(title='Underlier Price', tickformat=',.0f', showgrid=False, secondary_y=True, row=2, col=1)
    fig.update_layout(title=title, height=1000, width=1000)
    fig.show()
    off.plot(fig, filename=f'./TCA/{title}.html')

plot_fill_graph(df_adj, False)