import pandas as pd
import os
from SRUtils import process_time_cols, make_title
import plotly.express as px

import plotly.io as pio
pio.renderers.default = 'browser'

def plot_fill_bar(df, timeDelta='5min', save=False):
    startTime = df.loc[df.index[0], 'parentDttm']
    bucketFills = df.groupby(pd.Grouper(key='fillDttm', freq=timeDelta, origin=startTime))['fillQuantity'].sum()
    title = make_title(df)
    fig = px.bar(bucketFills,
                 labels={
                     "fillDttm": "Bucket Start",
                     "value": "Fill Quantity"
                 },
                 title=title + f', Bucketed Every {timeDelta}')
    fig.update_xaxes(tickvals=bucketFills.index, tickformat='%H:%M')
    fig.layout.update(showlegend=False)
    fig.show()
    return bucketFills

if __name__ == '__main__':
    df = pd.read_csv(os.path.join(os.getcwd(), 'FillData', 'Trades20210125.csv'))
    process_time_cols(df)
    parents = df['baseParentNumber'].unique()
    df = df[df['baseParentNumber'] == parents[0]]
    result = plot_fill_bar(df)