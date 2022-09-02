import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

import matplotlib.dates as mdates

from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px


def total_by_date(df_total, df_null):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df_total['received_date'],
                             y=df_total['count_mail'],
                             name='Total Mail'))

    fig.add_trace(go.Scatter(x=df_null['received_date'],
                             y=df_null['count_mail'],
                             name='Null Subject Mail',
                             mode='markers'))

    fig.update_layout(xaxis_title='Month',
                      yaxis_title='Total Mail')

    return fig


raw_report_df = pd.read_csv(r'D:\Downloads\CWNZ01.EML_REPORT_20220624.csv', header=0)
raw_report_df.drop_duplicates(['RECEIVED', 'SUBJECT', 'FROM', 'TO', 'NO_OF_ATTACHMENTS', 'BODY_CHARACTER_COUNT'],
                              inplace=True)
raw_report_df = raw_report_df[~raw_report_df.FILENAME.str.contains('neml', case=False)]

raw_report_df['received_date'] = pd.to_datetime(raw_report_df.RECEIVED).dt.date
raw_report_df['month'] = pd.to_datetime(raw_report_df.RECEIVED).dt.month

print(raw_report_df.head(5))

total_mail = raw_report_df.groupby('received_date', as_index=False).agg(
    count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))

null_subject = raw_report_df[~raw_report_df.SUBJECT.notna()].groupby('received_date', as_index=False).agg(
    count_mail=pd.NamedAgg(column='PATH', aggfunc='count')
)

vendor = raw_report_df[raw_report_df['FROM'].str.contains('loreal.com', case=False)]. \
    groupby('received_date', as_index=False).agg(count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))

app = Dash()
app.layout = html.Div(id='parent',
                      children=[
                          html.H1(id='H1', children='Checking Email Report',
                                  style={'textAlign': 'left',
                                         'marginTop': 40,
                                         'marginBottom': 40,
                                         'marginLeft': 40,
                                         'font-family': 'courier',
                                         }),
                          dcc.Graph(id='total_by_date', figure=total_by_date(total_mail, null_subject)),
                          html.H3(id='vendor-h3', children='Total Email of Top 10 Vendors',
                                  style={'font-family': 'courier',
                                         'marginTop': 20,
                                         'marginLeft': 40,
                                         'textAlign': 'left'
                                         }),
                          dcc.Dropdown(['Loreal', 'Blackmores'], 'Loreal', id='vendor-dropdown')
                      ]
                      )

if __name__ == '__main__':
    app.run_server(debug=True)
