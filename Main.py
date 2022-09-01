import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

import matplotlib.dates as mdates

import dash
from dash import html
from dash import dcc
import plotly.graph_objects as go
import plotly.express as px


def total_by_date(df):
    fig = go.Figure(data=go.Scatter(x=df['RECEIVED']
                                    , y=df['count_mail']))
    return fig


def main():
    raw_report_df = pd.read_csv(r'D:\Downloads\CWNZ01.EML_REPORT_20220624.csv', header=0)
    raw_report_df.drop_duplicates(['RECEIVED', 'SUBJECT', 'FROM', 'TO', 'NO_OF_ATTACHMENTS', 'BODY_CHARACTER_COUNT'],
                                  inplace=True)
    raw_report_df = raw_report_df[~raw_report_df.FILENAME.str.contains('neml', case=False)]

    raw_report_df['received_date'] = pd.to_datetime(raw_report_df.RECEIVED).dt.date
    raw_report_df['month'] = pd.to_datetime(raw_report_df.RECEIVED).dt.month

    print(raw_report_df.head(5))

    count_date = raw_report_df.groupby('received_date', as_index=False).agg(
        count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))

    app = dash.Dash()
    app.layout = html.Div(id='parent',
                          children=[
                              html.H1(id='H1', children='Checking Email Report', style={'textAlign': 'center',
                                                                                        'marginTop': 40,
                                                                                        'marginBottom': 40}),
                              dcc.Graph(id='line_plot', figure=total_by_date(raw_report_df))
                          ]
                          )

    app.run_server(debug=True)


if __name__ == '__main__':
    main()
