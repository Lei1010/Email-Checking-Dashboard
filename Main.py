import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

import matplotlib.dates as mdates

from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px

top_vendors = ['Loreal', 'JOHNSON & JOHNSON', 'BRAND FOILO', 'DIPLOMAT NEW ZEALAND', 'BEIERSDORF', 'MCPHERSONS',
               'REAL VITAMINS', 'SANOFI', 'AFT PHARMACEUTICALS', 'NESTLE', 'BLACKMORES']

top_vendor_emails = ['Loreal.com', 'Mcphersons.co']
top_CM_emails = ['adil.khan@chemistwarehouse.co.nz', 'akshay.gawli@chemistwarehouse.co.nz']

color = {}


def total_by_date(df_total):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df_total.received_date,
                             y=df_total.count_mail,
                             text=df_total.day_name
                             )
                  )

    fig.update_layout(xaxis_title='Month',
                      yaxis_title='Total Mail',
                      template='plotly_white',
                      hovermode='x unified')

    return fig


raw_report_df = pd.read_csv(r'D:\Downloads\CWNZ01.EML_REPORT_20220624.csv', header=0)
# raw_report_df = pd.read_csv(r'EML_REPORT_20220621.csv', header=0)
raw_report_df.drop_duplicates(['RECEIVED', 'SUBJECT', 'FROM', 'TO', 'NO_OF_ATTACHMENTS', 'BODY_CHARACTER_COUNT'],
                              inplace=True)
raw_report_df = raw_report_df[~raw_report_df.FILENAME.str.contains('neml', case=False)]

raw_report_df['received_date'] = pd.to_datetime(raw_report_df.RECEIVED).dt.date

# print(raw_report_df.head(5))

total_mail_df = raw_report_df.groupby('received_date', as_index=False).agg(
    count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))

total_mail_df['day_name'] = pd.to_datetime(total_mail_df.received_date).dt.day_name()
total_mail = len(raw_report_df)
total_null = len(raw_report_df[~raw_report_df.SUBJECT.notna()])
percent_null = round(total_null / total_mail, 4) * 100
total_no_sender = len(raw_report_df[~raw_report_df['TO'].notna()])
percent_no_sender = round(total_no_sender / total_mail, 4) * 100

# null_subject = raw_report_df[~raw_report_df.SUBJECT.notna()].groupby(['received_date'], as_index=False).agg(
#     count_mail=pd.NamedAgg(column='PATH', aggfunc='count')
# )

app = Dash(external_stylesheets=[dbc.themes.LUX])
app.title = "Checking Email Dashboard"

app.layout = html.Div(id='parent',
                      children=[
                          html.H1(id='H1', children='Checking Email Dashboard',
                                  style={'textAlign': 'left',
                                         'marginTop': 40,
                                         'marginBottom': 40,
                                         'marginLeft': 40,
                                         'font-family': 'courier',
                                         }),
                          dbc.Row(
                              [
                                  dbc.Col(
                                      html.Div(
                                          children="""Commented Line Here""",

                                      ),
                                      width=9,
                                  ),
                                  dbc.Col(width=2)

                              ],
                              justify='center'
                          ),
                          html.Br(),
                          dbc.Row(
                              [
                                  dbc.Col(
                                      dbc.Card(
                                          [
                                              html.H2(f"{total_mail}", className="card-title"),
                                              html.P("of Total Email", className="card-text"),
                                          ],
                                          body=True,
                                          color="light",
                                      )),
                                  dbc.Col(
                                      dbc.Card(
                                          [
                                              html.H2(f"{percent_null}%", className="card-title"),
                                              html.P("Emails with No Subject", className="card-text"),
                                          ],
                                          body=True,
                                          color="dark",
                                          inverse=True,
                                      )),
                                  dbc.Col(
                                      dbc.Card(
                                          [
                                              html.H2(f"{percent_no_sender}%", className="card-title"),
                                              html.P("Emails with No Sender", className="card-text"),
                                          ],
                                          body=True,
                                          color="primary",
                                          inverse=True,
                                      )),
                              ]
                          ),
                          html.Br(),
                          dcc.Graph(id='total_by_date', figure=total_by_date(total_mail_df)),
                          dbc.Row(
                              [
                                  dbc.Col(
                                      html.Div([html.H3(id='vendor-h3', children='Total Email of Top 10 Vendors',
                                                        style={'font-family': 'courier',
                                                               'marginTop': 20,
                                                               'marginLeft': 40,
                                                               'textAlign': 'left'
                                                               }),
                                                dcc.Dropdown(options=top_vendor_emails,
                                                             value=top_vendor_emails[0],
                                                             id='top-vendor-dropdown',
                                                             clearable=False),
                                                dcc.Graph(id='top-vendor-graph')
                                                ],
                                               ),

                                  ),
                                  dbc.Col(
                                      html.Div([html.H2(id='vendor-h3-c', children='Total Email of top 10 CMs',
                                                        style={'font-family': 'courier',
                                                               'marginTop': 20,
                                                               'marginLeft': 40,
                                                               'textAlign': 'left'
                                                               }
                                                        ),
                                                dcc.Dropdown(
                                                    options=top_CM_emails,
                                                    value=top_CM_emails[0],
                                                    id='top-cm-dropdown',
                                                    clearable=False
                                                ),
                                                dcc.Graph(id='top-cm-graph')
                                                ]
                                               ),
                                  )
                              ]
                          )
                      ],
                      style={
                          'marginLeft': 20,
                          'marginRight': 20,
                      }
                      )


@app.callback(
    Output("top-vendor-graph", "figure"),
    [Input("top-vendor-dropdown", "value")],
)
def top_10_vendors(value):
    fig = go.Figure()
    vendor = raw_report_df[raw_report_df['FROM'].str.contains(value, case=False)]. \
        groupby('received_date', as_index=False).agg(count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))
    fig.add_trace(go.Bar(x=vendor.received_date,
                         y=vendor.count_mail
                         )
                  )
    fig.update_layout(xaxis_title='Month',
                      yaxis_title='Total Mail',
                      hovermode='x unified',
                      template='plotly_white',
                      title=value,
                      )
    return fig


@app.callback(
    Output("top-cm-graph", "figure"),
    [Input("top-cm-dropdown", "value")]
)
def top_10_cm(value):
    cm = raw_report_df[raw_report_df['FROM'].str.contains(value, case=False)]. \
        groupby('received_date', as_index=False).agg(count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))
    fig = go.Figure()
    fig.add_trace(go.Bar(x=cm.received_date,
                         y=cm.count_mail
                         )
                  )
    fig.update_layout(xaxis_title='Month',
                      yaxis_title='Total Mail',
                      hovermode='x unified',
                      template='plotly_white',
                      title=value,
                      )
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
