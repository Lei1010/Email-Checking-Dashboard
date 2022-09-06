import pandas as pd
import snowflake.connector as sf
import json
import os
from snowflake.connector.pandas_tools import write_pandas

from dash import Dash, html, dcc
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import plotly.express as px

SCHEMA = 'CWNZ01'
TABLE = 'EML_REPORT_20220624'
TEMPLATE = 'plotly_white'

top_vendors = ['Loreal', 'JOHNSON & JOHNSON', 'BRAND FOILO', 'DIPLOMAT NEW ZEALAND', 'BEIERSDORF', 'MCPHERSONS',
               'REAL VITAMINS', 'SANOFI', 'AFT PHARMACEUTICALS', 'NESTLE', 'BLACKMORES']

top_vendor_emails = ['Loreal.com', 'Jnj.com', 'brandfolio.co', 'diplomat-global.com', 'beiersdorf.com', 'Mcphersons.co',
                     'realvitamins.co', 'sanofi.com', 'aftpharm.com', 'nestle.com', 'blackmoresnz.co']
top_CM_emails = ['adil.khan@chemistwarehouse.co.nz', 'akshay.gawli@chemistwarehouse.co.nz']

color = {}


def get_schema_name():
    cursor.execute("SELECT schema_name FROM information_schema.schemata where lower(schema_name) not like "
                   "'%information%'")
    df = cursor.fetchall()

    return [x for l in df for x in l]


def total_graph(total_df):
    fig = go.Figure()

    fig.add_trace(go.Scatter(x=total_df.received_date,
                             y=total_df.count_mail,
                             text=total_df.day_name
                             )
                  )

    fig.update_layout(xaxis_title='Month',
                      yaxis_title='Total Mail',
                      template=TEMPLATE,
                      hovermode='x unified')

    return fig


CONFIG = json.loads(open(r"config.json").read())

SF_ACCOUNT = CONFIG['ETL_EML_STAT']['account']
SF_USER = CONFIG['ETL_EML_STAT']['user']
SF_WAREHOUSE = CONFIG['ETL_EML_STAT']['warehouse']
SF_ROLE = CONFIG['ETL_EML_STAT']['role']
SF_DATABASE = CONFIG['ETL_EML_STAT']['database']
SF_SCHEMA = CONFIG['ETL_EML_STAT']['schema']
SF_PASSWORD = CONFIG['ETL_EML_STAT']['password']
SF_AUTH = CONFIG['ETL_EML_STAT']['authenticator']

conn = sf.connect(user=SF_USER, password=SF_PASSWORD, account=SF_ACCOUNT, authenticator=SF_AUTH,
                  warehouse=SF_WAREHOUSE, role=SF_ROLE, database=SF_DATABASE, schema=SF_SCHEMA)

cursor = conn.cursor()
try:
    cursor.execute(f"select * from {SCHEMA}.{TABLE}")
    all_rows = cursor.fetchall()
    num_fields = len(cursor.description)
    field_names = [i[0] for i in cursor.description]
finally:
    conn.close()

raw_report_df = pd.DataFrame(all_rows)
raw_report_df.columns=field_names

raw_report_df.drop_duplicates(['RECEIVED', 'SUBJECT', 'FROM', 'TO', 'NO_OF_ATTACHMENTS', 'BODY_CHARACTER_COUNT'],
                              inplace=True)
raw_report_df = raw_report_df[~raw_report_df.FILENAME.str.contains('neml', case=False)]
raw_report_df['received_date'] = pd.to_datetime(raw_report_df.RECEIVED).dt.date

total_mail_df = raw_report_df.groupby('received_date', as_index=False).agg(
    count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))
total_mail_df['day_name'] = pd.to_datetime(total_mail_df.received_date).dt.day_name()

total_mail = len(raw_report_df)
total_null = len(raw_report_df[~raw_report_df.SUBJECT.notna()])
percent_null_subject = round(total_null / total_mail, 4) * 100
total_no_sender = len(raw_report_df[~raw_report_df['TO'].notna()])
percent_no_sender = round(total_no_sender / total_mail, 4) * 100

top_20_address = raw_report_df.groupby('FROM', as_index=False).agg(count_mail=pd.NamedAgg(column='PATH', aggfunc='count'))
top_20_address.sort_values('count_mail', ascending=False, inplace=True, ignore_index=True)
top_20_address = top_20_address.head(20)

app = Dash(external_stylesheets=[dbc.themes.JOURNAL])
app.title = "Checking Email Dashboard"

app.layout = html.Div(id='parent',
                      children=[
                          html.H1(id='H1', children='Checking Email Dashboard', className='p-5 text-center bg-light'),
                          dbc.Row(
                              [
                                  dbc.Col(
                                      html.Div(
                                          children=f"""{SCHEMA}.{TABLE}"""
                                      ),
                                      style={'textAlign': 'center'}
                                  ),

                              ],
                              justify='center'
                          ),
                          html.Br(),
                          dbc.Row(
                              [
                                  dbc.Col(
                                      dbc.Card(
                                          [
                                              html.H2(id='total-mail-card',
                                                      children=f'{total_mail}',
                                                      className="card-title"),
                                              html.P("of Total Email", className="card-text"),
                                          ],
                                          body=True,
                                          color="light",
                                      )),
                                  dbc.Col(
                                      dbc.Card(
                                          [
                                              html.H2(id='percent-no-subject-card',
                                                      children=f'{percent_null_subject}%',
                                                      className="card-title"),
                                              html.P("Emails with No Subject", className="card-text"),
                                          ],
                                          body=True,
                                          color="dark",
                                          inverse=True,
                                      )),
                                  dbc.Col(
                                      dbc.Card(
                                          [
                                              html.H2(id='percent-no-sender-card',
                                                      children=f'{percent_no_sender}%',
                                                      className="card-title"),
                                              html.P("Emails with No Sender", className="card-text"),
                                          ],
                                          body=True,
                                          color="secondary",
                                          inverse=True,
                                      )),
                              ]
                          ),
                          html.Br(),
                          dcc.Graph(id='total_by_date',
                                    figure=total_graph(total_mail_df)),
                          dbc.Row(
                              [
                                  dbc.Col(
                                      html.Div(
                                          [
                                              html.H3(id='vendor-h3', children='Total Email of Top 10 Vendors',
                                                      style={'font-family': 'courier',
                                                             'marginTop': 20,
                                                             'marginLeft': 40,
                                                             'textAlign': 'left'
                                                             }),
                                              dcc.Dropdown(top_vendor_emails,
                                                           top_vendor_emails[0],
                                                           id='top-vendor-dropdown',
                                                           clearable=False),
                                              dcc.Graph(id='top-vendor-graph')
                                          ],
                                      ),

                                  ),
                                  dbc.Col(
                                      html.Div(
                                          [
                                              html.H3(id='vendor-h3-c', children='Total Email of top 10 CMs',
                                                      style={'font-family': 'courier',
                                                             'marginTop': 20,
                                                             'marginLeft': 40,
                                                             'textAlign': 'left'
                                                             }
                                                      ),
                                              dcc.Dropdown(
                                                  top_CM_emails,
                                                  top_CM_emails[0],
                                                  id='top-cm-dropdown',
                                                  clearable=False,

                                              ),
                                              dcc.Graph(id='top-cm-graph')
                                          ]
                                      ),
                                  )
                              ]
                          ),
                          dbc.Row(
                              [
                                  dbc.Col(

                                  ),
                                  dbc.Col(

                                  )
                              ]
                          )

                      ],
                      style={
                          'marginLeft': 20,
                          'marginRight': 20,
                      }
                      )


# @app.callback(
#     Output("table-name-dropdown", "options"),
#     [Input("schema-name-dropdown", "value")]
# )
# def get_table_name(value):
#     cursor.execute(f"SELECT table_name from information_schema.tables where table_schema = '{value}'")
#     options = cursor.fetchall()
#     return [x for l in options for x in l]

#
# @app.callback(Output("total-mail-card", 'children'),
#               Input('table-name-dropdown', 'table'))
# def total_mail_card(table):
#     total_mail = len(raw_report_df)
#     return f"{total_mail}"
#
#
# @app.callback(Output("percent-no-subject-card", 'children'),
#               Input("total-mail-card", 'total_mail')
#               )
# def no_subject_card(total_mail):
#     total_null = len(raw_report_df[~raw_report_df.SUBJECT.notna()])
#     percent_null = round(total_null / total_mail, 4) * 100
#     return f"{percent_null}%"
#
#
# @app.callback(Output("percent-no-sender-card", 'children'),
#               Input("total-mail-card", 'total_mail')
#               )
# def no_sender_card(total_mail):
#     total_no_sender = len(raw_report_df[~raw_report_df['TO'].notna()])
#     percent_no_sender = round(total_no_sender / total_mail, 4) * 100
#     return f"{percent_no_sender}%"


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
                      template=TEMPLATE,
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
                      template=TEMPLATE,
                      title=value,
                      )
    return fig


if __name__ == '__main__':
    app.run_server(debug=True)
