from dash import Dash, Input, Output, State, html, dcc, callback, ctx
import dash_bootstrap_components as dbc
#import pandas as pd
import time, random, requests
from os import getenv
from dash.exceptions import PreventUpdate
from datetime import datetime

app = Dash(__name__, external_stylesheets=[dbc.themes.SUPERHERO])

server = app.server
app.title = "Platform Tools"
app._favicon = ('icons8-steam-32.png')

origin = f"https://{getenv('DETA_SPACE_APP_HOSTNAME')}"

api_key = getenv("DETA_API_KEY")
headers = {"x-api-key": api_key}

authors = []
data = {}

def create_table(columns, data):
    table_th = []
    for c in columns:
        table_th.append(html.Th(c))
    table_header = [html.Thead(html.Tr(table_th))]
    table_rows = []
    for row in data:
        table_td = []
        for col in row:
            table_td.append(html.Td(col))
        table_rows.append(html.Tr(table_td))
    table_body = [html.Tbody(table_rows)]
    table = dbc.Table(table_header + table_body, bordered=True)
    return table


# Layout elements
title = html.H1('Post Dumper')
input_url = dbc.Input(placeholder="https://steamcommunity.com/app/552990/eventcomments/3807281795252533005")

input_dropdown = dcc.Dropdown(id='input_dropdown', clearable=False)
#spinner_parse = dbc.Spinner(size="sm")
slider = dcc.Slider(0, 15, step=1, value=3, id='slider')
dorpdown = dcc.Dropdown(
    options = [], value = [],
    multi=True, id='dropdown', style={'color':'#fffffff'}
)

btn_parse = dbc.Button('Dump', id='btn_parse', color="primary",
                       outline=True, n_clicks=0, disabled=False, 
                          style = {'width':'180px'})
btn_authors = dbc.Button("Random Authors", id='btn_authors', color="primary", outline=True, 
                         n_clicks=0, style = {'width':'180px'})
btn_download = dbc.Button('Download Data', color="primary", outline=True, 
                          style = {'width':'180px'})

spinner_parse = dbc.Spinner(id='spinner_parse', size="sm")

#input_group = dbc.InputGroup([input_dropdown, html.Div(children=btn_parse, id='div_parse')])

input_group = dbc.Row([dbc.Col([input_dropdown], width=9),
                        dbc.Col([btn_parse])], style = {'margin-top':'14px'}, align="center"),


# Layout body
tab1_content = dbc.Container(
html.Div([
    dbc.Row([title], style = {'margin-left':'7px', 'margin-top':'40px'}),
    #dbc.Row([input_group], style = {'margin-top':'7px'}),
    dbc.Row([dbc.Col([input_dropdown], width=9),
             dbc.Col([btn_parse])], style = {'margin-top':'7px'}, align="center"),
    dbc.Row(html.Div(id='output_parse', style = {'color':'#90EE90'})),

    #Authors
    dbc.Row([dbc.Col([html.H5('Random Authors'), slider], width=9),
            dbc.Col([btn_authors])], style = {'margin-top':'14px'},
            align="center"),
    dbc.Row(html.Div(id='output_authors', children='Authors list', style = {'color':'#90EE90'})),

    #Download
    dbc.Row([dbc.Col([html.H5('Columns to Download'), dorpdown], width=9),
        dbc.Col([btn_download])], style = {'margin-top':'14px'},
            align="center"),
    dbc.Row(html.Div(id='output_download', style = {'color':'#90EE90'})),
    dbc.Row(children=[], id='table', style = {'margin-top':'14px'})
    ]
),style={"height": "100vh", "width": "70%", "font-family":"Motiva Sans, Sans-serif"})


tab2_content = dbc.Container(
    html.Div(
        [
            html.P("This is News Grabber!", className="card-text"),
            dbc.Button("Don't click here", color="danger"),
        ]
    ),
    className="mt-3",
)

tabs = dbc.Tabs(
    [
        dbc.Tab(tab1_content, label="Post Dumper"),
        dbc.Tab(tab2_content, label="News Grabber"),
        dbc.Tab("This tab's content is never seen", label="Reviews Dumper"),
    ]
)

# Layout
app.layout = html.Div([tabs])


# Callbacks
@callback(Output('input_dropdown', 'options'), Input('btn_parse', 'n_clicks'))
def retrun_dropdown(n_clicks):
    if n_clicks == 0:
        resp= requests.get('http://localhost:8000/titles')
        global data
        data = resp.json()
        return data['title']
    else:
        raise PreventUpdate
    
@callback(Output('table', 'children'), Output('output_parse', 'children'), Output('btn_parse', 'disabled'), 
          Input('input_dropdown', 'value'), 
          prevent_initial_call=True)
def retrun_dropdown(value):
    if value is not None:
        global data, i, eventcomment_id
        i = data['title'].index(value)
        eventcomment_id = data['eventcomment_id'][i]       
        resp = requests.get(f'http://localhost:8000/comments/{eventcomment_id}')
        data_comments = resp.json()
        
        date_published = data['time_published'][i].split(' ')[0]
        time_parsed = data['time_parsed'][i] if data['time_parsed'][i] != '0' else '-'
        #time_parsed = '2023-08-23 22:50:20'

        if data_comments is None:
            return '', date_published, False
        
        '''
        global authors
        authors = data_comments['author']
        '''
        comment_link = f'https://steamcommunity.com/app/552990/eventcomments/{eventcomment_id}'+'#c{}'
        data_comments['text_with_link'] = []
        for comment_id, text in zip(data_comments['comment_id'], data_comments['text']):
            data_comments['text_with_link'].append(html.A(href=comment_link.format(comment_id), children=text, target="_blank"))
        use_cols = ['time', 'text', 'author', 'steam_id']
        use_data = ['time', 'text_with_link', 'author', 'steam_id']

        #cols = list(data_comments.keys())
        #table_data = list(data_comments.values())
        table_data = [data_comments[k] for k in use_data]
        table_data = list(map(list, zip(*table_data)))
        table = create_table(use_cols, table_data)

        note = ''
        if time_parsed == '-':
            disabled = False
        else:
            dt = datetime.utcnow() - datetime.strptime(time_parsed, '%Y-%m-%d %H:%M:%S')
            dt = int(dt.total_seconds() / 60) #time difference in minutes
            disabled = False if dt > 30 else True
            note = f'({30-dt} minutes till next dump)' if dt < 30 else ''
        output_parse_txt = f'Published: {date_published}, Last Dump: {time_parsed} {note}'
        return table,  output_parse_txt, disabled
    
@callback(Output('input_dropdown', 'value'), #Output('dropdown', 'options'), 
          Input('btn_parse', 'n_clicks'), State('input_dropdown', 'value'))
def retrun_parse(n_clicks, value):
    if n_clicks == 0:
        raise PreventUpdate
    elif n_clicks > 0:
        #resp = requests.get('http://localhost:8000/hello')
        #resp = requests.get(f"{origin}/parser/hello", headers=headers)
        global data, i, eventcomment_id
        #resp = requests.get('http://localhost:8000/comments/3807281795249062183')
        #resp = requests.get(f"{origin}/backend/comments", headers=headers)
        resp = requests.get(f'http://localhost:8000/dump/{eventcomment_id}')
        #data = resp.json()
        #global authors
        #authors = [r[1] for r in resp.json()['data'][0:5]]
        #cols = resp.json()['columns'][0:6]
        '''
        authors = data['author']
        cols = list(data.keys())
        table_data = list(data.values())
        table_data = list(map(list, zip(*table_data)))
        table = create_table(cols, table_data)
        '''
        return data['title'][i] #cols, cols
    else:
        pass

@callback(Output('output_download', 'children'), Input('dropdown', 'value'))
def retrun_dropdown(value):
    return ', '.join(value)

@callback(Output('output_authors', 'children'), Input('btn_authors', 'n_clicks'), State('slider', 'value'))
def retrun_authros(n_clicks, value):
    if n_clicks > 0 and value > 0:
        #list_authros = ["Arthur","Ford","Zaphod","Trillian"]
        n = value #value if value <= len(authors) else len(authors)
        global eventcomment_id
        resp = requests.get(f'http://localhost:8000/authors/{eventcomment_id}/{n}')
        #resp = requests.get(f"{origin}/backend/authors", headers=headers)
        rand_authors = resp.json()
        if rand_authors is None:
            return ''

        #rand_authors = random.sample(authors, n)
        return ', '.join(rand_authors['author']) #ctx.triggered_id  #'Authors list'
    else:
        pass

if __name__ == "__main__":
    app.run_server(debug=True)