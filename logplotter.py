import pandas as pd
import plotly.express as px
from datetime import datetime
from datetime import timedelta

def dfSubset(df, timeframe, tcol):
    '''
    Returns a subset of a dataframe that occurs within a specified time from the last entry

    Inputs
    df: input dataframe
    timeframe: length of time from the last entry in df to return. Can be 'month'/'week'/'day' (1 month/week/day), 'all' (all data),
    or a datetime.timedelta object
    tcol: column label to use to apply the timeframe
    
    '''
    
    # select data to plot
    if type(timeframe) == timedelta:
        td = timeframe
    elif timeframe == 'week':
        td = timedelta(weeks = 1)
    elif timeframe == 'day':
        td = timedelta(days = 1)
    elif timeframe == 'month':
        td = timedelta(weeks = 4)
    elif timeframe == 'all':
        td = None

    if td:
        last_time = df[tcol].max()
        start_time = last_time - td
        df = df[df[tcol] > start_time]

    return df


def plotXferTime(resultsfile:str, timeframe:str | timedelta = 'week',outputfile:str = None):
    '''
    Plots a scatterplot of transfer times to the AI server

    Inputs
    resultsfile: pathway for csv file containing parsed server logs
    timeframe: either a str or datetime.timedelta object specifying how much data to plot. By default plots the last week's worth of data
    outputfile (optional): save results as an interactive html

    Outputs:
    returns a figure

    '''

    tcol = 'study_start_time' #column name to use as index for time (ie the x axis for plots)

    # read in results
    df = pd.read_csv(resultsfile)
    df[tcol] = pd.to_datetime(df[tcol]) #make sure x axis is in datetime format

    # select subset to plot
    df = dfSubset(df, timeframe, tcol)

    # plot
    fig = px.scatter(df,'study_start_time','study_transfer_time',hover_data=['accession'])

    # save results
    if outputfile:
        fig.write_html(outputfile)

    return fig

def plotAItime(resultsfile:str, timeframe: str | timedelta = 'week', outputfile:str = None, metrics='all'):
    '''
    Plots a scatterplot of AI processing times

    Inputs
    resultsfile: pathway for csv file containing parsed AI logs
    timeframe: either a str or datetime.timedelta object specifying how much data to plot. By default plots the last week's worth of data
    metrics: list specifying which metrics to plot. 'all' will plot all metrics. Default 'all'
    outputfile (optional): save results as an interactive html

    Outputs:
    returns a figure

    '''

    tcol = 'pipeline_start' #column name to use as index for time (ie the x axis for plots)

    # read in results
    df = pd.read_csv(resultsfile)
    df[tcol] = pd.to_datetime(df[tcol]) #make sure x axis is in datetime format

    # select subset of time to plot
    df = dfSubset(df, timeframe, tcol)

    # plot
    if metrics == 'all':
        vars_to_plot = ['pipeline_time','push_time','AI_plus_push']
    else:
        vars_to_plot = metrics
    df = pd.melt(df,id_vars=['accession','series_no','pipeline_start'], value_vars=vars_to_plot, var_name = 'metric', value_name = 'time')
    fig = px.scatter(df,x='pipeline_start',y='time',facet_row='metric', hover_data=['accession'])
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
    fig.update_yaxes(matches=None)

    # save results
    if outputfile:
        fig.write_html(outputfile)

    return fig