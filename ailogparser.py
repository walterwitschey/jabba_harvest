import pandas as pd
import re
import numpy as np
import os

def ParseAILogLine(line: str):
    '''parses a single line from an AI log file'''
    parts = line.strip().split(" ")
    time = pd.to_datetime(parts[0] + ' ' + parts[1])
    module = parts[2]
    msgtype = parts[3]
    line_dat = None

    if re.match(r'\d+_\d',module):
        accession, series_no = module.split('_')
        msg = " ".join(parts[4:])
        event = None
        if 'Start pipeline' in msg:
            event = 'pipeline_start'
        elif 'JabbaPipeline run time' in msg:
            event = 'pipeline_end'
        elif 'Pushing dicom to PACS' in msg:
            event = 'push_start'
        elif 'Releasing connection' in msg:
            event = 'push_stop' 



        if event:
            line_dat = {
                'time': time,
                'accession': accession,
                'series_no': series_no,
                'event': event,
            }
    
    return line_dat

def ParseAILogs(filenames: list):
    '''Parse a list of AI logs'''
    log_dat = []
    for filename in filenames: 
        with open(filename) as file:
            print(f'processing {filename}')
            for line in file:
                line_dat = ParseAILogLine(line)
                if line_dat:
                    log_dat.append(line_dat)
    return log_dat

def AnalyzeAILogs(log_dat: list):
    '''Analyze a list of dicts that were parsed from AI logs'''
    df = pd.DataFrame.from_dict(log_dat)

    def tdInMin(t1,t2):
        return (t1 - t2).total_seconds()/60
    
    df = df.pivot_table(index=['accession','series_no'], columns='event', values='time',aggfunc=np.max) #using max as aggfunc because multiple push stops
    df = df.reset_index()
    df['pipeline_time'] = df.apply(lambda x: tdInMin(x['pipeline_end'],x['pipeline_start']), axis=1)
    df['push_time'] = df.apply(lambda x: tdInMin(x['push_stop'],x['push_start']), axis=1)
    df['AI_plus_push'] = df.apply(lambda x: tdInMin(x['push_stop'],x['pipeline_start']), axis=1)

    return df


if __name__ == '__main__':
    logdir = '/home/jabba/data/logs'
    logfiles = os.listdir(logdir)
    ai_files = [os.path.join(logdir,file) for file in logfiles if re.match(r'.*_ai_log_.*',file)]

    log_dat = ParseAILogs(ai_files)
    df = AnalyzeAILogs(log_dat)
    df.to_csv('parsed_ai_logs.csv') 
