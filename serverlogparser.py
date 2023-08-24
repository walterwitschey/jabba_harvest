import pandas as pd
import os
import numpy as np
import re

import jabba_log_parse as jlp


#
logdir = '/home/jabba/data/logs'
logfiles = os.listdir(logdir)
server_files = [os.path.join(logdir,file) for file in logfiles if re.match(r'^jabba_server_log_.*',file)]

# parse the server files
jabbaparser = jlp.JabbaLogParse()
print('loading')
jabbaparser.ParseServerLog(server_files)
print('analyzing')
jabbaparser.AnalyzeParsedServerLog()


# do some more analysis
study_start_times = jabbaparser.server_parsed.groupby(['study_uid'])['time'].min().rename('study_start_time')
study_stop_times = jabbaparser.server_parsed.groupby(['study_uid'])['time'].max().rename('study_stop_time')
series_start_times = jabbaparser.server_parsed.groupby(['study_uid','series_uid'])['time'].min().rename('series_start_time')
series_stop_times = jabbaparser.server_parsed.groupby(['study_uid','series_uid'])['time'].max().rename('series_stop_time')
jabbaparser.server_log_summary = jabbaparser.server_log_summary.merge(study_start_times, left_on='study_uid',right_on='study_uid')
jabbaparser.server_log_summary = jabbaparser.server_log_summary.merge(study_stop_times, left_on='study_uid',right_on='study_uid')
jabbaparser.server_log_summary = jabbaparser.server_log_summary.merge(series_start_times,left_on=['study_uid','series_uid'],right_on=['study_uid','series_uid'])
jabbaparser.server_log_summary = jabbaparser.server_log_summary.merge(series_stop_times,left_on=['study_uid','series_uid'],right_on=['study_uid','series_uid'])

# save
jabbaparser.server_log_summary.to_csv('parsed_transfer_logs.csv')