import os
from datetime import datetime
import pandas as pd
import sys
import argparse
import json
import glob

class JabbaLogParse:

    type="v2"

    def __init__(self):
        self.df=None
        accList=None
        self.accToUID={}

        self.server_parsed=None

    def ParseAILogFile(self, filename):
        rows=[]
        with open(filename) as file:
            next(file)
            for line in file:
                parts = line.strip().split(" ")
                if len(parts) >= 4:
                    date = parts[0]
                    time = parts[1]
                    try:
                        oldstmp = stmp
                        stmp = datetime.strptime(date + " " + time, "%Y-%m-%d %H:%M:%S")
                        module = parts[2]
                        if "JabbaPipeline" in module:
                            modParts = module.split("/")
                            series = modParts[1]

                            acc = modParts[0].split("_")[1]
                            if acc not in self.accList:
                                self.accList.append(acc)

                        level = parts[3]
                        message = " ".join(parts[4:])

                        if "Study: " in message:
                            uid = message.split("Study: ")[1]
                            nline= next(file)
                            nparts = nline.strip().split(" ")
                            nmessage = " ".join(nparts[4:])
                            acc = nmessage.split(",")[0].split("accessionNumber: ")[1]
                            self.uidToAcc[uid] = acc                           

                        #if level=="ERROR":
                        #    print(line)
                    except:
                        stmp=oldstmp
                        message = line.rstrip()
                else:
                    message = line.rstrip()

                rows.append({"date":stmp, "module":module, "level":level, "message":message})

        self.df = pd.DataFrame.from_dict(rows)
        return(True)

        return(rows)
    
    def ParseAILogs(self, processed, tree=None, dicom=None):
        rows=[]
        stmp=None
        self.accList=[]
        self.accToUID={}
        self.uidToAcc={}

        acc_list = self.server_parsed.accession.unique()
        print("acc_list size: ", str(len(acc_list)))
        for acc in acc_list:
            acc_dir = os.path.join(processed, acc)
            if os.path.exists(acc_dir):
                series_dirs = [os.path.join(acc_dir, x) for x in os.listdir(acc_dir) if os.path.isdir(os.path.join(acc_dir, x))]
                for series_dir in series_dirs:
                    series_log = os.path.join(series_dir, "")
                    log_file = glob.glob(os.path.join(series_dir, "*_log.txt"))

                    if os.path.exists(log_file[0]):
                        print(log_file[0])
                        #series_dat = self.ParseAILogFile(log_file)
                        #if series_dat is not None:
                        #    for row in series_dat:
                        #        rows.append(row)



    
    def ParseServerLogLine(self, line):
        parts = line.strip().split(" ")
        retval={"type":"none"}

        if 'C-STORE' in line:
            if len(parts) >= 4:
                time = pd.to_datetime(parts[0] + ' ' + parts[1])
                module = parts[2]
                level = parts[3]
                message = " ".join(parts[4:]).strip()  
                #print(message)
                acc = None
                study = None
                series = None
                instance = None


                try:
                    info = message.split("C-STORE: ")[1]
                except:
                    print(f'error parsing {message}')
                    return retval
                info_parts = info.split(",")

                if len(info_parts) == 3:
                    study = info_parts[0]
                    series = info_parts[1]
                    instance = info_parts[2]
                elif len(info_parts) == 4:
                    acc = info_parts[0]
                    study = info_parts[1]
                    series = info_parts[2]
                    instance = info_parts[3]

                retval = {"time":time, "type":"C-STORE", "module":module, "level":level, "study_uid": study, "series_uid": series, "instance_uid": instance, "accession": acc,}
        
        return(retval)
    
    def get_folder_size(self, folder):
        total_size = os.path.getsize(folder)
        for item in os.listdir(folder):
            itempath = os.path.join(folder, item)
            if os.path.isfile(itempath):
                total_size += os.path.getsize(itempath)
            elif os.path.isdir(itempath):
                total_size += self.get_folder_size(itempath)
        return total_size

    def AddTreeToDat(self, dat, tree, dicom):
        cpt_map={}
        accession_map={}
        ninstances=0
        institution_map={}
        size_map={}


        for line_dat in dat:
            n_bytes=0
            if line_dat["study_uid"] not in cpt_map:
                if dicom is not None and line_dat['study_uid'] not in size_map:
                    dicom_dir = os.path.join(dicom, line_dat["study_uid"])
                    if os.path.exists(dicom_dir):
                        
                        n_bytes = self.get_folder_size(dicom_dir)
                        print("folder size: "+str(n_bytes))
                    else:
                        print("No dicom folder for " + line_dat["accession"] )
                line_dat['size'] = n_bytes
                size_map[line_dat['study_uid']]=n_bytes

                tree_file = os.path.join(tree, line_dat["study_uid"] + ".json")
                if not os.path.exists(tree_file):
                    print("No tree file for " + line_dat["accession"] )

                if os.path.exists(tree_file):
                    with open(tree_file) as tree_f:
                        
                        tree_dat = json.load(tree_f)
                        for s in tree_dat['StudyList']:

                            if s['StudyInstanceUID']['Value'][0] == line_dat["study_uid"]:
                                
                                cpt = s['ProcedureCodeSequence']['Value'][0]['00080100']['Value'][0]
                                line_dat['cpt'] = cpt
                                accession = s['AccessionNumber']['Value'][0]
                                line_dat['accession'] = accession


                                cpt_map[line_dat['study_uid']] = cpt
                                accession_map[line_dat['study_uid']] = accession
                                

                                print(accession + " is type " + cpt)
                                print(line_dat)

                                for r in s['SeriesList']:
                                    ninstances += len(r['InstanceList'])
                                    inst = r['InstitutionName']['Value'][0]
                                    line_dat['institution'] = inst 
                                    institution_map[line_dat['study_uid']] = inst

                                line_dat['ninstances'] = ninstances
                else:
                    print("No tree file for " + line_dat["study_uid"] )
            else:
                line_dat['cpt'] = cpt_map[line_dat['study_uid']]
                line_dat['accession'] = accession_map[line_dat['study_uid']]
                line_dat['size'] = size_map[line_dat['study_uid']]

        
        return 0
    
    def GetTreeData(self, treedir: str, scanlist, dicomdir=None):
        '''
        Get dicom tree data in dataframe format
        
        Inputs
        treedir: directory where dicom trees are stored
        scanlist: list or array of accessions to fetch dicom trees for

        Outputs
        df_study: pandas dataframe containing info at the study level
        df_sries: pandas dataframe containing info at the series level

        '''
        df_study = pd.DataFrame(columns=['study_uid', 'cpt', 'study_description', 'institution'])
        df_series = pd.DataFrame(columns=['study_uid', 'series_uid', 'series_description'])

        for scan in scanlist:

            # get the file name TODO: try study_uid as filename if accession doesn't exist
            fname = os.path.join(treedir, f'{scan}.json')
            if not os.path.exists(fname):
                # print(f'cannot find {scan}')
                continue

            print(f'getting tree for {scan}')

            # load the json data
            with open(fname) as f:
                treedat = json.load(f)

            # iterate through studies in the file (can be more than one per accession
            for study in treedat['StudyList']:
    
                # get study level data
                study_uid = study['StudyInstanceUID']['Value'][0]
                try:
                    cpt = study['ProcedureCodeSequence']['Value'][0]['00080100']['Value'][0]
                except:
                    cpt = None
                study_description = study['StudyDescription']['Value'][0]
                inst = study['SeriesList'][0]['InstitutionName']['Value'][0] #should be the same for every series, so for efficiency's sake just get this once

                # append study data to end of dataframe
                df_study.loc[len(df_study)]={'study_uid':study_uid, 'cpt':cpt, 'study_description':study_description, 'institution':inst}

                # get series level data
                series_from_this_study = pd.DataFrame(columns=['study_uid', 'series_uid', 'series_description'])
                for seriesdat in study['SeriesList']:
                    # print(seriesdat)
                    try:
                        series_description = seriesdat['SeriesDescription']['Value'][0]
                    except:
                        series_description = None
                    series_uid = seriesdat['SeriesInstanceUID']['Value'][0]
                    
                    series_from_this_study.loc[len(series_from_this_study)] = {'study_uid':study_uid, 'series_uid':series_uid, 'series_description':series_description}

                # append series data to end of dataframe    
                df_series = pd.concat([df_series, series_from_this_study])
            
            
        return df_study, df_series

            

        

    def ParseServerLog(self, filenames, key=None, tree=None, dicom=None):
        log_dat = []

        for filename in filenames:
            print(f'parsing {filename}') 
            with open(filename) as file:
                for line in file:
                    line_dat = self.ParseServerLogLine(line)
                    if line_dat["type"] == "C-STORE":
                        log_dat.append(line_dat)

        if tree is not None:
            self.AddTreeToDat(log_dat, tree, dicom)

        if len(log_dat)>0:
            self.server_parsed = pd.DataFrame.from_dict(log_dat)

        return(True)
    
    def AnalyzeParsedServerLog(self, treedir=None):
        '''
        Take the raw parsed data (one line for every C-STORE) and summarize it at the study and series level.
        Since we only have timestamps for C-STORE requests, will assume negligible time to store any single instance
        and instead calculate delta between first and last C-STORE requests
        '''
        # collect all the studies and series while dropiping the instance level data. Store as new variable
        self.server_log_summary=self.server_parsed[['accession','study_uid','series_uid']].drop_duplicates()
        
        # calculate transfer times at study and series level
        study_xfer_time = self.server_parsed.groupby(['study_uid'])['time'].apply(lambda x: (max(x) - min(x)).total_seconds()/60)
        study_xfer_time = study_xfer_time.rename('study_transfer_time').reset_index()
        
        series_xfer_time = self.server_parsed.groupby(['study_uid','series_uid'])['time'].apply(lambda x: (max(x) - min(x)).total_seconds()/60)
        series_xfer_time = series_xfer_time.rename('series_transfer_time').reset_index()

        # calculate number of instances at study and series level
        instances_per_study = self.server_parsed.groupby(['study_uid'])['instance_uid'].apply('nunique')
        instances_per_study = instances_per_study.rename('instances_per_study').reset_index()

        instances_per_series = self.server_parsed.groupby(['study_uid','series_uid'])['instance_uid'].apply('nunique')
        instances_per_series = instances_per_series.rename('instances_per_series').reset_index()

        # add data to summary
        self.server_log_summary = self.server_log_summary.merge(study_xfer_time,left_on='study_uid',right_on='study_uid')
        self.server_log_summary = self.server_log_summary.merge(series_xfer_time,left_on=['study_uid','series_uid'],right_on=['study_uid','series_uid'])
        self.server_log_summary = self.server_log_summary.merge(instances_per_study,left_on='study_uid',right_on='study_uid')
        self.server_log_summary = self.server_log_summary.merge(instances_per_series,left_on=['study_uid','series_uid'],right_on=['study_uid','series_uid'])

        # add data from dicom trees if the directory is specified
        if treedir:
            study_df, series_df = self.GetTreeData(treedir, self.server_log_summary['accession'].unique())
            self.server_log_summary = pd.merge(self.server_log_summary,study_df,left_on='study_uid',right_on='study_uid',how='left')
            self.server_log_summary = pd.merge(self.server_log_summary,series_df,left_on=['study_uid','series_uid'],right_on=['study_uid','series_uid'],how='left')

        return(True)
    
def main():

    my_parser = argparse.ArgumentParser(description='Display DICOM Header Info')
    my_parser.add_argument('-s', '--server', type=str, help='server log file', required=True, nargs='+')
    my_parser.add_argument('-o', '--output', type=str, help='output file', required=False)
    my_parser.add_argument('-k', '--key', type=str, help='acc to uid key csv file', required=False)
    my_parser.add_argument('-t', '--tree', type=str, help='dir of dicom tree files', required=False)
    my_parser.add_argument('-d', '--dicom', type=str, help='base dir of dicom files', required=False)
    my_parser.add_argument('-p', '--processed', type=str, help='base dir of processed files', required=True)
    args = my_parser.parse_args()
    print(args)

    key=None
    tree=None
    dicom=None
    processed=None
    if args.tree:
        tree=args.tree
    if args.key:
        key = pd.read_csv(args.key)
        key = key.set_index('study_uid').to_dict()['accession_number']
    if args.processed:
        processed=args.processed
    if args.dicom:
        dicom=args.dicom


    parser = JabbaLogParse()

    success = parser.ParseServerLog(args.server, key=key, tree=tree, dicom=dicom)
    #if success and not args.ai_log and args.output:
    #    parser.server_parsed.to_csv(args.output, index=False)
    if success:
        print("Parse AI Logs")
        success = parser.ParseAILogs(processed, tree=tree, dicom=dicom)



    #fname2 = sys.argv[2]
    #x.ParsePrimary(fname2)
    #print(x.uidToAcc)
    


if __name__ == "__main__":
    main()
