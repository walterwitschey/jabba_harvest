from dcmhelper import getAllStudyMetadata, saveStudyData
import pandas as pd
import os

def dcmcrawl(parentdir,outputfile):
    datanew = getAllStudyMetadata(parentdir)
    datanew = pd.DataFrame.from_dict(datanew)
    if os.path.exists(outputfile):
        dataold = pd.read_csv(outputfile, dtype='object')
        Nold = len(dataold['accession'].unique())
    else:
        dataold = None

    if dataold is None:
        print(f'WARNING: output file does not already exist. Will create new outputfile at {outputfile}')
        saveStudyData(datanew,outputfile)
        Nnew = len(datanew['accession'].unique())
        print(f'metadata from {Nnew} studies added')
        return True
    
    if dataold is not None:
        datanew = pd.concat([dataold, datanew]).drop_duplicates(subset=['accession','Series Instance UID','Study Instance UID']).reset_index(drop=True)
        saveStudyData(datanew,outputfile)
        Nnew = len(datanew['accession'].unique())
        print(f'metadata from {Nnew - Nold} studies added ({Nnew} total studies parsed)')
        return datanew
        
        
if __name__ == '__main__':
    data = getAllStudyMetadata('/home/jabba/data/queue')
    saveStudyData(data,'test.csv')