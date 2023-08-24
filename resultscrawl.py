## imports
import os
import pandas as pd
from glob import glob

def results_crawl(processed_dir, output_file, error_file='./processed_stats_errors.txt'):
    accession_dirs = [os.path.join(processed_dir,d) for d in os.listdir(processed_dir) if os.path.isdir(os.path.join(processed_dir,d))]
    n_accessions = len(accession_dirs)

    subsequent_run = os.path.exists(output_file) #have already run this before if True

    if subsequent_run:
        allresults = pd.read_csv(output_file)
        processed_studies = allresults['accession'].unique().astype(str) #list of studies already processed
        # print(processed_studies)
    else:
        allresults = pd.DataFrame()

    errors=[]
    skipped=[]
    
    for n, accessiondir in enumerate(accession_dirs):
        print(f'processing {accessiondir} ({n+1}/{n_accessions})', end='\r')


        accnum = os.path.basename(accessiondir)
        # print(accnum)
        if subsequent_run and (accnum in processed_studies): #skip if already processed
            skipped.append(accnum)
            continue

        searchstr = os.path.join(accessiondir,'**/*_stats.csv')
        statfiles = glob(searchstr,recursive=True)
        
        for statfile in statfiles:
            try:
                theseresults = pd.read_csv(statfile)
                allresults=pd.concat([allresults, theseresults])
            except:
                errors.append(statfile)

        with open(error_file,'w') as f:
            f.write("\n".join(errors))


    allresults.to_csv(output_file)
    n_skipped = len(skipped)

    print(f'processed {n_accessions}. {n_accessions - n_skipped} added, {n_skipped} skipped (already processed before)')

    return True

if __name__ == "__main__":
    processed_dir = '/home/jabba/data/processed'
    output_file = './processed_studies_stats.csv'
    results_crawl(processed_dir,output_file)
