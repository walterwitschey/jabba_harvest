from pydicom.filereader import dcmread
import os
import re
import pandas as pd

def getStudyMetadata(studydir):
    
    studyinfo = []
    for subdir in os.listdir(studydir):
        series_dir = os.path.join(studydir,subdir)
        # print(series_dir)
        if not os.path.isdir(series_dir) : continue
        series_size = sum(os.path.getsize(os.path.join(series_dir,f)) for f in os.listdir(series_dir)) / 1e6
        first_dcm = os.listdir(series_dir)[0]
        first_dcm = os.path.join(series_dir,first_dcm)
        dcmdat = dcmread(first_dcm)
        ImageType = dcmdat.ImageType if hasattr(dcmdat,'ImageType') else None
        SeriesDescription = dcmdat.SeriesDescription if hasattr(dcmdat,'SeriesDescription') else None
        studyinfo.append({
            'Series Number': dcmdat.SeriesNumber,
            'Series Description': SeriesDescription,
            'Series Instance UID': dcmdat.SeriesInstanceUID,
            'Study Instance UID': dcmdat.StudyInstanceUID,
            'Accession Number': dcmdat.AccessionNumber,
            'Institution Name': dcmdat.InstitutionName,
            'Image Type': ImageType,
            'Size (MB)': series_size,
        })
        # print(f'series {dcmdat.SeriesNumber}: {dcmdat.SeriesDescription}')

    return studyinfo

def getStudyDir(dcmparentdir, studyid):
    studydir = [dir for dir in os.listdir(dcmparentdir) if re.match(f'{studyid}', dir)][0]
    studydir = os.path.join(dcmparentdir, studydir)
    return studydir

def getAllStudyMetadata(dcmparentdir, VERBOSE=True):
    studydirs = [os.path.join(dcmparentdir,dir) for dir in os.listdir(dcmparentdir) if os.path.isdir(os.path.join(dcmparentdir,dir))]
    allstudyinfo = []
    for studydir in studydirs:
        if VERBOSE: print(f'collecting info from {studydir}')
        # studyinfo = getStudyMetadata(studydir)
        try: 
            studyinfo = getStudyMetadata(studydir)
        except:
            print(f'error collecting from {studydir}')
            studyinfo = None
        if studyinfo: 
            allstudyinfo.extend(studyinfo)
    return allstudyinfo

def saveStudyData(data, outputfile):
    df = pd.DataFrame.from_dict(data)
    df.to_csv(outputfile)
    return True
