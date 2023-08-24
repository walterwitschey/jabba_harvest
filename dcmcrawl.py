from dcmhelper import getAllStudyMetadata, saveStudyData

data = getAllStudyMetadata('/home/jabba/data/queue')
saveStudyData(data,'test.csv')