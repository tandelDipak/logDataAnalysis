from pandas import DataFrame, read_csv
import pandas as pd
import numpy as np
import datetime
import time
import csv


location = r'/home/dipak/Downloads/topology.csv'
topology = pd.read_csv(location)
topology = pd.DataFrame(topology)

resultFyle = open("TRY.csv",'w')
wr = csv.writer(resultFyle, dialect='excel')
resultFyle1 = open("TRY1.csv",'w')
wr1 = csv.writer(resultFyle1, dialect='excel')

#change system name with short  name : use one another file 

l = [3,5,7,9,11]
r = [4,6,8,10,12]
col = topology.shape[1]
row = topology.shape[0]
pair = int((col - 3 ) / 2)

#combine left node's name with port
for column in l:
	topology.iloc[:,column] = topology.iloc[:,1] + "-" + topology.iloc[:,column] 

#combine right node's name with it's port
for column in r:
	topology.iloc[:,column] = topology.iloc[:,2] + "-" + topology.iloc[:,column]

#initilize empty set
group = [set() for i in range(0,pair)]
#make a set
for i, j in zip(range(0,pair), l):
	for k in range(0, topology.shape[0]):
		group[i].add(topology.iloc[k,j])
		group[i].add(topology.iloc[k,j+1])

#initilize empty dictionary
mark = [dict() for i in range(0,pair)]
#make a dictionary 
for i, j in zip(range(0,pair), l):
	for k in range(0, topology.shape[0]):
		mark[i][topology.iloc[k,j]] = [0]
		mark[i][topology.iloc[k,j+1]] = [0]


#read systemName mapping file  
location = r'/home/dipak/Downloads/systemName.csv'
systemName = pd.read_csv(location)
systemName = pd.DataFrame(systemName)
#initilize empty dictionary for system name mapping
systemMapping = dict()
for i in range(0,systemName.shape[0]):
	systemMapping[systemName.iloc[i,0]] = systemName.iloc[i,1]



#read log data from file and convert it to useable form
#location = r'/home/dipak/Downloads/Final.csv'
location = r'/home/dipak/Downloads/AlarmArchive1.csv'
df = pd.read_csv(location)
df = pd.DataFrame(df)
df.iloc[:,12] = df.iloc[:,12].fillna(value = df.iloc[:,1])
df.iloc[:,1] = pd.to_datetime(df.iloc[:,1])
df.iloc[:,12] = pd.to_datetime(df.iloc[:,12])
cn = ['Severity', 'Raise time ( GMT )', 'System type','Clear time ( GMT )', 'System name', 'Supression Flag', 'Component','Additional text', 'Description', 'Acknowledged', 'Annotation',  'Alarm type', 'Probable cause', 'Equipment type',  'Cleared by', 'CLFI']
df = df.reindex(cn, axis="columns")
#df.iloc[:,3] = df.iloc[:,2] - df.iloc[:,1]
#select column from full data starting from 0iy
select = [1,3,4,6,7,12]
df = df.iloc[:,select]

	#df.iloc[i,3] = df.iloc[i,2] + "-" + df.iloc[i,3]

for i in range(0,df.shape[0]):
	df.iloc[i,2] = systemMapping[df.iloc[i,2]]
	df.iloc[i,3] = df.iloc[i,3].split('-',1)[1]
	df.iloc[i,3] = df.iloc[i,2] + "-" + df.iloc[i,3]

#create list to insert extracted data
#output = [ [[[]]] for temp in range(0,pair+1)]
output = [ [ [[]] for temp1 in range(0,topology.shape[0])] for temp2 in range(0, pair+1)]
#initilize start and end time
dt = df.iloc[:,0]
start = dt[0] + datetime.timedelta(seconds = -3) 
end = dt[0] + datetime.timedelta(seconds = 0)
#start = dt[0] + datetime.timedelta(seconds = -12000) 
#end = dt[0] + datetime.timedelta(seconds = 12000)



'''for index, row in window.iterrows():
		a = row.iloc[:,3]
		out = a in x
		if out:
			#print(a)
			ports.update({a:1})
			#ports
			#time.sleep(100)
			#print("Yessss")
			#else:
			#print(a)
	#ports
	#time.sleep(5)
	#print("1")'''
#df = df[(df.iloc[:,4] == "Gauge Threshold Crossing Alert Summary") ]

store = df
while True:
	#time.sleep(2)
	window = df[(( df.iloc[:,0] < end ) & ( df.iloc[:,0] >= start))]
	start = start + datetime.timedelta(seconds = 1) 
	end = end + datetime.timedelta(seconds = 1)
	print("##########",start,"##########")
	print("##########",end,"##########")
	#print(window.iloc[:,0:4])
	#print("")
	for i, card in zip(list(range(0,pair)), l):
		#time.sleep(2)
		#for i, card in zip(range(0,pair), [3]):
		#print(row, end = " ")
		#print("@@@@@@@@@@@@@@   value of i is ", i,  "and value of card is ",card)
		for index, row in window.iterrows():
			#@print(index,end = " ")
			port = row[3]
			#print(port,end = " ")
			available = port  in group[i]
			#print(available)
			if available:
				mark[i].update({port:[1,row[0].strftime('%d/%m/%y %H:%M:%S'),row[1],row[2],row[4],row[5]]})
				#print(port,end = " : ")
				#print(mark[i][port])
				#print(row)
			#print(" ")
		#print("Next****************")
		#choose each fiber one by one variable : fiber
		for fiber in range(0,topology.shape[0]):
			#print("@@@",end = " ")
			#print("Checking fiber : ",fiber , end = "  ")
			#@print(fiber)
			if(mark[i][topology.iloc[fiber,card]][0] & mark[i][topology.iloc[fiber,card+1]][0]):
				#print(topology.iloc[fiber,0], end = " ")
				#print(mark[i][topology.iloc[fiber,card]][0], end = " ")
				#print(mark[i][topology.iloc[fiber,card+1]][0],end = " ")
				#!!!!!!output[i][fiber].append([start,end,topology.iloc[fiber,0],mark[i][topology.iloc[fiber,card]][0],mark[i][topology.iloc[fiber,card+1]][0],mark[i][topology.iloc[fiber,card]][0],mark[i][topology.iloc[fiber,card+1]][0]])
				output[i][fiber].append([start,end,topology.iloc[fiber,0],mark[i][topology.iloc[fiber,card]][0],mark[i][topology.iloc[fiber,card+1]][0],mark[i][topology.iloc[fiber,card]],mark[i][topology.iloc[fiber,card+1]]])
				#@print("COndi0")
				if((len(output[i][fiber]) >= 4)):
					#@print("Value checking COndi0")
					if((((output[i][fiber][2][3] == 1) & (output[i][fiber][2][4] == 0))) | ((output[i][fiber][2][3] == 0) & (output[i][fiber][2][4] == 1)) & ((output[i][fiber][3][3] == 1) & (output[i][fiber][3][4] == 1)) ):
						#@print("Start COndi0")
						repeat1 = output[i][fiber][1]
						repeat2 = output[i][fiber][2]
						#print("Removed repeated value")
						#print(repeat1)
						#print(repeat2)
						output[i][fiber].remove(repeat1)
						output[i][fiber].remove(repeat2)
						#@print("End COndi0")
				#@print("COndi1")
				if((len(output[i][fiber]) >= 4)):
					#@print("Value checking COndi1")
					if(((output[i][fiber][2][3] == 1) & (output[i][fiber][2][4] == 1)) & ( (output[i][fiber][3][3] == 1) & (output[i][fiber][3][4] == 1) ) & (output[i][fiber][1][3] != output[i][fiber][1][4])):
						#@print("Start COndi2")
						repeat = output[i][fiber][1]
						#print("Removed repeated value")
						#print(repeat)
						output[i][fiber].remove(repeat)
						#@print("End COndi1")
				if((len(output[i][fiber]) >= 4)):
						before = len(output[i][fiber])
						#print(before, end = " ")
						mem = [1,2,3]
						#print("***************Start processing********")
						choose = 0
						for filtered in mem:
							if(((output[i][fiber][filtered][3] == 0) & (output[i][fiber][filtered][4] == 1)) | ((output[i][fiber][filtered][3] == 1) & (output[i][fiber][filtered][4] == 0))):
								choose = 1
							else:
								if ((output[i][fiber][filtered][3] == 1) & (output[i][fiber][filtered][4] == 1)):
									choose = filtered
						#temp
						#print(output[i][fiber][choose])
						#print("Output this++++++++++++++++++++++++")
						if((output[i][fiber][choose][3] == 1) & (output[i][fiber][choose][4] == 0) ):
							ohh = [str(output[i][fiber][choose][2]),str(output[i][fiber][choose][5][1]),str(output[i][fiber][choose][5][2]),str(output[i][fiber][choose][5][3]),str(output[i][fiber][choose][5][4]),str(i),str(fiber)]
							wr.writerow(ohh)
							#print("leeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")	
						if((output[i][fiber][choose][3] == 0) & (output[i][fiber][choose][4] == 1) ):
							ohh = [str(output[i][fiber][choose][2]),str(output[i][fiber][choose][6][1]),str(output[i][fiber][choose][6][2]),str(output[i][fiber][choose][6][3]),str(output[i][fiber][choose][6][4]),str(i),str(fiber)]
							wr.writerow(ohh)
							#print("leeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
						if((output[i][fiber][choose][3] == 1) & (output[i][fiber][choose][4] == 1) ):
							ohh = [str(output[i][fiber][choose][2]),str(output[i][fiber][choose][6][1]),str(output[i][fiber][choose][6][2]),str(output[i][fiber][choose][6][3]),str(output[i][fiber][choose][6][4]),str(i),str(fiber)]
							wr.writerow(ohh)
							#print("leeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee !!!!!! both")
						for filtered in mem:
							#print(filtered) 
							value = output[i][fiber][1]
							if filtered == 3:
								#print(value, end = " ")
								dgdg = 1
							else:
								#print(value)
								dgdg = 1
							output[i][fiber].remove(value)
						after = len(output[i][fiber])
						#print("VALUE OF OHH IS ",ohh)
						#print(after)				
			#*********************************************************************************
			else:
				if(mark[i][topology.iloc[fiber,card]][0] | mark[i][topology.iloc[fiber,card+1]][0]):
					#print(topology.iloc[fiber,0], end = " ")
					#print(mark[i][topology.iloc[fiber,card]][0], end = " ")
					#print(mark[i][topology.iloc[fiber,card+1]][0])
					#!!!!!!!!!!output[i][fiber].append([start,end,topology.iloc[fiber,0],mark[i][topology.iloc[fiber,card]][0],mark[i][topology.iloc[fiber,card+1]][0],mark[i][topology.iloc[fiber,card]][0],mark[i][topology.iloc[fiber,card+1]][0]])
					output[i][fiber].append([start,end,topology.iloc[fiber,0],mark[i][topology.iloc[fiber,card]][0],mark[i][topology.iloc[fiber,card+1]][0],mark[i][topology.iloc[fiber,card]],mark[i][topology.iloc[fiber,card+1]]])
					if((len(output[i][fiber]) >= 4)):
							before = len(output[i][fiber])
							#print(before, end = " ")
							mem = [1,2,3]
							#print("***************Start processing********")
							choose = 0
							for filtered in mem:
								if(((output[i][fiber][filtered][3] == 0) & (output[i][fiber][filtered][4] == 1)) | ((output[i][fiber][filtered][3] == 1) & (output[i][fiber][filtered][4] == 0))):
									choose = 1
								else:
									if ((output[i][fiber][filtered][3] == 1) & (output[i][fiber][filtered][4] == 1)):
										choose = filtered
							#temp
							#print(output[i][fiber][choose])
							#print("Output this++++++++++++++++++++++++")
							if((output[i][fiber][choose][3] == 1) & (output[i][fiber][choose][4] == 0) ):
								ohh = [str(output[i][fiber][choose][2]),str(output[i][fiber][choose][5][1]),str(output[i][fiber][choose][5][2]),str(output[i][fiber][choose][5][3]),str(output[i][fiber][choose][5][4]),str(i),str(fiber)]
								wr.writerow(ohh)
								#print("leeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
							if((output[i][fiber][choose][3] == 0) & (output[i][fiber][choose][4] == 1) ):
								ohh = [str(output[i][fiber][choose][2]),str(output[i][fiber][choose][6][1]),str(output[i][fiber][choose][6][2]),str(output[i][fiber][choose][6][3]),str(output[i][fiber][choose][6][4]),str(i),str(fiber)]
								wr.writerow(ohh)
								#print("leeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
							if((output[i][fiber][choose][3] == 1) & (output[i][fiber][choose][4] == 1) ):
								ohh = [str(output[i][fiber][choose][2]),str(output[i][fiber][choose][6][1]),str(output[i][fiber][choose][6][2]),str(output[i][fiber][choose][6][3]),str(output[i][fiber][choose][6][4]),str(i),str(fiber)]
								wr.writerow(ohh)
								#print("leeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee !!!!!! both")
							for filtered in mem:
								#print(filtered)
								value = output[i][fiber][1]
								if filtered == 3:
									#print(value, end = " ")
									dgdg = 1
								else:
									#print(value)
									dgdg = 1
								output[i][fiber].remove(value)
							#print("VALUE OF OHH IS ",ohh)
							after = len(output[i][fiber])
							#print(after)
			#print( " ")
	for i, j in zip(list(range(0,pair)), l):
		for k in list(range(0, topology.shape[0])):
			mark[i][topology.iloc[k,j]] = [0]
			mark[i][topology.iloc[k,j+1]] = [0]


resultFyle.close()