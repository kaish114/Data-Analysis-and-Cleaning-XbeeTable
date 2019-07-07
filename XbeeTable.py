
import sys

def progressbar(it, prefix="", size=60, file=sys.stdout):
    count = len(it)
    def show(j):
        x = int(size*j/count)
        file.write("%s[%s%s] %i/%i\r" % (prefix, "#"*x, "."*(size-x), j, count))
        file.flush()        
    show(0)
    for i, item in enumerate(it):
        yield item
        show(i+1)
    file.write("\n")
    file.flush()

import time
# Above Function is used to create Progress bar




# # 1. Importing Libraries

import numpy as np
import pandas as pd

# # 2. Reading WiSenseData


#Reading the Indoor dataset
dff = pd.read_csv('XbeeTable.csv' , header = None)  

# # 3. Renaming the Columns
df = dff.rename(columns={0: 'datestamp', 1: 'nodeAddress' , 2: 'BME280_Temp', 3: 'BME280_Hum', 4: 'BME280_Pres', 5: 'DS18B20_Temp', 6: 'DHT_Temp', 7: 'DHT_Hum', 8: 'BATTERY_Vol'})


#Copying the original dataset ('df') into data1
data1 = df.copy() 


#Converting datatype of 'timeStamp' to datetime type
data1['datestamp'] = pd.to_datetime(data1['datestamp'])  


# # Functions to Clean the DataSet


# Now We will create new columns in our Dataset as below.
# These column will contain value '1' if corresponding values are changed changed else it will contain 0
data1['BME280_Temp_changed'] = 0
data1['BME280_Pres_changed'] = 0
data1['DS18B20_Temp_changed'] = 0
data1['DHT_Temp_changed'] = 0


# # Following is the function to clean temperature and pressure


nodes = data1['nodeAddress'].unique() # this line will create an array having total unique nodes


#Function to clean 'BME280_Temp'


def BME280_Temp_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'BME280_Temp'])
            time0 = (df.loc[i,'datestamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'BME280_Temp'])
                time1 = (df.loc[j , 'datestamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'BME280_Temp'] = val0
                  df.loc[j, 'BME280_Temp_changed'] = 1
                  k = j
                  break
                elif((val1) > 100  and minutes > 30.0):
                  df.loc[j,'BME280_Temp'] = 'NaN'
                  k = j
                  break
                
                elif((val1) < 0  and minutes > 30.0):
                  df.loc[j,'BME280_Temp'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                    
                 
                
                
# Function to clean 'BME280_Pres'

def BME280_Pres_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'BME280_Pres'])
            time0 = (df.loc[i,'datestamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'BME280_Pres'])
                time1 = (df.loc[j , 'datestamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'BME280_Pres'] = val0
                  df.loc[j, 'BME280_Pres_changed'] = 1
                  k = j
                  break
                elif(((val1) > 1000 or (val1) < 750 ) and minutes > 30.0):
                  df.loc[j,'BME280_Pres'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break                
                

            
            
            

#Function to clean 'DS18B20_Temp'


def DS18B20_Temp_clean(df):
    for n in tqdm_notebook(nodes , desc = 'Processing records'):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'DS18B20_Temp'])
            time0 = (df.loc[i,'datestamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'DS18B20_Temp'])
                time1 = (df.loc[j , 'datestamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'DS18B20_Temp'] = val0
                  df.loc[j, 'DS18B20_Temp_changed'] = 1
                  k = j
                  break
                elif((val1) > 60  and minutes > 30.0):
                  df.loc[j,'DS18B20_Temp'] = data1['DS18B20_Temp'].mean()
                  k = j
                  break
                
                elif((val1) < 0  and minutes > 30.0):
                  df.loc[j,'DS18B20_Temp'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                       
                                 
                        
                        
                        
#Function to clean 'DHT_Temp'


def DHT_Temp_clean(df):
    for n in progressbar(nodes, "Computing: "):
    #for n in nodes:
        k = 0
        for i in range(k , df.shape[0]-1):
          if(df.loc[i, 'nodeAddress'] == n):
            val0 = float(df.loc[i,'DHT_Temp'])
            time0 = (df.loc[i,'datestamp' ])
            for j in range(i+1, df.shape[0]-1):
              if(df.loc[j, 'nodeAddress'] == n):
                val1 = float(df.loc[j , 'DHT_Temp'])
                time1 = (df.loc[j , 'datestamp'])
                timedelta = time1 - time0
                minutes = timedelta.total_seconds() / 60
                
                if (abs(val1 - val0) > 10 and minutes < 30.0):
                  df.loc[j,'DHT_Temp'] = val0
                  df.loc[j, 'DHT_Temp_changed'] = 1
                  k = j
                  break
                elif((val1) > 100  and minutes > 30.0):
                  df.loc[j,'DHT_Temp'] = 'NaN'
                  k = j
                  break
                
                elif((val1) < 0  and minutes > 30.0):
                  df.loc[j,'DHT_Temp'] = 'NaN'
                  k = j
                  break
                else:
                  k = j
                  break
                                            



print('Cleaning BME280_Temp')
BME280_Temp_clean(data1)

print('Cleaning BME280_Pres')
BME280_Pres_clean(data1)

print('Cleaning DS18B20_Temp')
DS18B20_Temp_clean(data1)

print('Cleaning DTH_Temp')
DHT_Temp_clean(data1)

