import pandas as pd
import sys

from datetime import datetime
from dateutil import parser
from math import floor


def _epochtime(row):
    parsed_time = float(parser.parse(row['localtime']).timestamp())
    return parsed_time


def load_data(path):
    '''
    Reads data into pandas dataframe
    Data should be in JSON format
    Adds UNIX time column (epoch)
    '''
    # Load dataset    
    print('Loading dataset: ' + path)
    df = pd.read_json(path, lines=True)
    
    # Convert timestamp to UNIX time and as column
    df['epoch'] = df.apply(_epochtime, axis=1)
    
    return df


def runtime(start):
    return datetime.now().timestamp() - start


def get_past_data(df, cutoff):
        
    past_data = df[df['epoch'] < cutoff]
    df = df[df['epoch'] >= cutoff]

    return df, past_data



def realtime(data, speed, refresh):
    
    '''
      CURRENT:
    ======================================================================
    instead of using actual runtime, can use simulated runtime
    simulated runtime = refresh time * speed * iteration
    
    start time is set to epoch time of first row, rounded down


          OBSOLETE:
        ======================================================================
        separates dataframe based on epoch
        cutoff based on runtime of program 
        Changed to current implementation due to time.sleep() inconsistencies
    '''

    earliest = floor(data.iloc[0].epoch)
    start = datetime.now().timestamp()
    i = 0.0

    while len(data) > 0:
        cutoff = (refresh * speed * i) + earliest
        data, sample = get_past_data(data, cutoff)
        
        print('Time: ' + str(refresh * speed * i))
        print('Test: ' + str(len(data)))
        print('Sample:\n')
        print(sample)
        
        i += 1
        
        input('Continue?\n')
        #time.sleep(refresh)


if __name__ == '__main__':
    
    
    path = sys.argv[1]
    speed = int(sys.argv[2])
    refresh = int(sys.argv[3])
    

    df = load_data(path)    
    input('Data loaded. Press any key to continue.\n')
    
    realtime(df, speed, refresh)
    

    