import pandas as pd
import sys

from datetime import datetime
from dateutil import parser
from math import floor
from time import sleep


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


def print_info(time, sample):
    print('Time: ' + str(time))
    print('Sample:\n')
    print(sample)

def realtime(data, speed, refresh, func):
    
    '''
      CURRENT:
    ======================================================================
    separates dataframe based on epoch
    cutoff based on runtime of program 
    sleep time is modified to ensure accuracy < 20 ms (roughly)
    
    

          OBSOLETE:
        ======================================================================
        instead of using actual runtime, can use simulated runtime
        simulated runtime = refresh time * speed * iteration
        
        start time is set to epoch time of first row, rounded down
    '''

    earliest = floor(data.iloc[0].epoch)
    start = datetime.now().timestamp()
    
    #i = 0.0

    while len(data) > 0:
        inner_start = datetime.now().timestamp()
        
        print('Total time: ' + str(runtime(start)))

        cutoff_time = runtime(start) * speed + earliest
        data, sample = get_past_data(data, cutoff_time)
        
        
        
        '''time_elapsed = refresh * speed * i
        cutoff = time_elapsed + earliest
        data, sample = get_past_data(data, cutoff)
        '''
        func(cutoff_time, sample)
        
        
        '''
        #whatever logic you want executed per cycle goes here
        #ex: plot on map
        #maybe in future add function as a parameter
        '''
        inner_runtime = runtime(inner_start)
        print('r - i: ' + str(refresh-inner_runtime))    
        sleep(refresh - inner_runtime - 0.01)


if __name__ == '__main__':
    
    
    path = sys.argv[1]
    speed = float(sys.argv[2])
    refresh = float(sys.argv[3])
    

    df = load_data(path)    
    input('Data loaded. Press any key to continue.\n')
    
    realtime(df, speed, refresh, print_info)
    

    