'''
Python 3.7
Written by Niki Hamidi Vadeghani @NikiHV
Created on August 29, 2021
Last change on September 12, 2021

- Filename: L5_M10.py
- Dependencies: None
- Non standard libraries (need installation): pandas
- Content: 
    |- <function get_L5> -> Calculates the mid-point of the five hours with 
    the lowest average value, of the given data as a time series.
    |- <function get_M10> -> Calculates the mid-point of the ten hours with 
    the highest average value, of the given data as a time series.

'''
import datetime

import pandas as pd


def get_L5(data):
    '''Calculates de L5 feature over <data> time serie. 
    L5 is the mid-point of the five hours with the lowest average value.
    Parameters: <data> - pandas.Series type object, has to have more than
    Returns: datetime.Timestamp type object
    '''
    ## Auxiliar function
    def get_L5_short_period(short_data):
        '''Calculates the L5 over a set of data shorter than 1 day.
        '''
        averages_5h = dict()
        for beg_window in short_data.index:
            ## Use windows of 5 hours length
            end_window = beg_window + datetime.timedelta(hours=5)
            ## If the window exceeds the record length finish the iteration
            if end_window > short_data.index[-1]:
                break
            ## Obtain the mean value of the data in this window
            mean = short_data[beg_window:end_window].mean()
            ## Append the mean value to averages_5h with the mid-point time as index
            averages_5h[beg_window + datetime.timedelta(hours=2.5)] = mean
        ## Obtain L5
        L5 = min(averages_5h, key=averages_5h.get)
        return L5
    
    ## Beginning and ending timestamps of data
    beg_timestamp = data.index[0]
    end_timestamp = data.index[-1]

    ## Exception handler: if get_l5 argument is shorter than 5 hours.
    if (end_timestamp - beg_timestamp).total_seconds() < 5*3600:
        print('Error in get_L5: the data given is shorter than 5 hours.')
        return False
    
    ## If the record is longer than 1 day calculate the L5 as the mean of the 
    ## L5 for each day
    if beg_timestamp + datetime.timedelta(days=1) < end_timestamp:
        ## List where the L5 of each day will be stored
        L5s = []
        
        ## Iterate in each day
        beg_day = beg_timestamp
        while beg_day < end_timestamp:
            ## The day ends at 00:00
            end_day = (beg_day + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            ## If the record finishes before the day ends stop there
            if end_day > end_timestamp:
                end_day = end_timestamp
            ## Calculate the L5 of this day
            L5_day = get_L5_short_period(data[beg_day:end_day])
            L5s.append(L5_day)
            ## Move to the next day
            beg_day = end_day
        
        ## Average the L5s of each day
        L5 = pd.Series(L5s).mean()

    ## If <data> is shorter than 1 day calculates the L5 over the entire record.
    else:
        L5 = get_L5_short_period(data)

    return L5


def get_M10(data):
    '''Calculates de M10 feature over <data> time serie. 
    M10 is the mid-point of the ten hours with highest average value.
    Parameters: <data> - pandas.Series type
    Returns: datetime.Timestamp type
    '''
    ## Auxiliar function
    def get_M10_short_period(short_data):
        '''Calculates the M10 over a set of data shorter than 1 day.
        '''
        averages_10h = dict()

        for beg_window in short_data.index:
            ## Use windows of 10 hours length
            end_window = beg_window + datetime.timedelta(hours=10)
            ## If the window exceeds the record length finish the iteration
            if end_window > short_data.index[-1]:
                break
            ## Obtain the mean value of the data in this window
            mean = short_data[beg_window:end_window].mean()
            ## Append the mean value to averages_5h with the mid-point time as index
            averages_10h[beg_window + datetime.timedelta(hours=5)] = mean
        
        ## Obtain M10
        M10 = max(averages_10h, key=averages_10h.get)
        return M10
    
    ## Beginning and ending timestamps of data
    beg_timestamp = data.index[0]
    end_timestamp = data.index[-1]
    
    ## If the record is longer than 1 day calculate the M10 as the mean of the 
    ## M10 for each day
    if beg_timestamp + datetime.timedelta(days=1) < end_timestamp:
        ## List where the L5 of each day will be stored
        M10s = []
        
        ## Iterate in each day
        beg_day = beg_timestamp
        while beg_day < end_timestamp:
            ## The day ends at 00:00
            end_day = (beg_day + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            ## If the record finishes before the day ends stop there
            if end_day > end_timestamp:
                end_day = end_timestamp
            ## Calculate the M10 of this day
            M10_day = get_M10_short_period(data[beg_day:end_day])
            M10s.append(M10_day)
            ## Move to the next day
            beg_day = end_day
        
        ## Average the M10s of each day
        M10 = pd.Series(M10s).mean()

    ## If <data> is shorter than 1 day calculates the M10 over the entire record
    else:
        M10 = get_M10_short_period(data)

    return M10

################################################################################
#################################  TEST  ZONE  #################################
################################################################################
if __name__ == "__main__":
    pass
    # import os
    
    # part_id = 1
    # save_path = os.path.join('Base de datos', 'retrieved_data',
    #             f'participant{part_id:0>2}_acc_lat.pkl')
    # df_vmc = pd.read_pickle(save_path)
    # print(df_vmc)