'''
Python 3.7
Written by Niki Hamidi Vadeghani @NikiHV
Created on August 20, 2021
Last change on September 30, 2021

- Filename: feature_calculator.py
- Dependencies: ServerHandler.py
- Non standard libraries (need installation): numpy, pandas, neo4j
- Contents: 
    |- <class FeatureCalculator> -> processes the query responses obtained
    by ServerHandler class. It can calculate the VMC index for a time series, 
    gets the HR as a time series and calculates the Body Mass Index (BMI).

'''
import datetime

import numpy as np
import pandas as pd
from neo4j import data

from server_handler import ServerHandler


class FeatureCalculator:

    def __init__(self, uri, user, password, database=None):
        self.driver = ServerHandler(uri, user, password, database)
        self.num_parts = self.driver.num_parts
        
    def close(self):
        self.driver.close()

    @staticmethod
    def response2dataframe(response, set_coli_as_index=0):
        '''Converts a response from session.run() to a Pandas DataFrame.
        <set_col_as_index> : int, column index which will set as the DataFrame
        index column. Give None if no change is wanted.
        '''
        data = response.data()
        ## If <response> is empty
        if data == []:
            dataframe = pd.DataFrame()
        ## If <response> has data
        else: 
            dataframe = pd.DataFrame(data)
            if set_coli_as_index is not None:
                col_name = dataframe.columns[set_coli_as_index]
                dataframe = dataframe.set_index(col_name)
        return dataframe

    def get_participants_dates(self, part_id=None):
        '''Queries the properties timestampStart, timestampEnd, listSaturdays 
        and listSundays of all Participant nodes if part_id=None, or of a
        specific Participant with <part_id> id. Returns a DataFrame with 
        'timestampStart', 'timestampEnd', 'listSaturdays' and 'listSundays' 
        columns with Participant id as index.
        '''
        ## Query
        response = self.driver.query_participants_dates(part_id)
        ## Query's response to DataFrame
        df = self.response2dataframe(response)
        return df

    def get_days_by_weekday_weekend(self, dates):
        '''Recieves <dates> DataFrame with 'timestampStart', 'timestampEnd', 
        'listSaturdays' and 'listSundays' columns and the Participant's id as
        index. Returns a dictionary with the Participant id as keys and a 
        list of two lists as values, each one containing the dates of week and 
        weekend days.
        '''
        ## Initialize empty dict
        participants_days = {id:[] for id in dates.index.values}
        
        for id in dates.index.values:
            ## Get dates of part_id participant
            date_start    = dates.loc[id, 'timestampStart']
            date_end      = dates.loc[id, 'timestampEnd']
            date_saturday = dates.loc[id, 'listSaturdays']
            date_sunday   = dates.loc[id, 'listSundays']
            
            ## Create lists of week and weekend days
            weekends = date_saturday + date_sunday
            weekdays = []
            current = date_start
            while current.date() < date_end.date(): # does not include the end date
                if current.date() not in weekends:
                    weekdays.append(current)
                current += datetime.timedelta(days=1)

            ## Populate: save dates lists in dict
            participants_days[id].append(weekdays)
            participants_days[id].append(weekends)

        # print(participants_days)
        return participants_days

    def get_acc_data(self, part_id, lower_limit, upper_limit):
        '''Method which queries and processes the acceleration data from 
        Participant <part_id> id between <lower_limit> and <upper_limit> 
        timestamp bounds. 
        Returns a DataFrame with 'timestamp' as index, and 'lateral', 
        'longitudinal' and 'vertical' columns.
        '''
        ### String formating for adapting to neo4j
        
        ## Select the minimum lenght of the string format of the dates to 
        ## homogenize the format
        min_len = min(len(str(lower_limit)), len(str(upper_limit)))
        
        ## If lower_limit and upper_limit have the same string size so noone 
        ## has a final extra chunk.
        last_chunk_lower = ''  
        last_chunk_upper = ''  
        ## If lower_limit and upper_limit have different sizes, get the final 
        ## extra chunk of the longer one (corresponding to the time zone)
        if len(str(lower_limit)) > min_len:
            last_chunk_lower = str(lower_limit)[min_len:]
        elif len(str(upper_limit)) > min_len:
            last_chunk_upper = str(upper_limit)[min_len:]
        # print(f'last_chunk = {last_chunk_lower} | {last_chunk_upper}')

        ## Append the final chunk crosswise to uniform formats
        lower_limit_str = '"' + str(lower_limit) + last_chunk_upper + '"'
        upper_limit_str = '"' + str(upper_limit) + last_chunk_lower + '"'
        # print('lower_limit = ', lower_limit)
        # print('lower_limit_str = ', lower_limit_str)
        # print('upper_limit = ', upper_limit)
        # print('upper_limit_str = ', upper_limit_str)

        ## Query acceleration data
        response = self.driver.query_acc_data(part_id, lower_limit_str, upper_limit_str)

        ## If any exception during query return an empty DataFrame
        if response == False:
            ## Create an empty DataFrame (with NaN values) 
            empty_df = pd.DataFrame(
                columns=['lateral', 'longitudinal', 'vertical'],
                index=[lower_limit]) # set the <lower_limit> as timestamp
            empty_df.index.name = 'timestamp'
            return empty_df
        
        ## Unpack the response
        response_lat, response_lon, response_ver = response

        ## Responses to DataFrames
        df_lat = self.response2dataframe(response_lat)
        df_lon = self.response2dataframe(response_lon)
        df_ver = self.response2dataframe(response_ver)

        ## Join the three acceleration directions
        df_acc = pd.concat([df_lat, df_lon, df_ver], axis=1)
        
        ## If query goes fine but responses are empty
        if len(df_acc.columns) == 0:
            print('Warning in get_acc_data: Got an empty response while '
                  'querying acc data.')
            ## Return an empty DataFrame with the correct structure
            empty_df = pd.DataFrame(
                columns=['lateral', 'longitudinal', 'vertical'],
                index=[lower_limit])
            empty_df.index.name = 'timestamp'
            return empty_df
        
        # print(df_acc)
        return df_acc

    def get_r(self, acc):
        '''Calculates the r vector from acceleration data <acc> as:
                        r = sqrt( lat^2 + lon^2 + ver^2 )
        Parameter: <acc> - DataFrame with the 'timestamp' as index and 
                'lateral', 'longitudinal' and 'vertical' columns.
        Returns a Pandas Series. 
        '''
        r = (acc['lateral']**2 + acc['longitudinal']**2 + 
                    acc['vertical']**2) ** (0.5)
        return r  # type(r) = <class 'pandas.core.series.Series'>

    def get_VMC(self, r, first_timestamp):
        '''Parameter <r> : Series of r values over timestamps.
        Returns a dictionary with the first timestamp as key and the VMC (float)
        as value. If <r> is an empty Series returns nan in the dict value.
        '''
        ## If <r> as no data return a dict with None value
        if len(r.index) == 0:
            VMC = {first_timestamp: None}
       
        else:
            ## Calculate the VMC value
            VMC_value = (r - r.mean()).abs().mean()
            
            ## Calculate the representative timestamp as the mean of the 
            ## timestamps (indexes of <r>).
            ## (Gives the same result as excluding indexes of NaN values)
            r_index = pd.DataFrame(r.index) # Index object to DataFrame
            ## 'timestamp' column is neo4j.DateTime type --> convert to 
            ## pandas.Timestamp (cannot be directly, has to convert to string 
            ## first). This is needed to average the timestamps.
            r_index['timestamp'] = r_index['timestamp'].apply(lambda x: pd.Timestamp(str(x)))
            ## Average the timestamps
            avg_timestamp = np.mean(r_index['timestamp'])
            
            VMC = {avg_timestamp: VMC_value}
        return VMC 

    def get_VMC_serie(self, part_id, on_weekday=False, on_weekend=True):
        '''Calculates the VMC of Participant with <part_id> id along week days 
        and/or weekend days, depending on <on_weekday> and <on_weekend> 
        parameters. Uses a 1 minute windows. 
        Returns a Pandas Series.
        '''
        ## Participant dates
        part_dates = self.get_participants_dates(part_id)
        date_start = part_dates.loc[part_id, 'timestampStart']
        date_end   = part_dates.loc[part_id, 'timestampEnd']
        ## Get the timestamps of week and weekend days of the participant
        weekdays, weekends = self.get_days_by_weekday_weekend(part_dates)[part_id]
        ## Add to 'days' list the days' timestamps we want
        days = []
        if on_weekday: days.extend(weekdays)
        if on_weekend: days.extend(weekends)
        ## Exception handler
        if days == []:
            print('Error in get_VMC_serie(): must select at least one \
                parameter <on_weekday> and/or <on_weekend> as True.')
            return
    
        ## Here we are going to collect the VMCs in the time period specified
        ##      Uses dict instead of Pandas object for speed saving when 
        ##      appending data.
        vmc_serie = dict() # empty dict

        ## Just for testing
        # date_end = neo4j.time.DateTime(2019, 1, 16, 8, 35, 52.980)

        ## Iterate by day in 'days' list
        for day_beg in days:
            # print('day_beg = ', day_beg)
            ## Just for testing
            # day_beg = neo4j.time.DateTime(2019, 1, 16, 8, 35, 49.039)
            
            ## Define when the day ends
            day_end = (day_beg + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            ## Do not overflow the recording
            if day_end > date_end:
                day_end = date_end
            # print('day_end = ', day_end)

            ## Iterate by hour
            hour_beg = day_beg
            while hour_beg < day_end:
                # print('hour_beg = ', hour_beg)
                hour_end = hour_beg + datetime.timedelta(hours=1)
                ## Do not overflow the recording
                if hour_end > day_end:
                    hour_end = day_end
                # print('hour_end = ', hour_end)

                ## Testing values, for part_id=1
                # start = '"2019-01-16T08:35:49.039000000-03:00"'
                # end = '"2019-01-16T08:35:52.867000000-03:00"'

                acc_1hour = self.get_acc_data(part_id, hour_beg, hour_end)
                # print(acc_1hour)
                r_1hour = self.get_r(acc_1hour)
                # print(r_1hour)

                ## Compute VMC in 1 minute windows
                minute = hour_beg
                while minute < r_1hour.index[-1]:
                    ## Select each minute by clock, not deltas
                    next_minute = (minute + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
                    ## Do not overflow the recording
                    if next_minute > r_1hour.index[-1]:
                        next_minute = r_1hour.index[-1]
                    ## Get VMC in a 1 minute window
                    vmc = self.get_VMC(r_1hour[minute : next_minute], minute)
                    # print('vmc = ', vmc)
                    ## Append it to the VMC dict
                    vmc_serie.update(vmc) # append a dict to another
                    ## Move to the next minute
                    minute = next_minute

                ## Move to the next hour
                hour_beg = hour_end
        
        ## Convert dict to Pandas Series
        vmc_serie = pd.Series(vmc_serie, dtype=float)

        return vmc_serie

    def get_VMC_serie_by_date_range(self, part_id, beg_timestamp, end_timestamp):
        '''Calculates the VMC of Participant with <part_id> id between 
        <beg_timestamp> and <end_timestamp>. Uses a 1 minute windows. 
        Returns a Pandas Series.
        Parameters:
        - <part_id> : int
        - <beg_timestamp> and <end_timestamp> : str in format 
            "YYYY-MM-DDThh:mm:ss.ffffff-tttt" where
            Y:year, M:month, D:day, T has to go, h:hour, m:minute, s:second, 
            f:microsecond, '-tttt' or '+tttt' for the timezone as hhmm.
        '''
        ## Here we are going to collect the VMCs in the time period specified
        ##      Uses dict instead of Pandas object for speed saving when 
        ##      appending data.
        vmc_serie = dict() # empty dict

        acc_data = self.get_acc_data(part_id, beg_timestamp, end_timestamp)
        r = self.get_r(acc_data)

        ## Compute VMC in 1 minute windows
        minute = datetime.datetime.strptime(beg_timestamp, "%Y-%m-%dT%H:%M:%S.%f%z")
        while minute < r.index[-1]:
            ## Select each minute by clock, not deltas
            next_minute = (minute + datetime.timedelta(minutes=1)).replace(second=0, microsecond=0)
            ## Do not overflow the recording
            if next_minute > r.index[-1]:
                next_minute = r.index[-1]
            ## Get VMC in a 1 minute window
            vmc = self.get_VMC(r[minute : next_minute], minute)
            ## Append it to the VMC dict
            vmc_serie.update(vmc) # append a dict to another
            ## Move to the next minute
            minute = next_minute

        ## Convert dict to Pandas Series
        vmc_serie = pd.Series(vmc_serie, dtype=float)

        return vmc_serie

    def get_HR_data(self, part_id, lower_limit, upper_limit):
        '''Method which queries and processes HR data from Participant <part_id>
        id between <lower_limit> and <upper_limit> timestamp bounds. 
        Returns a DataFrame with 'timestamp' as index, and 'HR' column.
        '''
        ## Select the minimum printable lenght of the dates to 
        ## homogenize the format
        min_len = min(len(str(lower_limit)), len(str(upper_limit)))
        
        ## String formating for adapting to neo4j
        lower_limit_str = '"' + str(lower_limit)[:min_len] + '"'
        upper_limit_str = '"' + str(upper_limit)[:min_len] + '"'

        ## Query HR data
        response = self.driver.query_HR_data(part_id, lower_limit_str, upper_limit_str)

        ## If any exception during query return an empty DataFrame
        if response == False:
            ## Create an empty DataFrame (with NaN values) 
            empty_df = pd.DataFrame(columns=['HR'],
                index=[lower_limit]) # set the <lower_limit> as timestamp
            empty_df.index.name = 'timestamp'
            return empty_df

        ## Responses to DataFrame
        df_hr = self.response2dataframe(response)
        
        ## If query goes fine but responses are empty
        if len(df_hr.columns) == 0:
            ## Return an empty DataFrame with the correct structure
            empty_df = pd.DataFrame(columns=['HR'], index=[lower_limit])
            empty_df.index.name = 'timestamp'
            return empty_df

        return df_hr

    
    def get_HR_serie(self, part_id, on_weekday=False, on_weekend=True):
        '''Obtains the HR of Participant with <part_id> id along week days 
        and/or weekend days, depending on <on_weekday> and <on_weekend> 
        parameters. 
        Returns a Pandas Series.
        '''
        ## Participant dates
        part_dates = self.get_participants_dates(part_id)
        date_start = part_dates.loc[part_id, 'timestampStart']
        date_end   = part_dates.loc[part_id, 'timestampEnd']
        ## Get the timestamps of week and weekend days of the participant
        weekdays, weekends = self.get_days_by_weekday_weekend(part_dates)[part_id]
        ## Add to 'days' list the days' timestamps we want
        days = []
        if on_weekday: days.extend(weekdays)
        if on_weekend: days.extend(weekends)
        ## Exception handler
        if days == []:
            print('Error in get_HR_serie(): must select at least one \
                parameter <on_weekday> and/or <on_weekend> as True.')
            return
    
        ## Here we are going to collect the HRs in the time period specified
        hr_serie = pd.DataFrame(columns=['HR'])
        hr_serie.index.name = 'timestamp'

        ## Iterate by day in 'days' list
        for day_beg in days:
            # print('day_beg = ', day_beg)
            
            ## Define when the day ends
            day_end = (day_beg + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            if day_end > date_end:
                day_end = date_end
            # print('day_end = ', day_end)

            hr_day = self.get_HR_data(part_id, day_beg, day_end)
            hr_serie =  pd.concat([hr_serie, hr_day])
        
        ## Convert DataFrame to Series
        hr_serie = hr_serie.squeeze()

        return hr_serie

    def get_height_weight_data(self, part_id=None):
        '''Queries and processes the height and weight data of Participant 
        <part_id> id if given. If part_id=None look for the data of all 
        Participants.
        Returns a DataFrame with id index and 'height' and 'weight' columns.
        '''
        ## Query
        response = self.driver.query_height_weight_data(part_id)
        df_hw = self.response2dataframe(response)
        return df_hw

    def get_BMI(self, part_id=None):
        '''Calculates the Body Mass Index (BMI) of the Participant <part_id> id
        if given, or all Participants if part_id=None.
        Returns a Pandas Series with Participant ids as index and BMI values.
        '''
        ## Get the processed data
        hw = self.get_height_weight_data(part_id)
        ## Calculate the BMI
        BMI = hw['weight'] / hw['height']**2
        return BMI


    ######################### FUNCIONES FÁCILES PARA PLOTEAR (rafa) #########################3333

    def get_lf_feature_data(self, feature_name, sensor_name):
        '''Method which queries and processes MP data from Participant <part_id>
        id between <lower_limit> and <upper_limit> timestamp bounds. 
        Returns a DataFrame with 'timestamp' as index, and 'MP' column.
        '''
        

        response = self.driver.query_lf_feature(feature_name, sensor_name)

        
        df_hr = self.response2dataframe(response)
        
        

        return df_hr

    
    def get_MP_data(self):
        '''Method which queries and processes MP data from Participant <part_id>
        id between <lower_limit> and <upper_limit> timestamp bounds. 
        Returns a DataFrame with 'timestamp' as index, and 'MP' column.
        '''
        
        response = self.driver.query_MP_data()

        
        df_hr = self.response2dataframe(response)
        
        

        return df_hr

    def get_acc_datav2(self, time):
        '''Method which queries and processes the acceleration data from 
        Participant <part_id> id between <lower_limit> and <upper_limit> 
        timestamp bounds. 
        Returns a DataFrame with 'timestamp' as index, and 'lateral', 
        'longitudinal' and 'vertical' columns.
        '''
        

        ## Query acceleration data

        time_ = '"' + str(time) + '"'

        response = self.driver.query_acc_datav2(time_)
        
        response_lat, response_lon, response_ver = response

        ## Responses to DataFrames
        df_lat = self.response2dataframe(response_lat)
        df_lon = self.response2dataframe(response_lon)
        df_ver = self.response2dataframe(response_ver)

        ## Join the three acceleration directions
        df_acc = pd.concat([df_lat, df_lon, df_ver], axis=1)
        
        return df_acc

    


################################################################################
#################################  TEST  ZONE  #################################
################################################################################
if __name__ == "__main__":
    pass
    # import os
    # import time

    ## Test data base
    # calculator = FeatureCalculator("bolt+s://orosticaingenieria.cl:6687", "user", "password", "chronotype")

    ## Oficial data base
    # calculator = FeatureCalculator("neo4j://orosticaingenieria.cl:6087", "user", "password", "chronotype")

    # start_time = time.time()
    # # vmc_serie = calculator.get_VMC_serie(1)
    # # hr = calculator.get_HR_data(1, "2019-01-19T00:00:00.000000000", "2019-01-20T00:00:00.000000000")
    # # hr = calculator.get_HR_serie(1)
    # hw = calculator.get_BMI(1)
    # print(time.time() - start_time)
    
    # print(hw)
    
    # calculator.close()
