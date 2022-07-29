'''
Python 3.7
Written by Niki Hamidi Vadeghani @NikiHV
Created on August 29, 2021
Last change on August 30, 2021

- Filename: server_handler.py
- Dependencies: None
- Non standard libraries (need installation): neo4j
- Content: 
    |- <class ServerHandler> -> connects to the specified database server and
    makes queries to retrive the needed data. Returns the server responses.

'''
import neo4j


class ServerHandler:

    def __init__(self, uri, user, password, database=None):
        self.driver = neo4j.GraphDatabase.driver(uri, auth=(user, password))
        self.session = self.driver.session(database=database)
        self.query_num_parts()
        
    def close(self):
        self.session.close()
        self.driver.close()

    def query_num_parts(self):
        '''Obtains the number of Participant nodes in the database and saves it
        in the <num_parts> attribute.
        '''
        result = self.session.run("MATCH (p:Participant) RETURN count(p)")
        self.num_parts = result.value()[0]

    def query_participants_dates(self, part_id=None):
        '''Queries the properties timestampStart, timestampEnd, listSaturdays 
        and listSundays of all Participant nodes if part_id=None, or of a
        specific Participant with <part_id> id. 
        Returns a neo4j.work.result.Result object.
        '''
        if part_id is None:
            response = self.session.run('''
                MATCH (p:Participant) 
                RETURN p.id AS id, 
                    p.timestampStart AS timestampStart, 
                    p.timestampEnd AS timestampEnd, 
                    p.listSaturdays AS listSaturdays, 
                    p.listSundays AS listSundays
                ''')
        else:
            response = self.session.run(f'''
                MATCH (p:Participant {{id: {part_id}}}) 
                RETURN p.id AS id, 
                    p.timestampStart AS timestampStart, 
                    p.timestampEnd AS timestampEnd, 
                    p.listSaturdays AS listSaturdays, 
                    p.listSundays AS listSundays
                ''')

        return response

    def query_acc_data(self, part_id, lower_limit_str, upper_limit_str):
        '''Method which queries acceleration data from Participant <part_id> id 
        between <lower_limit_str> and <upper_limit_str> timestamp bounds. 
        Returns a neo4j.work.result.Result object.
        '''
        try:
            ## Lateral acceleration query
            response_lat = self.session.run(f'''
                MATCH (participant:Participant {{id: {part_id} }})-[uses:USES]->
                    (equivital:Sensor {{name:'equivital'}})-[acc:ACC_LAT]->
                    (sample:HFsample)
                WHERE sample.timestamp > datetime({lower_limit_str}) AND 
                    sample.timestamp < datetime({upper_limit_str})
                RETURN sample.timestamp AS timestamp,
                    sample.value     AS lateral
                ORDER BY sample.timestamp
                ''')

            ## Longitudinal acceleration query
            response_lon = self.session.run(f'''
                MATCH (participant:Participant {{id: {part_id} }})-[uses:USES]->
                    (equivital:Sensor {{name:'equivital'}})-[acc:ACC_LON]->
                    (sample:HFsample)
                WHERE sample.timestamp > datetime({lower_limit_str}) AND 
                    sample.timestamp < datetime({upper_limit_str})
                RETURN sample.timestamp AS timestamp,
                    sample.value     AS longitudinal
                ORDER BY sample.timestamp
                ''')

            ## Vertical acceleration query
            response_ver = self.session.run(f'''
                MATCH (participant:Participant {{id: {part_id} }})-[uses:USES]->
                    (equivital:Sensor {{name:'equivital'}})-[acc:ACC_VER]->
                    (sample:HFsample)
                WHERE sample.timestamp > datetime({lower_limit_str}) AND 
                    sample.timestamp < datetime({upper_limit_str})
                RETURN sample.timestamp AS timestamp,
                    sample.value     AS vertical
                ORDER BY sample.timestamp
                ''')

        ## If any exception during query return False
        except Exception as error:
            print('Exception rised while querying acc data.')
            print(type(error).__name__, error.args)
            return False
        
        return response_lat, response_lon, response_ver

    def query_HR_data(self, part_id, lower_limit_str, upper_limit_str):
        '''Method which queries heart rate (HR) data from Participant <part_id> 
        id between <lower_limit_str> and <upper_limit_str> timestamp bounds. 
        Returns a neo4j.work.result.Result object.
        '''
        ## Query the HR data
        try:
            response = self.session.run(f'''
                MATCH (participant:Participant {{id: {part_id} }})-[uses:USES]->
                      (equivital:Sensor {{name:'activitymodule'}})-[hr:HR]->
                      (sample:LFsample)
                WHERE sample.timestamp > datetime({lower_limit_str}) AND 
                      sample.timestamp < datetime({upper_limit_str})
                RETURN sample.timestamp AS timestamp,
                       sample.value     AS HR
                ORDER BY sample.timestamp
                ''')
        ## If any exception during query return False
        except Exception as error:
            print('Exception rised while querying HR data.')
            print(type(error).__name__, error.args)
            return False
        
        return response



    def query_height_weight_data(self, part_id=None):
        '''Method which queries the height and weight data from Participant 
        <part_id> id if given. If part_id=None returns the BMI of all 
        Participant nodes.
        Returns a neo4j.work.result.Result object.
        '''
        if part_id is None:
            response = self.session.run('''
                MATCH (p:Participant) 
                RETURN p.id AS id, 
                    p.height AS height, 
                    p.weight AS weight
                ''')
        else:
            response = self.session.run(f'''
                MATCH (p:Participant {{id: {part_id}}}) 
                RETURN p.id AS id, 
                    p.height AS height, 
                    p.weight AS weight
                ''')

        return response


    ################### FUNCIONES MÁS FÁCILES PARA PLOTEAR (rafa) ########################       
    
    
    def query_lf_feature(self, feature_name, sensor_name):

        ## Query the fetaure data
        try:
            response = self.session.run(f'''
                MATCH (participant:Participant)-[uses:USES]->
                      (equivital:Sensor {{name:'{sensor_name}'}})-[mp:{feature_name}]->
                      (sample:LFsample)
                RETURN participant.id AS participant,
                       sample.timestamp AS timestamp,
                       sample.value     AS {feature_name}
                ORDER BY sample.timestamp
                ''')
        ## If any exception during query return False
        except Exception as error:
            print('Exception rised while querying MEAN_PRESS data.')
            print(type(error).__name__, error.args)
            return False
        
        return response

    
    def query_MP_data(self):
        '''Method which queries Mean Pressure (MP) data from Participant <part_id> 
        id between <lower_limit_str> and <upper_limit_str> timestamp bounds. 
        Returns a neo4j.work.result.Result object.
        '''
        ## Query the HR data
        try:
            response = self.session.run(f'''
                MATCH (participant:Participant)-[uses:USES]->
                      (equivital:Sensor {{name:'oscar'}})-[mp:MEAN_PRESS]->
                      (sample:LFsample)
                RETURN participant.id AS participant,
                       sample.timestamp AS timestamp,
                       sample.value     AS MEAN_PRESS
                ORDER BY sample.timestamp
                ''')
        ## If any exception during query return False
        except Exception as error:
            print('Exception rised while querying MEAN_PRESS data.')
            print(type(error).__name__, error.args)
            return False
        
        return response
    def query_acc_datav2(self, time):
        '''Method which queries acceleration data from Participant <part_id> id 
        between <lower_limit_str> and <upper_limit_str> timestamp bounds. 
        Returns a neo4j.work.result.Result object.
        '''
        try:
            ## Lateral acceleration query
            response_lat = self.session.run(f'''
                MATCH (participant:Participant)-[uses:USES]->
                    (equivital:Sensor {{name:'equivital'}})-[acc:ACC_LAT]->
                    (sample:HFsample)
                WHERE sample.timestamp.minute = 51 AND sample.timestamp.hour = 19
                RETURN participant.id AS participant,
                    sample.timestamp AS timestamp,
                    sample.value     AS lateral
                ORDER BY sample.timestamp
                ''')

            ## Longitudinal acceleration query
            response_lon = self.session.run(f'''
                MATCH (participant:Participant)-[uses:USES]->
                    (equivital:Sensor {{name:'equivital'}})-[acc:ACC_LON]->
                    (sample:HFsample)
                WHERE sample.timestamp.minute = 51 AND sample.timestamp.hour = 19
                RETURN participant.id AS participant,
                    sample.timestamp AS timestamp,
                    sample.value     AS lateral
                ORDER BY sample.timestamp
                ''')

            ## Vertical acceleration query
            response_ver = self.session.run(f'''
                MATCH (participant:Participant)-[uses:USES]->
                    (equivital:Sensor {{name:'equivital'}})-[acc:ACC_VER]->
                    (sample:HFsample)
                WHERE sample.timestamp.minute> 51 AND sample.timestamp.hour = 19
                RETURN participant.id AS participant,
                    sample.timestamp AS timestamp,
                    sample.value     AS lateral
                ORDER BY sample.timestamp
                ''')

        ## If any exception during query return False
        except Exception as error:
            print('Exception rised while querying acc data.')
            print(type(error).__name__, error.args)
            return False
        
        return response_lat, response_lon, response_ver


    