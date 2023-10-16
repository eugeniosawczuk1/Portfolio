import json

class Service:

    def __init__(self):
        '''
            This class manages the auth for the api.
        '''
        self.api_key = json.load(open('weather_api/credentials.json', 'r'))['api_key']