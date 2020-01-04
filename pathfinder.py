from dbconnection import Connection
from mplogger import *

import googlemaps

gmaps = googlemaps.Client(key='')


c = Connection(sender_config)
data = c.execute('SELECT Parkruns.ParkrunID, Parkruns.ParkrunName, Regions.Region, Countries.Country, Parkruns.Latitude, Parkruns.Longitude FROM Parkruns INNER JOIN Regions ON dbo.Parkruns.RegionID = Regions.RegionID INNER JOIN Countries ON Regions.CountryID = Countries.CountryID WHERE Parkruns.LocationType IS NULL')

for row in data:
    geocode_result = gmaps.geocode('{} parkrun, {}, {}'.format(row['ParkrunName'], row['Region'], row['Country']))
    #print('result for {} parkrun returned {} results.'.format(row['ParkrunName'],len(geocode_result)))
    if len(geocode_result)>0:
        #pick out the data we want:
        lat = geocode_result[0]['geometry']['location']['lat']
        lng = geocode_result[0]['geometry']['location']['lng']
        locationType = geocode_result[0]['geometry']['location_type']
        address = geocode_result[0]['formatted_address'].replace("'","''") #.split(',')[0]
        postcode = None
        for i in geocode_result[0]['address_components']:
            if i['types'][0] == 'locality':
                suburb = i['long_name'].replace("'","''")
            if i['types'][0] == 'postal_code':
                postcode = i['long_name']
        sql = "UPDATE Parkruns SET Latitude = {}, Longitude = {}, LocationType = '{}', Address = '{}', Suburb = '{}'".format(lat, lng, locationType, address, suburb)
        if postcode is None:
            sql += ", Postcode = NULL"
        else:
            sql += ", Postcode = '{}'".format(postcode)
        sql += " WHERE ParkrunID = {}".format(row['ParkrunID'])
        c.execute(sql)
        print('Updated {} parkrun to LAT:{}, LNG:{}'.format(row['ParkrunName'], lat, lng))
    
data = c.execute('SELECT AthleteID, GivenAddress, Latitude, Longitude, State, Country FROM Athletes WHERE GivenAddress IS NOT NULL')
for row in data:
    if row['State'] is None:
        geocode_result = gmaps.geocode(row['GivenAddress'])
        if len(geocode_result)>0:
            lat = geocode_result[0]['geometry']['location']['lat']
            lng = geocode_result[0]['geometry']['location']['lng']
            locationType = geocode_result[0]['geometry']['location_type']
            address = geocode_result[0]['formatted_address'].replace("'","''") #.split(',')[0]
            postcode = None
            streetNumber = None
            streetName = None
            for i in geocode_result[0]['address_components']:
                if i['types'][0] == 'locality':
                    suburb = i['long_name'].replace("'","''")
                if i['types'][0] == 'administrative_area_level_1':
                    state = i['long_name'].replace("'","''")
                if i['types'][0] == 'country':
                    country = i['long_name'].replace("'","''")
                if i['types'][0] == 'postal_code':
                    postcode = i['long_name']
                if i['types'][0] == 'route':
                    streetName = i['long_name'].replace("'","''")
                if i['types'][0] == 'street_number':
                    streetNumber = i['long_name'].replace("'","''")
            sql = "UPDATE Athletes SET Latitude = {}, Longitude = {}, LocationType = '{}', FormattedAddress = '{}', Suburb = '{}', State = '{}', Country = '{}'".format(lat, lng, locationType, address, suburb, state, country)
            if postcode is not None:
                sql += ", Postcode = '{}'".format(postcode)
            if streetNumber is not None:
                sql += ", StreetNumber = '{}'".format(streetNumber)
            if streetName is not None:
                sql += ", StreetName = '{}'".format(streetName)
            sql += " WHERE AthleteID = {}".format(row['AthleteID'])
            #print(sql)
            c.execute(sql)
            print('Updated athleteID {} to LAT:{}, LNG:{}'.format(row['AthleteID'], lat, lng))
    else:
        state = row['State']
        country = row['country']
        orig = {'lat':row['Latitude'], 'lng':row['Longitude']}
        #Pull a list of parkruns and their locations in the country
        parkruns = c.execute("SELECT ParkrunID, Latitude, Longitude FROM Parkruns INNER JOIN Regions ON Parkruns.RegionID = Regions.RegionID INNER JOIN ParkrunTypes ON Parkruns.ParkrunTypeID = ParkrunTypes.ParkrunTypeID INNER JOIN Countries ON Regions.CountryID = Countries.CountryID WHERE ParkrunType = 'Standard' AND Countries.Country = '{}'".format(country))
        print("{} parkruns found in athletes home country of {}".format(len(parkruns), country))
        #Pull a list of parkruns with existing distance info to the athlete
        parkrunDistances = c.execute("SELECT AthleteID, ParkrunID, Distance, Duration FROM AthleteParkrunDistance WHERE AthleteID = {} AND Distance > 0".format(row['AthleteID']))
        parkrunDistances = [x['ParkrunID'] for x in parkrunDistances]
        #Remove all parkruns from the list that have already been measured
        parkruns = [x for x in parjruns if x['ParkrunID'] not in parkrunDistances]
        print("{} parkruns to be measured".format(len(parkruns)))
        
        
    while len(parkruns)>0:
        dests = {}
        dests['ID'] = [x['ParkrunID'] for x in parkruns[:100]]
        dests['location'] = [{'lat':x['Latitude'], 'lng':x['Longitude']} for x in parkruns[:100]]
        d = gmaps.distance_matrix(origins = orig, destinations = dests['location'])
        dests['distance'] = [x['distance']['value'] if x['status'] == 'OK' else 0 for x in d['rows'][0]['elements']]
        dests['duration'] = [x['duration']['value'] if x['status'] == 'OK' else 0 for x in d['rows'][0]['elements']]
        for i in range(len(dests['ID'])):
            try:
                sql = "INSERT INTO AthleteParkrunDistance(AthleteID, ParkrunID, Distance, Duration) VALUES ({}, {}, {}, {})".format(row['AthleteID'], dests['ID'][i], dests['distance'][i], dests['duration'][i])
                c.execute(sql)
            except:
                sql = "UPDATE AthleteParkrunDistance SET Distance = {}, Duration = {} WHERE AthleteID = {} AND ParkrunID = {}".format(dests['distance'][i], dests['duration'][i], row['AthleteID'], dests['ID'][i])
                c.execute(sql)
        parkruns = parkruns[100:]
            
