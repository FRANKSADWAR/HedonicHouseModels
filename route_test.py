import traceback
from django.contrib.gis import db
from django.db import connection,connections
from geojson import loads,Feature,FeatureCollection
import geojson
import psycopg2
import json
import math
import logging

logger = logging.getLogger(__name__)

database = 'routes_test'
user = 'postgres'
password = 'RootRender90'

conn = psycopg2.connect(database=database,user=user,password=password)

def getNearest(x,y):
    """
    params : x
           : y
    using the k-nearest neighbour to get the nearest vertex given a set of coordinates       
    """
    query = """
            SELECT id FROM ways_noded_vertices_pgr  ORDER BY 
            the_geom <-> ST_SetSRID(ST_Point(%s,%s),4326) LIMIT 1;
            """
    cur = conn.cursor()
    cur.execute(query,(x,y))
    point = cur.fetchone()
    return point

def getShortest(gid):
    """
    This query runs the pgr_dijkstra algorithm to return the shortest route between
    start and end nodes.
    param gid:  id of the nearest vertex to the store returned by the getNearest function
    """
    start_id = getNearest(36.852721,-1.313261)
    query_sho = """
                SELECT dijkstra.*,ST_AsGeoJSON(ways_noded.the_geom) as route_geom,ways_noded.length FROM 
                pgr_dijkstra('SELECT id,source,target,cost,reverse_cost FROM ways_noded',85038,%s)
                AS dijkstra LEFT JOIN ways_noded ON (edge=id);   
                """
    
    cur = conn.cursor()
    cur.execute(query_sho,gid)
    data = cur.fetchall()
    
    last_row = data.pop() # remove the last row since it has no geometry information and also has a NoneType value for length

    route_result = []
    route_length = []
    total_cost = last_row[5]  # the index 5 of the last row gives the aggregate cost of the route, get it as the total cost of the route
    
    # iterate over the query results to get the route geometryand length
    for seg in data:
        lens = seg[7]         # get the  length value of each segment
        seg_geom = seg[6]     # get the geometry value of each segment
        
        seg_geom_geojs = loads(seg_geom)   # load the geometry as geojson, the geometry is Type string, so it allows loading as geojson
        seg_feature = Feature(geometry=seg_geom_geojs,properties={})   # create a feature of the loaded geometries
        
        route_length.append(lens)         # append all the length values to this list in order to sum them later
        route_result.append(seg_feature)  # append all the created features to create a feature collection later
    
    length_in_km = round(sum(route_length),2)
    
    # create a feature collection from the features returned
    route_details = FeatureCollection(route_result,distance_in_Km=length_in_km,time_in_minutes=total_cost)
    try:
        return str(route_details)
    except:
        logger.error("Error while creating GeoJSON file" + str(route_details))
        logger.error(traceback.format_exc()) 

def getStoreXY():
    query = """
            SELECT x,y from stores; 
            """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows

if __name__ == "__main__":
    fr = getNearest(36.940559,-1.368009)
    #if run as top level module
    dataset = getShortest(fr)
    print(dataset)
    


   




    

