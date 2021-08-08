import csv
from shapely.geometry import Point, Polygon
from datetime import datetime , timezone
import haversine as hs
import pytz
import random
import json
import sys
import asyncio
from azure.eventhub.aio import EventHubProducerClient 
from azure.eventhub import EventData


### Default Values
#print(pytz.all_timezones)
tz = pytz.timezone('Asia/Kolkata') # Setting default timezone to Asia/Kolata, get list of timezones at: pytz.all_timezones
routes_csv_file_path = '../bmtc/data/bmtc_routes_map.csv'
headers_present = True
#routes = {}
bus_routes = []
traffic_lights = []
traffic_congestion = []
start_time = 5
def generate_random_latlon_betweenlatlons(start_latlong, end_latlong):
    start_long = float(start_latlong[1])
    start_lat = float(start_latlong[0])
    end_long = float(end_latlong[1])
    end_lat = float(end_latlong[0])
    random_latlong = []
    random_long = random.uniform(start_long,end_long)
    random_lat = random.uniform(start_lat,end_lat)
    random_latlong.append(random_lat)
    random_latlong.append(random_long)
    return random_latlong
        

def generate_traffic_lights(route_id,bus_stops):    
    traffic_lights_busstops = []
    traffic_lights_route = {}
    traffic_lights_route["route_id"] =  route_id
    n = 0    
    ### Load The Routes Map CSV File
    bus_stops_json = json.loads(bus_stops)
    
    ## Build Traffic Light against bus stops
    while n < (len(bus_stops_json)-2):      
        start_latlong = bus_stops_json[n]["latlons"]        
        end_latlong = bus_stops_json[n+1]["latlons"]
        no_traffic_light = random.randint(0,6)
        traffic_light_detail = {"start_latlong":start_latlong,"end_latlong":end_latlong,"no_red_light":no_traffic_light+1}
        list_traffic_red_lights = []   
        t_l = {}
        t = 0        
        while t <= no_traffic_light:                   
            random_latlong = generate_random_latlon_betweenlatlons(start_latlong,end_latlong)
            list_traffic_red_lights.append({"traffic_light_latlong":random_latlong,"delay_in_mins":random.uniform(2,30.6)})            
            t = t + 1
        traffic_light_detail["list_traffic_red_lights"] =  list_traffic_red_lights
        traffic_lights_busstops.append(traffic_light_detail)
        n = n + 1
    traffic_lights_route["traffic_lights_busstops"] = traffic_lights_busstops
    return traffic_lights_route


def generate_random_congestion(route_id,bus_stops):
    congestion = {}
    congestion["route_id"] = route_id
    congestion_details = []
    n = 0
    bus_stops_json = json.loads(bus_stops)
    if len(bus_stops_json) > 2:
        congestion_no = random.randrange(1,len(bus_stops_json)-1)
    
        while n <= congestion_no:
            x = random.randrange(0,len(bus_stops_json)-2)
            start_latlong = bus_stops_json[x]["latlons"]
            end_latlong = bus_stops_json[x+1]["latlons"]                      
            random_latlong = generate_random_latlon_betweenlatlons(start_latlong,end_latlong)
            congestion_detail = {}
            congestion_detail["latlong"] = random_latlong
            congestion_detail["delay_in_mins"] = random.uniform(10.5,30.6)
            congestion_detail["start_latlong"] = start_latlong
            congestion_detail["end_latlong"] = end_latlong
            congestion_details.append(congestion_detail)
            n = n + 1
    congestion["congestion_details"] = congestion_details    
    return congestion
    
async def send_messages_eh(messages):
    eh_conn_string = "Endpoint=sb://bmtc-live.servicebus.windows.net/;SharedAccessKeyName=send-data;SharedAccessKey=5HghBjf/QoxcxpwLYky9gRYPcPN0AhSa5VBQbnQsM7E=;EntityPath=live-bus-data"
    producer = EventHubProducerClient.from_connection_string(eh_conn_string)
    async with producer:
        event_data_batch = await producer.create_batch()
        for message in messages:
            event_data_batch.add(EventData(str(message)))
        await producer.send_batch(event_data_batch)

def main(mode):

    with open(routes_csv_file_path) as routes_csv_file:
        csv_routes = csv.reader(routes_csv_file,delimiter=',')
        line_count = 0
        for row in csv_routes:
            if len(row[3]) > 0 and len(row[0]) > 0 and len(row[2]) > 0:                
                if headers_present == True and line_count == 0:
                    routes_headers = row
                    line_count = line_count + 1            
                else:
                    #routes["route_id"] = row[0]
                    route_times = str(row[2]).split(',')
                    
                    ### Process to load bus routes by routeno and time in bus_routes
                    for route_time in route_times:
                        routes = {}                                                           
                        routes["route_id"] = row[0] + "_" + str(route_time).replace(':','').strip()                    
                        routes["route_no"] = row[0]
                        routes["route_start_time"] = str(route_time).strip()                        
                        routes["bus_stops"] = row[3]
                        bus_routes.append(routes)
                    line_count = line_count + 1
    
   ### For Each Route & Time Repeat below
    for route in bus_routes:
        ### Generate Random Traffic Lights - GenerateTrafficLight(route_id,route_start_time,bus_stops (latlong, time)) => route_id, red_lights
        
        traffic_lights.append(generate_traffic_lights(route_id=route["route_id"],bus_stops=route["bus_stops"]))        
        ### Generate Random Congestion - GenerateCongestionData(route_id,route_start_time,bus_stops(latlong,time)) => route_id, congestion_blocks

        traffic_congestion.append(generate_random_congestion(route_id=route["route_id"],bus_stops=route["bus_stops"]))      
        
    ### 
    
    
    
    print(traffic_lights[90])
    print(len(traffic_lights))
    print("--------")    
    print(traffic_congestion[20])    
    print(len(traffic_congestion))
    print("---------")

    
    if mode == "live":
        ### Cron job will be set to run every five minutes
        messages = []
        ## Start Time 5:00 AM
        # For each route in routes
        for route in bus_routes:            
            
            # For each route:  the start time is greater or equal to current time
            
            now = datetime.now(tz)
            route_start_time = datetime.strptime(route["route_start_time"],'%H:%M').replace(tzinfo=tz)
            if now >= route_start_time:
                # Identify potential bus stop by - avg speed Rand(20,60); distance travelled between start time and current time ; find latlong
                rand_speed_in_kmph = random.uniform(20,40)
                time_diff_hours = (now - route_start_time).seconds/3600
                distance_traveled = rand_speed_in_kmph * time_diff_hours
                i = 0
                found_closest_bus_stop = False
                origin_latlon = (0,0)
                dest_latlon = (0,0)
                calculated_bus_stop = {}
                bus_stop_from_index = -1
                bus_stop_to_index = -1
                bus_stops = json.loads(route["bus_stops"])
                previous_bus_stop = None
                for bus_stop in bus_stops:                    
                    if i == 0:
                        latlons = bus_stop["latlons"]
                        origin_latlon = (float(latlons[0]),float(latlons[1]))
                        previous_bus_stop = latlons
                        i = i + 1
                    else:
                        latlons = bus_stop["latlons"]
                        dest_latlon = (float(latlons[0]),float(latlons[1]))
                        dist_in_kms = hs.haversine(origin_latlon,dest_latlon)
                        if dist_in_kms >= distance_traveled and found_closest_bus_stop == False:
                            found_closest_bus_stop = True
                            bus_stop_from_index = i - 1
                            bus_stop_to_index = i
                        else:
                            previous_bus_stop = latlons
                        i = i + 1
                if bus_stop_from_index > -1 and bus_stop_to_index > -1:
                    bus_stops = json.loads(route["bus_stops"])
                    bus_stop_from = bus_stops[bus_stop_from_index]["latlons"]
                    bus_stop_from_name = bus_stops[bus_stop_from_index]["busstop"]
                    bus_to = bus_stops[bus_stop_to_index]["latlons"]
                    bus_to_name = bus_stops[bus_stop_to_index]["busstop"]
                    # Get a random position between the from and to bus stop
                    bus_live_latlong = generate_random_latlon_betweenlatlons(bus_stop_from,bus_to)

                    # Get traffic light delay -  Against the previous bus stop 
                    traffic_light_delay_latlong = None
                    traffic_red_light_detail = None
                    no_red_light = 0
                    for traffic_light_route in traffic_lights:
                        if route["route_id"] == traffic_light_route["route_id"]: 
                            #print("Route ID found" + traffic_light_route["route_id"])
                            traffic_lights_busstops = traffic_light_route["traffic_lights_busstops"]
                            #print("traffic  lights busstops " + str(traffic_light_route["traffic_lights_busstops"] ))
                            for traffic_lights_busstop in traffic_lights_busstops:
                                if traffic_lights_busstop["start_latlong"][0] == bus_stop_from[0] and traffic_lights_busstop["start_latlong"][1] == bus_stop_from[1] and traffic_lights_busstop["end_latlong"][0] == bus_to[0] and traffic_lights_busstop["end_latlong"][1] == bus_to[1]:
                                    traffic_red_light_detail = traffic_lights_busstop["list_traffic_red_lights"]
                                    no_red_light = traffic_lights_busstop["no_red_light"]
                    # Get Random congestion delay - Against the previous bus stop
                    traffic_congestion_delay_latlong = None
                    traffic_congestion_total_delay_mins = 0                
                    for t_c in traffic_congestion:
                        if route["route_id"] == t_c["route_id"]:
                            congestion_details = t_c["congestion_details"]
                            for congestion in congestion_details:                    
                                if congestion["start_latlong"][0] == bus_stop_from[0] and congestion["start_latlong"][1] == bus_stop_from[1] and congestion["end_latlong"][0] == bus_to[0] and congestion["end_latlong"][1] == bus_to[1]:
                                    traffic_congestion_delay_latlong = congestion["latlong"]
                                    traffic_congestion_total_delay_mins = congestion["delay_in_mins"]
                    # Generate - Current latlong position, Start delay , estimated time to next bus stop, traffic light wait in mins, congestion in mins, unknown delay in mins, Capacity percentage (20-120)
                    capacity = random.uniform(30,120)                    
                    message = {"route_id": route["route_id"], "route_no": route["route_no"], "bus_stop_from_loc":bus_stop_from, "bus_stop_from_name":bus_stop_from_name, "bus_to_loc":bus_to, "bus_to_name":bus_to_name , "live_loc_latlong":bus_live_latlong,"traffic_red_light_detail":traffic_red_light_detail,"no_red_light":no_red_light, "congestion_delay_in_mins":traffic_congestion_total_delay_mins,"max_congestion_loc":traffic_congestion_delay_latlong,"capacity":capacity}
                    print("Message: " + str(message))
                    print("------------------------------------")
                    messages.append(message)

                else:
                    test = 1
                    #print("skipping bus route as it is not yet time")    
            else:
                print("skipping bus route as it is not yet time")
        
        ### Send Message to Event Hub
        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_messages_eh(messages=messages))
        
            
            
             

        ## Add capacity
    elif mode == "batch":
        print("batch")

    ## Generate Expected Scheduled times for bus stops

    ### Generate Messages to be sent -> Save to folder




if __name__ == "__main__":
    if len(sys.argv) > 1:
        mode = sys.argv[1]
        if mode == "live" or mode == "batch":
            main(mode)
        else:
            print("Invalid Arguments passed, please enter mode acceptable values: live, batch")
    else:
        print("No Arguments passed, please enter mode acceptable values: live, batch")
