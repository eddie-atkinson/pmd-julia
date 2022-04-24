import pandapower as pp
import pandas as pd
import json

def write_json(dict, filename):
    with open(filename, "w") as outfile:
        outfile.write(json.dumps(dict))

bus_index_map = pd.read_csv("bus_index_map.csv")

bus_name_index_map = {v:k for (k, v) in ({**bus_index_map.to_dict(orient="records")[0]}.items())}

net = pp.from_pickle("model.p") 
asymmetric_load = net["asymmetric_load"]
asymmetric_sgen = net["asymmetric_sgen"]

sgens = []
loads = []
for row in asymmetric_load.to_dict(orient="records"): 
    phase = row["name"]
    bus = row["bus"]
    bus_name = bus_name_index_map[bus]
    loads.append({
        "phase": phase,
        "bus_index": bus,
        "bus_name": bus_name
    })

for row in asymmetric_load.to_dict(orient="records"): 
    phase = row["name"]
    bus = row["bus"]
    bus_name = bus_name_index_map[bus]
    sgens.append({
        "phase": phase,
        "bus_index": bus,
        "bus_name": bus_name
    })


write_json(bus_name_index_map, "bus_index_name_map.json")
write_json(loads, "loads.json")
write_json(sgens, "sgens.json")

