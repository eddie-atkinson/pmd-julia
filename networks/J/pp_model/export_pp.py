import pandapower as pp
import pandas as pd
import json

def write_json(dict, filename):
    with open(filename, "w") as outfile:
        outfile.write(json.dumps(dict))

def read_json(filename):
    with open(filename, "r") as infile:
        return json.loads(infile.read())

bus_index_map = pd.read_csv("bus_index_map.csv")

load_names_with_solar= [
    "2",
    "3",
    "4",
    "5",
    "7",
    "8",
    "10",
    "11",
    "14",
    "19",
    "20",
    "21",
    "24",
    "25",
    "26",
    "29",
    "30",
    "33",
    "34",
    "35",
    "38",
    "39",
    "40",
    "42",
    "46",
    "50",
    "55",
    "58",
    "61",
    "63",
    "64",
    "66",
    "67",
    "71",
    "72",
    "74",
    "76",
    "81",
    "83",
    "84",
    "85",
    "86",
]
bus_name_index_map = {v:k for (k, v) in ({**bus_index_map.to_dict(orient="records")[0]}.items())}

net = pp.from_pickle("model.p") 
asymmetric_load = net["asymmetric_load"]
asymmetric_sgen = net["asymmetric_sgen"]

sgens = {}
loads = {}


loads_json = read_json("loads.json")
loads_info = dict()

for i, load in enumerate(loads_json):
    loads_info[i] = load

for i, row  in enumerate(asymmetric_load.to_dict(orient="records")): 
    phase = row["name"]
    bus = row["bus"]
    bus_name = bus_name_index_map[bus]
    loads[loads_info[i]["name"]] = {
        "phase": phase,
        "bus_index": bus,
        "bus_name": bus_name,
        "index": i,
    }

for i, row in enumerate(asymmetric_sgen.to_dict(orient="records")): 
    phase = row["name"]
    bus = row["bus"]
    bus_name = bus_name_index_map[bus]
    load_name = load_names_with_solar[i]
    index = loads[load_name]["index"]
    sgens[loads_info[index]["name"]] = {
        "phase": phase,
        "bus_index": bus,
        "bus_name": bus_name,
        # We are just naively assigning sgens to profiles by index in pp anyway
        # Basically the 0th sgen gets attached to the 0th profile even if the mapping didn't work that way
        "index": i
    }


write_json(bus_name_index_map, "bus_index_name_map.json")
write_json(loads, "loads_pp.json")
write_json(sgens, "sgens_pp.json")

