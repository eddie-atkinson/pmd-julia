using JSON
using PowerModelsDistribution
import Ipopt
using PyCall
using JuMP

# Include local modules
include("helpers.jl")

"""
Stuff that needs to be done:
* need to figure out how to constrain power factor
* need to figure out how to run results and dump them efficiently into a file
"""
network_name = "J"
NUM_TS_ITERATIONS = 161280
CHUNK_SIZE = 1

network_path = joinpath((pwd(), "networks", network_name, "Master.dss"))

network_model = parse_file(network_path)

# Remove the existing voltage bounds
remove_all_bounds!(network_model)

# Add back the voltage bounds we care about
add_bus_absolute_vbounds!(
    network_model,
    phase_lb_pu=0.9,
    phase_ub_pu=1.1,
)

move_loads_to_pp_phase(network_model, network_name)
pp_model = load_pp_pickle_model_sgen_json(network_name)
add_solar_network_model(network_model, pp_model, network_name)


# TODO: What the do the numbers in this vector even mean?
# Not sure, but this should make grid energy more expensive than solar
network_model["voltage_source"]["source"]["cost_pg_parameters"] = [0, 1, 0]





# write_json(sol_eng, "res.json")

start_index = 1
data_base_path = joinpath(pwd(), "input_data")
pv_path = joinpath(data_base_path, "test_pv.csv")
active_path = joinpath(data_base_path, "test_active.csv")
reactive_path = joinpath(data_base_path, "test_reactive.csv")

pv_df = CSV.read(pv_path, DataFrame)
active_df = CSV.read(active_path, DataFrame)
reactive_df = CSV.read(reactive_path, DataFrame)

# Drop datetime, we don't need it
active_df = active_df[:, Not(:datetime)]
reactive_df = reactive_df[:, Not(:datetime)]
pv_df = pv_df[:, Not(:datetime)]

# The data is in MWh we need it in kWh
pv_df = pv_df .* 1000
active_df = active_df .* 1000
reactive_df = reactive_df .* 1000

# for range_end = range(CHUNK_SIZE, stop=NUM_TS_ITERATIONS, step=CHUNK_SIZE)

add_time_series(network_name, network_model, active_df, reactive_df, pv_df, 1800, 1802)

# Convert to mathematical representation
data_math = transform_data_model(network_model, kron_reduce=false, phase_project=false, multinetwork=true)


# Infer no load voltages
add_start_vrvi!(data_math)

model = instantiate_mc_model(data_math, IVRENPowerModel, build_mn_mc_opf)
res = optimize_model!(model, optimizer=Ipopt.Optimizer)
res_math = res["solution"]

sol_eng = transform_solution(res_math, data_math, make_si=true)

# Start from next index to go
# global start_index = range_end + 1
# break
# end

write_json(sol_eng, "res.json")
