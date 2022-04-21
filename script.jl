using Distributed
addprocs(4)
@everywhere using PowerModelsDistribution
@everywhere import Ipopt
using PyCall

# Include local modules
@everywhere include("helpers.jl")

"""
Stuff that needs to be done:
* need to figure out how to constrain power factor
* need to figure out how to run results and dump them efficiently into a file
"""
network_name = "J"
NUM_TS_ITERATIONS = 5
CHUNK_SIZE = 2
RUN_DISTRIBUTED = false
DEFAULT_VBASE = 0.230

network_path = joinpath((pwd(), "networks", network_name, "Master.dss"))

network_model = parse_file(network_path)

# Remove the existing voltage bounds
remove_all_bounds!(network_model)


network_model["settings"]["vbases_default"] = Dict{String,Real}()
# Add explicit base voltage to each bus to prevent calc voltage bases shenanigans
vbases = network_model["settings"]["vbases_default"]
for bus_id in keys(network_model["bus"])
    vbases[bus_id] = DEFAULT_VBASE
end

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

@everywhere function run_ts_model(network_name::String, network_model::Dict{String,Any}, start_index, end_index, active_df, reactive_df, pv_df)
    add_time_series(network_name, network_model, active_df, reactive_df, pv_df, start_index, end_index)
    data_math = transform_data_model(network_model, multinetwork=true)
    model = instantiate_mc_model(data_math, IVRUPowerModel, build_mn_mc_opf)
    res = optimize_model!(model, optimizer=Ipopt.Optimizer)
    res_math = res["solution"]

    sol_eng = transform_solution(res_math, data_math, make_si=true)
    write_results(network_name, sol_eng, "res-$end_index")
    sol_eng
end


if RUN_DISTRIBUTED
    @sync @distributed for range_end = range(start=CHUNK_SIZE, stop=NUM_TS_ITERATIONS, step=CHUNK_SIZE)
        start_idx = range_end - (CHUNK_SIZE - 1)
        run_ts_model(network_name, network_model, start_idx, range_end, active_df, reactive_df, pv_df)
        nothing
    end
else
    print("Running serial\n")
    bus_index_name_map = get_bus_index_name_map(network_name)
    bus_name_index_map = Dict(value => key for (key, value) in bus_index_name_map)
    add_time_series_single_step(network_model, bus_name_index_map, active_df, reactive_df, pv_df, 1800)
    data_math = transform_data_model(network_model, multinetwork=true)
    model = instantiate_mc_model(data_math, IVRUPowerModel, build_mn_mc_opf)
    res = optimize_model!(model, optimizer=Ipopt.Optimizer)
    res_math = res["solution"]
    sol_eng = transform_solution(res_math, data_math, make_si=true)
    write_results(network_name, sol_eng, "res-single-step")
    write_json(sol_eng, "res-single-step.json")
end
