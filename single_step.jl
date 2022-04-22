using PowerModelsDistribution
import Ipopt
include("helpers.jl")
include("reduction.jl")

network_name = "J"
# DEFAULT_VBASE = 0.230

network_path = joinpath((pwd(), "networks", network_name, "Master.dss"))

network_model = parse_file(network_path)

# Set power base to 1 kW
network_model["settings"]["sbase_default"] = 1

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


# cost_pg_parameters = [quadratic, linear, constant]
# NB: constant term doesn't matter
network_model["voltage_source"]["source"]["cost_pg_parameters"] = [0.0, 1.0, 0.0]


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

# The data is in MW we need it in kW
pv_df = pv_df .* 1000
active_df = active_df .* 1000
reactive_df = reactive_df .* 1000

bus_index_name_map = get_bus_index_name_map(network_name)
bus_name_index_map = Dict(value => key for (key, value) in bus_index_name_map)

add_time_series_single_step(network_model, bus_name_index_map, active_df, reactive_df, pv_df, 1800)
# rm_trailing_lines_eng!(network_model)
reduce_lines_eng!(network_model)
data_math = transform_data_model(network_model)
data_math = reduce_lines_math(data_math)

model = instantiate_mc_model(data_math, IVRUPowerModel, build_mc_opf)
res = optimize_model!(model, optimizer=Ipopt.Optimizer)
res_math = res["solution"]
sol_eng = transform_solution(res_math, data_math, make_si=true)
write_json(sol_eng, "res-single-step.json")