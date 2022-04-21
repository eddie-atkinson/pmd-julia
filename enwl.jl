using PowerModelsDistribution
import Ipopt
include("helpers.jl")

# network_name = "N4F4"
network_name = "N13F3"
DEFAULT_VBASE = 0.230

network_path = joinpath((pwd(), "networks", network_name, "Master.dss"))

network_model = parse_file(network_path, kron_reduce=false)


# Remove the existing voltage bounds
remove_all_bounds!(network_model)

reduce_lines!(network_model)

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



for (load_name, load) in network_model["load"]
    gen_bus_name = load["bus"]
    gen_phases = load["connections"]
    add_solar!(
        network_model,
        "$load_name",
        gen_bus_name,
        gen_phases,
        configuration=WYE,
        pg_lb=[0.0],
        pg_ub=[6.90],
        qg_lb=[-Inf],
        qg_ub=[Inf],
        # Quadratic, linear, constant
        cost_pg_parameters=[0, -1, 0]
    )
end


# cost_pg_parameters = [quadratic, linear, constant]
# NB: constant term doesn't matter
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

load_buses = [load["bus"] for (load_name, load) in network_model["load"]]
bus_names = filter(x -> x !== "sourcebus", sort(load_buses))
bus_name_index_map = Dict{String,Int64}(bus_name => i - 1 for (i, bus_name) in enumerate(bus_names))

add_time_series_single_step(network_model, bus_name_index_map, active_df, reactive_df, pv_df, 1800)
# Needed to remove loops to get it to run
transform_loops!(network_model)
data_math = transform_data_model(network_model, kron_reduce=false, phase_project=false)
# Added back no load version
add_start_vrvi!(data_math)
# Note using explicit neutral model
model = instantiate_mc_model(data_math, IVRENPowerModel, build_mn_mc_opf)
res = optimize_model!(model, optimizer=Ipopt.Optimizer)
res_math = res["solution"]
sol_eng = transform_solution(res_math, data_math, make_si=true)
write_json(sol_eng, "res-single-step.json")