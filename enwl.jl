using PowerModelsDistribution
import Ipopt
include("helpers.jl")
include("reduction.jl")

# network_name = "N4F4"
network_name = "N13F3"
network_path = joinpath((pwd(), "networks", network_name, "Master.dss"))

network_model = parse_file(network_path, kron_reduce=false)

network_model["is_kron_reduced"] = true
network_model["settings"]["sbase_default"] = 1

# Remove the existing voltage bounds
remove_all_bounds!(network_model)


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
        pg_ub=[0.5],
        qg_lb=[-0.5],
        qg_ub=[0.5],
        # Quadratic, linear, constant
        cost_pg_parameters=[0.0, -1.0, 0.0]
    )
    break
end


# cost_pg_parameters = [quadratic, linear, constant]
# NB: constant term doesn't matter
network_model["voltage_source"]["source"]["cost_pg_parameters"] = [0.0, 1.0, 0.0]


# data_base_path = joinpath(pwd(), "input_data")
# pv_path = joinpath(data_base_path, "test_pv.csv")
# active_path = joinpath(data_base_path, "test_active.csv")
# reactive_path = joinpath(data_base_path, "test_reactive.csv")

# pv_df = CSV.read(pv_path, DataFrame)
# active_df = CSV.read(active_path, DataFrame)
# reactive_df = CSV.read(reactive_path, DataFrame)

# # Drop datetime, we don't need it
# active_df = active_df[:, Not(:datetime)]
# reactive_df = reactive_df[:, Not(:datetime)]
# pv_df = pv_df[:, Not(:datetime)]

# # The data is in MW we need it in kW
# pv_df = pv_df .* 1000
# active_df = active_df .* 1000
# reactive_df = reactive_df .* 1000

load_buses = [load["bus"] for (load_name, load) in network_model["load"]]
bus_names = filter(x -> x !== "sourcebus", sort(load_buses))
bus_name_index_map = Dict{String,Int64}(bus_name => i - 1 for (i, bus_name) in enumerate(bus_names))

# add_time_series_single_step(network_model, bus_name_index_map, active_df, reactive_df, pv_df, 1800)
# Needed to remove loops to get it to run
transform_loops!(network_model)

# rm_trailing_lines_eng!(network_model)
reduce_lines_eng!(network_model)

data_math = transform_data_model(network_model, kron_reduce=false, phase_project=false)

for (l, load) in data_math["load"]
    load["connections"] = filter(x -> x != 4, load["connections"])
end

for (g, gen) in data_math["gen"]
    if ~occursin("voltage_source", gen["source_id"])
        gen["connections"] = filter(x -> x != 4, gen["connections"])
        continue
    end
    gen_bus = gen["gen_bus"]

    gen_branch = findfirst(b -> b["f_bus"] == gen_bus, data_math["branch"])
    gen["gen_bus"] = data_math["branch"][gen_branch]["t_bus"]
    delete!(data_math["branch"], gen_branch)
    delete!(data_math["bus"], gen_bus)
    gen["connections"] = filter(x -> x != 4, gen["connections"])
    gen["connections"] = gen["connections"][1:3]
    gen["vg"] = gen["vg"][1:3]

    gen["pg"] = gen["pg"][1:3]

    gen["qg"] = gen["qg"][1:3]

    gen["pmax"] = gen["pmax"][1:3]

    gen["pmin"] = gen["pmin"][1:3]

    gen["qmax"] = gen["qmax"][1:3]

    gen["qmin"] = gen["qmin"][1:3]

    gen["cost"] = 1000 .* gen["cost"]
    gen["pmin"] = [0.0, 0.0, 0.0]
    gen["pmax"] = [1.0, 1.0, 1.0]
    gen["qmin"] = [-1.0, -1.0, -1.0]
    gen["qmax"] = [1.0, 1.0, 1.0]
end

delete!(data_math["bus"], "7")
data_math["bus"]["1"]["bus_type"] = 3
# data_math = reduce_lines_math(data_math)

# Note using explicit neutral model
model = instantiate_mc_model(data_math, IVRUPowerModel, build_mc_opf)
res = optimize_model!(model, optimizer=Ipopt.Optimizer)
res_math = res["solution"]
sol_eng = transform_solution(res_math, data_math, make_si=true)
write_json(sol_eng, "res-single-step.json")