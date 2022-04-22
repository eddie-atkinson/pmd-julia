using JSON
using PowerModelsDistribution
import Ipopt
using PyCall
using JuMP


include("helpers.jl")

network_name = "test"

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

add_solar!(
    network_model,
    "1",
    "68",
    [1, 4],
    configuration=WYE,
    pg_lb=[0.0],
    pg_ub=[6.90],
    # Back of the envelope calc
    # TODO: actually set this properly
    qg_lb=[-4.14],
    qg_ub=[4.14],
    # qg_lb=[-Inf],
    # qg_ub=[Inf],
    cost_pg_parameters=[0, 0, 0],
)

network_model["voltage_source"]["source"]["cost_pg_parameters"] = [0, 1, 0]


data_math = transform_data_model(network_model, kron_reduce=false, phase_project=false)

add_start_vrvi!(data_math)

model = instantiate_mc_model(data_math, IVRENPowerModel, build_mc_opf)

# add_solar_power_constraints(model, data_math, network_model)

res = optimize_model!(model, optimizer=Ipopt.Optimizer)

res_math = res["solution"]


sol_eng = transform_solution(res_math, data_math, make_si=true)

write_json(sol_eng, "res_small.json")
