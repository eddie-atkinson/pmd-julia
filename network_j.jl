using PowerModelsDistribution
using Ipopt
using Dates
using DataFrames
include("helpers.jl")
const PMD = PowerModelsDistribution

# Various control variables for changing the program we are running
network_name = "J"
OPTIMAL_POWER_FLOW = false
SOLAR = false
N_ITER = 53760

dss_path = "networks/J/Master.dss"

function save_results(sol_eng, step, termination_status, bus_name_index_map, record_solar, load_df, bus_df, pv_df, data_eng)
    converged = termination_status == PMD.LOCALLY_SOLVED
    for (load_name, load) in sol_eng["load"]
        load_bus_name = data_eng["load"]["$load_name"]["bus"]
        load_bus_index = bus_name_index_map[load_bus_name]
        push!(
            load_df,
            Dict(
                :step => step,
                :load => load_name,
                :qd => load["qd"][1],
                :pd => load["pd"][1],
                :converged => converged,
                :bus_name => load_bus_name,
                :bus_index => load_bus_index,
            ),
        )
    end
    for (bus_name, bus) in sol_eng["bus"]
        # Add a row for each phase
        for i = 1:3
            phase_voltage = bus["vm"][i]
            bus_id = bus_name_index_map[bus_name]
            push!(
                bus_df,
                Dict(
                    :step => step,
                    :bus_index => bus_id,
                    :bus_name => bus_name,
                    :vm => phase_voltage,
                    :phase => i,
                    :converged => converged
                ),
            )
        end
    end
    if record_solar
        for (solar_name, solar) in sol_eng["solar"]
            qg = solar["qg"][1]
            pg = solar["pg"][1]
            solar_bus_name = data_eng["solar"]["$solar_name"]["bus"]
            solar_bus_index = bus_name_index_map[solar_bus_name]

            pmax = data_eng["solar"]["$solar_name"]["pg_ub"][1]
            sg = sqrt(qg^2 + pg^2)
            pf = pg / sg
            push!(
                pv_df,
                Dict(
                    :step => step,
                    :solar => solar_name,
                    :qg => qg,
                    :pg => pg,
                    :pf => pf,
                    :converged => converged,
                    :pmax => pmax,
                    :bus_name => solar_bus_name,
                    :bus_index => solar_bus_index,
                ),
            )
        end
    end
end



data_eng = parse_file(dss_path)
data_eng["settings"]["sbase_default"] = 1.0
join_lines_eng!(data_eng)

# Remove the existing voltage bounds
remove_all_bounds!(data_eng)


if OPTIMAL_POWER_FLOW
    # Add back the voltage bounds we care about
    add_bus_absolute_vbounds!(
        data_eng,
        phase_lb_pu=0.9,
        phase_ub_pu=1.1,
    )

end

# cost_pg_parameters = [quadratic, linear, constant]
# NB: constant term doesn't matter
data_eng["voltage_source"]["source"]["cost_pg_parameters"] = [0.0, 1.0, 0.0]

data_base_path = joinpath(pwd(), "input_data")
pv_path = joinpath(data_base_path, "test_pv.csv")
active_path = joinpath(data_base_path, "test_active.csv")
reactive_path = joinpath(data_base_path, "test_reactive.csv")

pv_df = CSV.read(pv_path, DataFrame)
active_df = CSV.read(active_path, DataFrame)
reactive_df = CSV.read(reactive_path, DataFrame)

# transform(pv_df, :datetime => ByRow(x -> DateTime(x, DateFormat("y-m-d H:M:S"))), renamecols=false)

# Drop datetime, we don't need it
active_df = active_df[:, Not(:datetime)]
reactive_df = reactive_df[:, Not(:datetime)]
pv_df = pv_df[:, Not(:datetime)]

# The data is in MW we need it in kW
pv_df = pv_df .* 1000
active_df = active_df .* 1000 .* 0.7
reactive_df = reactive_df .* 1000 .* 0.7

load_buses = [load["bus"] for (load_name, load) in data_eng["load"]]
bus_index_name_map = get_bus_index_name_map(network_name)
bus_name_index_map = Dict{String,Int64}(value => key for (key, value) in bus_index_name_map)

move_loads_to_pp_phase(data_eng, network_name)

bus_results_df = DataFrame(step=Vector{Int64}(), bus_index=Vector{Int64}(), bus_name=Vector{String}(), phase=Vector{Int64}(), vm=Vector{Float64}(), converged=Vector{Bool}())
load_results_df = DataFrame(step=Vector{Int64}(), load=Vector{String}(), bus_index=Vector{Int64}(), bus_name=Vector{String}(), qd=Vector{Float64}(), pd=Vector{Float64}(), converged=Vector{Bool}())
pv_results_df = SOLAR ? DataFrame(step=Vector{Int64}(), pf=Vector{Float64}(), solar=Vector{String}(), bus_index=Vector{Int64}(), bus_name=Vector{String}(), qg=Vector{Float64}(), pg=Vector{Float64}(), converged=Vector{Bool}(), pmax=Vector{Float64}()) : DataFrame()


if SOLAR
    add_solar_network_model(data_eng, network_name, OPTIMAL_POWER_FLOW)
end

for i = 1:N_ITER

    solar_ts_field = OPTIMAL_POWER_FLOW ? "pg_ub" : "pg"

    add_load_time_series_single_step(bus_name_index_map, data_eng, active_df, reactive_df, i)
    add_solar_time_series_single_step(bus_name_index_map, data_eng, pv_df, i, solar_ts_field)

    data_math = transform_data_model(data_eng)

    nw = data_math

    vsource_gen = findfirst(x -> contains(x["source_id"], "voltage_source.source"), nw["gen"])
    vsource_bus = nw["gen"][vsource_gen]["gen_bus"]
    vsource_branch = findfirst(x -> x["f_bus"] == vsource_bus, nw["branch"])
    vsource_new_bus = nw["branch"][vsource_branch]["t_bus"]
    nw["gen"][vsource_gen]["gen_bus"] = vsource_new_bus
    delete!(nw["bus"], vsource_bus)
    delete!(nw["branch"], vsource_branch)
    # now set up the reference bus correctly
    b = nw["bus"][string(vsource_new_bus)]
    b["vm"] = 1.08 * [1, 1, 1]
    b["va"] = [0, -2pi / 3, 2pi / 3]
    b["bus_type"] = 3
    b["vmin"] = 1.08 * [1, 1, 1]
    b["vmax"] = 1.08 * [1, 1, 1]

    build_fn = OPTIMAL_POWER_FLOW ? build_mc_opf : build_mc_pf

    model = instantiate_mc_model(data_math, ACPUPowerModel, build_fn)

    if OPTIMAL_POWER_FLOW
        add_solar_power_constraints(model)
    end


    result = optimize_model!(model, optimizer=Ipopt.Optimizer)

    sol_eng = transform_solution(result["solution"], data_math)

    save_results(sol_eng, i, result["termination_status"], bus_name_index_map, SOLAR, load_results_df, bus_results_df, pv_results_df, data_eng)
end

pf_path_prefix = OPTIMAL_POWER_FLOW ? "opf" : "pf"
solar_path_prefix = SOLAR ? "solar_true" : "solar_false"
output_path = joinpath((pwd(), "simulation_results", "$pf_path_prefix-$solar_path_prefix"))


CSV.write(joinpath((output_path, "load.csv")), load_results_df)
CSV.write(joinpath((output_path, "pv.csv")), pv_results_df)
CSV.write(joinpath((output_path, "bus.csv")), bus_results_df)