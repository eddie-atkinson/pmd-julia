using PowerModelsDistribution
using Ipopt
using Dates
using DataFrames
using CSV
include("helpers.jl")
const PMD = PowerModelsDistribution

# Various control variables for changing the program we are running
network_name = "J"


function save_results(sol_eng, step, termination_status, bus_name_index_map, load_name_index_map, sgen_name_index_map, record_solar, load_df, bus_df, pv_df, data_eng)
    converged = termination_status == PMD.LOCALLY_SOLVED
    for (load_name, load) in sol_eng["load"]
        load_bus_name = data_eng["load"]["$load_name"]["bus"]
        load_bus_index = bus_name_index_map[load_bus_name]
        load_index = load_name_index_map[load_name]
        push!(
            load_df,
            Dict(
                :step => step,
                :load => load_name,
                :load_index => load_index,
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
            pv_index = sgen_name_index_map[solar_name]
            pmax = data_eng["solar"]["$solar_name"]["pg_ub"][1]
            sg = sqrt(qg^2 + pg^2)
            pf = pg / sg
            push!(
                pv_df,
                Dict(
                    :step => step,
                    :solar => solar_name,
                    :solar_index => pv_index,
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


function run_timeseries(optimal_power_flow, solar, data_eng, start_index, end_index)
    data_base_path = joinpath(pwd(), "input_data")
    pv_path = joinpath(data_base_path, "train_pv.csv")
    active_path = joinpath(data_base_path, "train_active.csv")
    reactive_path = joinpath(data_base_path, "train_reactive.csv")

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

    bus_index_name_map = get_bus_index_name_map(network_name)
    bus_name_index_map = Dict{String,Int64}(value => key for (key, value) in bus_index_name_map)
    load_name_index_map = get_load_df_index_map(network_name)
    sgen_name_index_map = get_sgen_df_index_map(network_name)

    bus_results_df = DataFrame(step=Vector{Int64}(), bus_index=Vector{Int64}(), bus_name=Vector{String}(), phase=Vector{Int64}(), vm=Vector{Float64}(), converged=Vector{Bool}())
    load_results_df = DataFrame(step=Vector{Int64}(), load=Vector{String}(), load_index=Vector{Int64}(), bus_index=Vector{Int64}(), bus_name=Vector{String}(), qd=Vector{Float64}(), pd=Vector{Float64}(), converged=Vector{Bool}())
    pv_results_df = SOLAR ? DataFrame(step=Vector{Int64}(), pf=Vector{Float64}(), solar_index=Vector{Int64}(), solar=Vector{String}(), bus_index=Vector{Int64}(), bus_name=Vector{String}(), qg=Vector{Float64}(), pg=Vector{Float64}(), converged=Vector{Bool}(), pmax=Vector{Float64}()) : DataFrame()

    for i = start_index:end_index
        println(i)
        println("--------------")
        solar_ts_field = optimal_power_flow ? "pg_ub" : "pg"

        add_load_time_series_single_step(load_name_index_map, data_eng, active_df, reactive_df, i)
        add_solar_time_series_single_step(sgen_name_index_map, data_eng, pv_df, i, solar_ts_field)

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

        build_fn = optimal_power_flow ? build_mc_opf : build_mc_pf

        model = instantiate_mc_model(data_math, ACPUPowerModel, build_fn)

        if optimal_power_flow
            add_solar_power_constraints(model)
        end


        result = optimize_model!(model, optimizer=Ipopt.Optimizer)

        sol_eng = transform_solution(result["solution"], data_math)

        save_results(sol_eng, i, result["termination_status"], bus_name_index_map, load_name_index_map, sgen_name_index_map, solar, load_results_df, bus_results_df, pv_results_df, data_eng)
    end
    pf_path_prefix = optimal_power_flow ? "opf" : "pf"
    solar_path_prefix = solar ? "solar_true" : "solar_false"
    output_path = joinpath((pwd(), "simulation_results", "$pf_path_prefix-$solar_path_prefix"))

    CSV.write(joinpath((output_path, "load-$end_index.csv")), load_results_df)
    CSV.write(joinpath((output_path, "pv-$end_index.csv")), pv_results_df)
    CSV.write(joinpath((output_path, "bus-$end_index.csv")), bus_results_df)
end