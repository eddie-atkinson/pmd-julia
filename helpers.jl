using JSON
using PowerModelsDistribution
import Ipopt
using PyCall
using JuMP
using DataFrames
using CSV



phase_map = Dict{String,Number}(
    "a" => 1,
    "b" => 2,
    "c" => 3
)

function write_json(val, path::String)
    open(path, "w") do f
        JSON.print(f, val)
    end
end

function read_json(path::String)
    open(path, "r") do f
        JSON.parse(f)
    end
end


function load_pp_pickle_model_sgen_json(network_name::String)
    model_path = joinpath((pwd(), "networks", network_name, "pp_model", "model.p"))
    # Need scipy and pandas to expand pp model pickles
    PyCall.Conda.add("scipy")
    PyCall.Conda.add("pandas")
    py"""
    import pickle
    def load_pickle(path):
        with open(path, "rb") as infile:
            data = pickle.load(infile)
        return data
    """
    load_pickle = py"load_pickle"
    model = load_pickle(model_path)
    model
end


function get_bus_index_name_map(network_name::String)
    bus_path = joinpath((pwd(), "networks", network_name, "pp_model", "buses.json"))
    buses = read_json(bus_path)
    bus_index_name_map = Dict{Int64,String}()
    for bus = values(buses)
        bus_index_name_map[bus["index"]] = bus["name"]
    end
    bus_index_name_map
end

function move_loads_to_pp_phase(network_model::Dict{String,Any}, network_name::String)
    base_path = joinpath((pwd(), "networks", network_name, "pp_model"))

    loads_path = joinpath((base_path, "loads.json"))

    loads = read_json(loads_path)

    bus_index_name_map = get_bus_index_name_map(network_name)

    # Assume each bus only has a single load
    bus_name_phase_map = Dict{String,Int8}()
    for load = values(loads)
        bus_index = load["bus"]
        phase = load["phase"]
        bus_name = bus_index_name_map[bus_index]
        phase_number = phase_map[phase]
        bus_name_phase_map[bus_name] = phase_number
    end

    for (load_name, load) in network_model["load"]
        bus_name = load["bus"]
        phase_number = bus_name_phase_map[bus_name]
        new_connections = [phase_number, 4]
        network_model["load"][load_name]["connections"] = new_connections
    end
    nothing
end


function add_solar_network_model(network_model::Dict{String,Any}, pp_model, network_name::String)
    asymmetric_sgen_data = pp_model["asymmetric_sgen"]["DF"]["data"]
    asymmetric_sgen_columns = pp_model["asymmetric_sgen"]["DF"]["columns"]

    name_index = findfirst(x -> x == "name", asymmetric_sgen_columns)
    bus_index = findfirst(x -> x == "bus", asymmetric_sgen_columns)
    bus_index_name_map = get_bus_index_name_map(network_name)

    # Voltage source is ID 1 so start from 2
    id = 2
    for solar_gen in eachrow(asymmetric_sgen_data)
        gen_bus_index = solar_gen[bus_index]
        gen_bus_name = bus_index_name_map[gen_bus_index]
        gen_phase_name = solar_gen[name_index]
        gen_phase_number = phase_map[gen_phase_name]
        add_solar!(
            network_model,
            "$id",
            gen_bus_name,
            [gen_phase_number, 4],
            configuration=WYE,
            # Not actually used in OPF but it complains if not set
            pg=[0.0],
            qg=[0.0],
            pg_lb=[0.0],
            pg_ub=[6.90],
            qg_lb=[-6.9],
            qg_ub=[6.9],
            # Quadratic, linear, constant
            cost_pg_parameters=[0.0, -1.0, 0.0],
        )
        id += 1
    end
    nothing
end


function add_load_time_series(bus_name_index_map, network_model, active_df, reactive_df, start_index, stop_index)
    time_series = network_model["time_series"]

    active_rows = active_df[start_index:stop_index, :]
    reactive_rows = reactive_df[start_index:stop_index, :]

    for (load_id, load) in network_model["load"]
        pd_nom_time_series_name = "pd_nom_load_$load_id"
        qd_nom_time_series_name = "qd_nom_load_$load_id"

        # Julia indexes from 1, but the pandapower indices start from 0
        load_number = bus_name_index_map[load["bus"]] + 1
        # This is disgusting, but subsets the df column into a vector
        pd_nom_load = active_rows[:, load_number]
        qd_nom_load = reactive_rows[:, load_number]

        time_series[pd_nom_time_series_name] = Dict{String,Any}(
            "replace" => true,
            "time" => 1:size(active_rows)[1],
            "values" => pd_nom_load
        )

        time_series[qd_nom_time_series_name] = Dict{String,Any}(
            "replace" => true,
            "time" => 1:size(reactive_rows)[1],
            "values" => qd_nom_load
        )

        load["time_series"] = Dict(
            "pd_nom" => pd_nom_time_series_name,
            "qd_nom" => qd_nom_time_series_name
        )
    end
    nothing
end

function add_solar_time_series(bus_name_index_map, network_model, pv_df, start_index, end_index)
    time_series = network_model["time_series"]
    pv_rows = pv_df[start_index:end_index, :]
    for (solar_id, solar) in network_model["solar"]
        pg_time_series_name = "pg_ub_solar_$solar_id"

        # Julia indexes from 1, but the pandapower indices start from 0
        solar_number = bus_name_index_map[solar["bus"]] + 1
        pg_solar = pv_rows[:, solar_number]


        time_series[pg_time_series_name] = Dict{String,Any}(
            "replace" => true,
            "time" => 1:size(pv_rows)[1],
            "values" => pg_solar
        )

        solar["time_series"] = Dict(
            "pg_ub" => pg_time_series_name
        )
    end
    nothing
end

function add_time_series(network_name::String, network_model::Dict{String,Any}, active_df, reactive_df, pv_df, start_index, end_index)
    bus_index_name_map = get_bus_index_name_map(network_name)
    bus_name_index_map = Dict(value => key for (key, value) in bus_index_name_map)

    network_model["time_series"] = Dict{String,Any}()

    add_load_time_series(bus_name_index_map, network_model, active_df, reactive_df, start_index, end_index)
    add_solar_time_series(bus_name_index_map, network_model, pv_df, start_index, end_index)

    nothing
end


function add_load_time_series_single_step(bus_name_index_map::Dict{String,Int64}, network_model::Dict{String,Any}, active_df::DataFrame, reactive_df::DataFrame, step_index::Int64)

    for (_, load) in network_model["load"]
        # Julia indexes from 1, but the pandapower indices start from 0
        load_number = bus_name_index_map[load["bus"]] + 1
        load["pd_nom"] = [active_df[step_index, load_number]]
        load["qd_nom"] = [reactive_df[step_index, load_number]]

    end
    nothing
end


function add_solar_time_series_single_step(bus_name_index_map::Dict{String,Int64}, network_model::Dict{String,Any}, pv_df::DataFrame, step_index::Int64)
    for (solar_id, solar) in network_model["solar"]

        # Julia indexes from 1, but the pandapower indices start from 0
        solar_number = bus_name_index_map[solar["bus"]] + 1
        solar["pg_ub"] = [pv_df[step_index, solar_number]]
    end
    nothing
end

function add_time_series_single_step(network_model::Dict{String,Any}, bus_name_index_map::Dict{String,Int64}, active_df::DataFrame, reactive_df::DataFrame, pv_df::DataFrame, step_index::Int64)
    add_load_time_series_single_step(bus_name_index_map, network_model, active_df, reactive_df, step_index)
    add_solar_time_series_single_step(bus_name_index_map, network_model, pv_df, step_index)
end

function add_solar_power_constraints(model::AbstractUnbalancedPowerModel, data_math::Dict{String,Any}, network_model::Dict{String,Any})
    for gen = values(data_math["gen"])
        # We don't want to constrain the voltage source
        if ~occursin("solar", gen["source_id"])
            continue
        end
        index = gen["index"]

        # Want a scalar subexpression, not a vector
        pg_opt_var = var(model, :pg, index)[1]
        qg_opt_var = var(model, :qg, index)[1]

        @NLconstraint(model.model, qg_opt_var^2 - 0.36 * pg_opt_var^2 <= 0)
        @NLconstraint(model.model, pg_opt_var^2 - 0.96 * qg_opt_var^2 >= 0)
    end
end


function write_results(network_name::String, solution::Dict{String,Any}, output_path::String)
    bus_index_name_map = get_bus_index_name_map(network_name)
    bus_name_index_map = Dict(value => key for (key, value) in bus_index_name_map)
    # Assume we are using multinetwork and have at least one network
    loads = keys(solution["nw"]["1"])


    bus_df = DataFrame(nw=Vector{String}(), bus=Vector{String}(), phase=Vector{Int64}(), voltage=Vector{Float64}())
    load_df = DataFrame(nw=Vector{String}(), load=Vector{String}(), qd=Vector{Float64}(), pd=Vector{Float64}())
    pv_df = DataFrame(nw=Vector{String}(), solar=Vector{String}(), qg=Vector{Float64}(), pg=Vector{Float64}())
    for (network_id, network) in solution["nw"]
        for (load_name, load) in network["load"]
            push!(load_df, Dict(:nw => network_id, :load => load_name, :qd => load["qd"][1], :pd => load["pd"][1]))
        end
        for (solar_name, solar) in network["solar"]
            push!(pv_df, Dict(:nw => network_id, :solar => solar_name, :qg => solar["qg"][1], :pg => solar["pg"][1]))
        end

        for (bus_name, bus) in network["bus"]
            # Add a row for each phase
            for i = 1:3
                phase_vi = bus["vi"][i]
                phase_vr = bus["vr"][i]
                phase_voltage = sqrt(phase_vi^2 + phase_vr^2)
                bus_id = bus_name_index_map[bus_name]
                push!(bus_df, Dict(:nw => network_id, :bus => "$bus_id", :voltage => phase_voltage, :phase => i))
            end
        end
    end
    sort!(load_df, [:nw])
    sort!(pv_df, [:nw])
    sort!(bus_df, [:nw])

    CSV.write("$output_path-load.csv", load_df)
    CSV.write("$output_path-pv.csv", pv_df)
    CSV.write("$output_path-bus.csv", bus_df)
end
