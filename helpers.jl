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



function get_bus_index_name_map(network_name::String)
    bus_path = joinpath((pwd(), "networks", network_name, "pp_model", "bus_index_name_map.json"))
    bus_map = read_json(bus_path)
    Dict{Int64,String}(parse(Int64, key) => value for (key, value) in bus_map)
end

function move_loads_to_pp_phase(network_model::Dict{String,Any}, network_name::String)
    base_path = joinpath((pwd(), "networks", network_name, "pp_model"))

    loads_path = joinpath((base_path, "loads_pp.json"))

    loads = read_json(loads_path)

    for (load_name, load) in loads
        network_load = network_model["load"][load_name]
        phase_number = phase_map[load["phase"]]
        new_connections = [phase_number, 4]
        network_load["connections"] = new_connections
    end
    nothing
end


function add_solar_network_model(network_model::Dict{String,Any}, network_name::String, optimal_power_flow::Bool)
    base_path = joinpath((pwd(), "networks", network_name, "pp_model"))

    solar_path = joinpath((base_path, "sgens_pp.json"))

    solar = read_json(solar_path)
    qg_ub = optimal_power_flow ? [6.90] : [0.0]
    qg_lb = optimal_power_flow ? [-6.90] : [0.0]

    pg_ub = optimal_power_flow ? [6.90] : [0.0]
    pg_lb = optimal_power_flow ? [0.0] : [0.0]


    for (sgen_name, sgen) in solar
        gen_bus_name = sgen["bus_name"]
        gen_phase_name = sgen["phase"]
        gen_phase_number = phase_map[gen_phase_name]
        add_solar!(
            network_model,
            sgen_name,
            gen_bus_name,
            [gen_phase_number, 4],
            configuration=WYE,
            # Not actually used in OPF but it complains if not set
            pg=[0.0],
            qg=[0.0],
            pg_lb=pg_lb,
            pg_ub=pg_ub,
            qg_lb=qg_lb,
            qg_ub=qg_ub,
            # Quadratic, linear, constant
            cost_pg_parameters=[0.0, -1.0, 0.0],
        )
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

function add_solar_time_series(bus_name_index_map, network_model, pv_df, start_index, end_index, ts_field="pg_ub")
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
            ts_field => pg_time_series_name
        )
    end
    nothing
end

function get_load_df_index_map(network_name::String)
    path = joinpath((pwd(), "networks", "$network_name", "pp_model", "loads.json"))
    loads = read_json(path)
    df_map = Dict{String,Int64}()
    for (i, load) in enumerate(loads)
        df_map[load["name"]] = i - 1
    end
    df_map
end

function get_sgen_df_index_map(network_name::String)
    path = joinpath((pwd(), "networks", "$network_name", "pp_model", "sgens_pp.json"))
    sgens = read_json(path)
    df_map = Dict{String,Int64}()
    for (sgen_name, sgen) in sgens
        df_map[sgen_name] = sgen["index"]
    end
    df_map
end

function add_time_series(network_name::String, network_model::Dict{String,Any}, active_df, reactive_df, pv_df, start_index, end_index)
    bus_index_name_map = get_bus_index_name_map(network_name)
    bus_name_index_map = Dict(value => key for (key, value) in bus_index_name_map)

    network_model["time_series"] = Dict{String,Any}()

    add_load_time_series(bus_name_index_map, network_model, active_df, reactive_df, start_index, end_index)
    add_solar_time_series(bus_name_index_map, network_model, pv_df, start_index, end_index)

    nothing
end


function add_load_time_series_single_step(load_name_index_map::Dict{String,Int64}, network_model::Dict{String,Any}, active_df::DataFrame, reactive_df::DataFrame, step_index::Int64)

    for (load_name, load) in network_model["load"]
        # Indexing from 0 in python byt not here
        load_number = load_name_index_map[load_name] + 1
        load["pd_nom"] = [active_df[step_index, load_number]]
        load["qd_nom"] = [reactive_df[step_index, load_number]]

    end
    nothing
end


function add_solar_time_series_single_step(load_name_index_map::Dict{String,Int64}, data_eng::Dict{String,Any}, pv_df::DataFrame, step_index::Int64, ts_field="pg_ub")
    if haskey(data_eng, "solar")
        for (solar_name, solar) in data_eng["solar"]
            # Python vs julia indexing need to add one
            solar_number = load_name_index_map[solar_name] + 1
            solar[ts_field] = [pv_df[step_index, solar_number]]
        end
    end
    nothing
end

function add_time_series_single_step(network_model::Dict{String,Any}, bus_name_index_map::Dict{String,Int64}, active_df::DataFrame, reactive_df::DataFrame, pv_df::DataFrame, step_index::Int64)
    add_load_time_series_single_step(bus_name_index_map, network_model, active_df, reactive_df, step_index)
    add_solar_time_series_single_step(bus_name_index_map, network_model, pv_df, step_index)
end



function _line_reverse_eng!(line)
    prop_pairs = [("f_bus", "t_bus")]

    for (x, y) in prop_pairs
        tmp = line[x]
        line[x] = line[y]
        line[y] = tmp
    end
end

function _get_required_buses_eng!(data_eng)
    buses_exclude = []
    for comp_type in ["load", "shunt", "generator", "voltage_source"]
        if haskey(data_eng, comp_type)
            buses_exclude = union(buses_exclude, [comp["bus"] for (_, comp) in data_eng[comp_type]])
        end
    end
    if haskey(data_eng, "switch")
        buses_exclude = union(buses_exclude, [sw["f_bus"] for (_, sw) in data_eng["switch"]])
        buses_exclude = union(buses_exclude, [sw["t_bus"] for (_, sw) in data_eng["switch"]])
    end
    if haskey(data_eng, "transformer")
        buses_exclude = union(buses_exclude, vcat([tr["bus"] for (_, tr) in data_eng["transformer"]]...))
    end

    return buses_exclude
end

function join_lines_eng!(data_eng)
    @assert data_eng["data_model"] == ENGINEERING

    # a bus is eligible for reduction if it only appears in exactly two lines
    buses_all = collect(keys(data_eng["bus"]))
    buses_exclude = _get_required_buses_eng!(data_eng)

    # per bus, list all inbound or outbound lines
    bus_lines = Dict(bus => [] for bus in buses_all)
    for (id, line) in data_eng["line"]
        push!(bus_lines[line["f_bus"]], id)
        push!(bus_lines[line["t_bus"]], id)
    end

    # exclude all buses that do not have exactly two lines connected to it
    buses_exclude = union(buses_exclude, [bus for (bus, lines) in bus_lines if length(lines) != 2])

    # now loop over remaining buses
    candidates = setdiff(buses_all, buses_exclude)
    for bus in candidates
        line1_id, line2_id = bus_lines[bus]
        line1 = data_eng["line"][line1_id]
        line2 = data_eng["line"][line2_id]

        # reverse lines if needed to get the order
        # (x)--fr-line1-to--(bus)--to-line2-fr--(x)
        if line1["f_bus"] == bus
            _line_reverse_eng!(line1)
        end
        if line2["f_bus"] == bus
            _line_reverse_eng!(line2)
        end

        reducable = true
        reducable = reducable && line1["linecode"] == line2["linecode"]
        reducable = reducable && all(line1["t_connections"] .== line2["t_connections"])
        if reducable

            line1["length"] += line2["length"]
            line1["t_bus"] = line2["f_bus"]
            line1["t_connections"] = line2["f_connections"]

            delete!(data_eng["line"], line2_id)
            delete!(data_eng["bus"], bus)
            for x in candidates
                if line2_id in bus_lines[x]
                    bus_lines[x] = [setdiff(bus_lines[x], [line2_id])..., line1_id]
                end
            end
        end
    end

    return data_eng
end

function is_source(generator::Dict{String,Any})
    return occursin("voltage_source", generator["source_id"])
end

function add_solar_power_constraints(model::AbstractUnbalancedPowerModel)

    for (id, gen) in ref(model)[:gen]
        # Don't constrain the voltage source 
        if is_source(gen)
            continue
        end
        pg_opt_var = [var(model, :pg, id)[c] for c in gen["connections"]]
        qg_opt_var = [var(model, :qg, id)[c] for c in gen["connections"]]

        pmax = gen["pmax"][1]

        @constraint(model.model, qg_opt_var .<= -tan(acos(0.8)) * pg_opt_var)
        @constraint(model.model, qg_opt_var .>= -tan(acos(0.8)) * pg_opt_var)
        @constraint(model.model, (pg_opt_var .^ 2 + qg_opt_var .^ 2) .<= pmax^2)
    end
end