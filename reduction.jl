function _line_reverse!(line)
    prop_pairs = [("f_bus", "t_bus"), ("g_fr", "g_to"), ("b_fr", "b_to")]

    for (x, y) in prop_pairs
        tmp = line[x]
        line[x] = line[y]
        line[y] = tmp
    end
end


function _line_reverse_eng!(line)
    prop_pairs = [("f_bus", "t_bus")]

    for (x, y) in prop_pairs
        tmp = line[x]
        line[x] = line[y]
        line[y] = tmp
    end
end


function _get_required_buses_math!(data_math)
    buses_exclude = []
    for comp_type in ["load", "shunt", "gen"]
        if haskey(data_math, comp_type)
            buses_exclude = union(buses_exclude, [comp["$(comp_type)_bus"] for (_, comp) in data_math[comp_type]])
        end
    end
    for comp_type in ["switch", "transformer"]
        if haskey(data_math, comp_type)
            buses_exclude = union(buses_exclude, [sw["f_bus"] for (_, sw) in data_math[comp_type]])
            buses_exclude = union(buses_exclude, [sw["t_bus"] for (_, sw) in data_math[comp_type]])
        end
    end

    return buses_exclude
end


function reduce_lines_math!(data_math)
    @assert data_math["data_model"] == MATHEMATICAL

    # a bus is eligible for reduction if it only appears in exactly two lines
    buses_all = collect(keys(data_math["bus"]))
    buses_exclude = _get_required_buses_math!(data_math)

    # per bus, list all inbound or outbound lines
    bus_lines = Dict(bus => [] for bus in buses_all)
    for (id, line) in data_math["branch"]
        push!(bus_lines[string(line["f_bus"])], id)
        push!(bus_lines[string(line["t_bus"])], id)
    end

    # exclude all buses that do not have exactly two lines connected to it
    buses_exclude = union(buses_exclude, [bus for (bus, lines) in bus_lines if length(lines) != 2])
    print("Excluding $buses_exclude")
    # now loop over remaining buses
    candidates = setdiff(buses_all, buses_exclude)
    for bus in candidates
        line1_id, line2_id = bus_lines[bus]
        line1 = data_math["branch"][line1_id]
        line2 = data_math["branch"][line2_id]

        # reverse lines if needed to get the order
        # (x)--fr-line1-to--(bus)--to-line2-fr--(x)
        if line1["f_bus"] == bus
            _line_reverse!(line1)
        end
        if line2["f_bus"] == bus
            _line_reverse!(line2)
        end

        reducable = true
        reducable = reducable && iszero(line1["g_to"]) && iszero(line1["b_to"])
        reducable = reducable && iszero(line2["g_fr"]) && iszero(line2["b_fr"])
        if reducable
            line1["br_r"] = line1["br_r"] .+ line2["br_r"]
            line1["br_x"] = line1["br_x"] .+ line2["br_x"]
            line1["g_to"] = line2["g_fr"]
            line1["b_to"] = line2["b_fr"]
            line1["t_bus"] = line2["f_bus"]

            delete!(data_math["branch"], line2_id)
            for x in candidates
                if line2_id in bus_lines[x]
                    bus_lines[x] = [setdiff(bus_lines[x], [line2_id])..., line1_id]
                end
            end
        end
    end

    return data_math
end


reduce_lines_math(data_math) = reduce_lines_math!(deepcopy(data_math))


# ENGINEERING MODEL REDUCTION


function reduce_lines_eng!(data_eng)
    rm_trailing_lines_eng!(data_eng)
    join_lines_eng!(data_eng)
end


reduce_lines_eng(data_eng) = reduce_lines_eng!(deepcopy(data_eng))


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


function rm_trailing_lines_eng!(data_eng)
    @assert data_eng["data_model"] == ENGINEERING

    buses_exclude = _get_required_buses_eng!(data_eng)

    line_has_shunt = Dict()
    bus_lines = Dict(k => [] for k in keys(data_eng["bus"]))
    for (id, line) in data_eng["line"]
        lc = data_eng["linecode"][line["linecode"]]
        line_has_shunt[id] = !all(iszero(lc[k]) for k in ["b_fr", "b_to", "g_fr", "g_to"])
        push!(bus_lines[line["f_bus"]], id)
        push!(bus_lines[line["t_bus"]], id)
    end

    eligible_buses = [bus_id for (bus_id, line_ids) in bus_lines if length(line_ids) == 1 && !(bus_id in buses_exclude) && !line_has_shunt[line_ids[1]]]

    while !isempty(eligible_buses)
        for bus_id in eligible_buses
            # this trailing bus has one associated line
            line_id = bus_lines[bus_id][1]
            line = data_eng["line"][line_id]

            delete!(data_eng["line"], line_id)
            delete!(data_eng["bus"], bus_id)

            other_end_bus = line["f_bus"] == bus_id ? line["t_bus"] : line["f_bus"]
            bus_lines[other_end_bus] = setdiff(bus_lines[other_end_bus], [line_id])
            delete!(bus_lines, bus_id)
        end

        eligible_buses = [bus_id for (bus_id, line_ids) in bus_lines if length(line_ids) == 1 && !(bus_id in buses_exclude) && !line_has_shunt[line_ids[1]]]
    end
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
