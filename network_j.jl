using PowerModelsDistribution
using Ipopt
include("helpers.jl")
const PMD = PowerModelsDistribution

network_name = "J"
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
        if is_source(gen)
            continue
        end
        pg_opt_var = var(model, :pg, id).data[1]
        qg_opt_var = var(model, :qg, id).data[1]

        pmax = gen["pmax"][1]
        # Relies on sbase_default being 1
        # @NLconstraint(model.model, 0.64 * (pg_opt_var^2 + qg_opt_var^2) <= pg_opt_var^2)
        @constraint(model.model, (pg_opt_var^2 + qg_opt_var^2) <= pmax^2)
    end
end


##


dss_path = "networks/J/Master.dss"

data_eng = parse_file(dss_path)
join_lines_eng!(data_eng)

# Remove the existing voltage bounds
remove_all_bounds!(data_eng)


# Add back the voltage bounds we care about
add_bus_absolute_vbounds!(
    data_eng,
    phase_lb_pu=0.9,
    phase_ub_pu=1.1,
)


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

# Drop datetime, we don't need it
active_df = active_df[:, Not(:datetime)]
reactive_df = reactive_df[:, Not(:datetime)]
pv_df = pv_df[:, Not(:datetime)]

# The data is in MW we need it in kW
pv_df = pv_df .* 1000
active_df = active_df .* 1000
reactive_df = reactive_df .* 1000

load_buses = [load["bus"] for (load_name, load) in data_eng["load"]]
bus_names = filter(x -> x !== "sourcebus", sort(load_buses))
bus_name_index_map = Dict{String,Int64}(bus_name => i - 1 for (i, bus_name) in enumerate(bus_names))

move_loads_to_pp_phase(data_eng, network_name)
pp_model = load_pp_pickle_model_sgen_json(network_name)
add_solar_network_model(data_eng, pp_model, network_name)

add_time_series_single_step(data_eng, bus_name_index_map, active_df, reactive_df, pv_df, 1800)

data_math = transform_data_model(data_eng)

vsource_gen = findfirst(x -> contains(x["source_id"], "voltage_source.source"), data_math["gen"])
vsource_bus = data_math["gen"][vsource_gen]["gen_bus"]
vsource_branch = findfirst(x -> x["f_bus"] == vsource_bus, data_math["branch"])
vsource_new_bus = data_math["branch"][vsource_branch]["t_bus"]
data_math["gen"][vsource_gen]["gen_bus"] = vsource_new_bus
delete!(data_math["bus"], vsource_bus)
delete!(data_math["branch"], vsource_branch)
# now set up the reference bus correctly
b = data_math["bus"][string(vsource_new_bus)]
b["vm"] = 1.08 * [1, 1, 1]
b["va"] = [0, -2pi / 3, 2pi / 3]
b["bus_type"] = 3
b["vmin"] = 1.08 * [1, 1, 1]
b["vmax"] = 1.08 * [1, 1, 1]


model = instantiate_mc_model(data_math, ACPUPowerModel, build_mc_opf)

add_solar_power_constraints(model)


result = optimize_model!(model, optimizer=Ipopt.Optimizer)

sol_eng = transform_solution(result["solution"], data_math)


for (sid, solar) in sol_eng["solar"]
    pg = solar["pg"][1]
    qg = solar["qg"][1]
    s = sqrt(pg^2 + qg^2)
    pf = pg / s
    print("$sid PF = $pf")
    print("\n")
end