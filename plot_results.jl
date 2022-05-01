using DataFrames
using CSV
using PlotlyJS

VBASE = 0.230
bus_df = CSV.read("./simulation_results/opf-solar_true/bus.csv", DataFrame)

# bus_df = bus_df[1:Int64(nrow(bus_df) / 2), :]
# bus_df = bus_df[1:10, :]
_ = bus_df[:, :phase_str] = replace(string.(bus_df[:, :phase]), "1" => "a", "2" => "b", "3" => "c")
bus_df[:, :vm_pu] = bus_df[:, :vm] ./ VBASE
boxplot = plot(bus-df
    bus_df,
    x=:bus_index,
    y=:vm_pu,
    kind="box",
    boxpoints="suspectedoutliers",
    labels=Dict{Symbol,String}(
        :vm => "Voltage (pu)",
        :bus_index => "Bus",
        :phase_str => "Phase"
    ),
    facet_row=:phase_str,
    facet_orders=Dict{Symbol,Vector{String}}(:phase_str => ["a", "b", "c"]),
)
savefig(boxplot, "p.png")
nothing