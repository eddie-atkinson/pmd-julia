using Distributed
cores = 36
addprocs(cores)

@everywhere using PowerModelsDistribution
@everywhere using Ipopt
@everywhere using Dates
@everywhere using DataFrames
@everywhere include("helpers.jl")
@everywhere include("run_timeseries.jl")
@everywhere const PMD = PowerModelsDistribution

# Various control variables for changing the program we are running
@everywhere network_name = "J"
@everywhere OPTIMAL_POWER_FLOW = true
@everywhere SOLAR = true
@everywhere N_ITER = 296641
@everywhere CHUNKSIZE = 8240
@everywhere dss_path = "networks/J/Master.dss"



@everywhere data_eng = parse_file(dss_path)
@everywhere data_eng["settings"]["sbase_default"] = 10000
@everywhere join_lines_eng!(data_eng)

# Remove the existing voltage bounds
@everywhere remove_all_bounds!(data_eng)


if OPTIMAL_POWER_FLOW
    # Add back the voltage bounds we care about
    @everywhere add_bus_absolute_vbounds!(
        data_eng,
        phase_lb_pu=0.9,
        phase_ub_pu=1.1,
    )

end

# cost_pg_parameters = [quadratic, linear, constant] 
# NB: constant term doesn't matter
@everywhere data_eng["voltage_source"]["source"]["cost_pg_parameters"] = [0.0, 1.0, 0.0]


if SOLAR
    @everywhere add_solar_network_model(data_eng, network_name, OPTIMAL_POWER_FLOW)
end

@everywhere move_loads_to_pp_phase(data_eng, network_name)


@sync @distributed for range_end = range(start=1, stop=N_ITER, step=CHUNKSIZE)
    start_idx = range_end - (CHUNKSIZE - 1)
    if start_idx == 0
        start_idx = 1
    end
    run_timeseries(OPTIMAL_POWER_FLOW, SOLAR, data_eng, start_idx, range_end)
    nothing
end

