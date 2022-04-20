### A Pluto.jl notebook ###
# v0.19.0

using Markdown
using InteractiveUtils

# ╔═╡ a3d1557a-000b-4bc8-a390-571c08e55586
begin
    import JuMP
    import PlutoUI
    using PowerModelsDistribution
    import InfrastructureModels
    import Ipopt
    import Plots
end

const PMD = PowerModelsDistribution

NOTEBOOK_DIR = join(split(split(@__FILE__, "#==#")[1], "/")[1:end-1], "/")
EVs = collect(1:10); # set of EVs

begin
    timestamps = collect(0:1:10) # timestamps
    D_k = timestamps[2:end] .- timestamps[1:end-1] # duration of each timestep
    K = 1:length(D_k) # set of timesteps
end;

begin
    Emax_e = fill(82.0, length(EVs)) # maximum SoC in KWh
    E0_e = Emax_e .* (1 / length(EVs) * [0:length(EVs)-1...]) # initial SoC in KWh
    Pmax = 5.0 # maximum charge in KW
    Pmax_e = fill(Pmax, length(EVs)) # all EVs have the same maximum charge
end;

begin
    Plots.plot(xticks=([0, 1], ["empty", "full"]), xlabel="SoC")
    Plots.plot!(ylabel="dissatisfaction")
    Plots.plot!([0, 1], [1, 0], label="", linewidth=2)
end

cp_model = JuMP.Model();

begin
    # charge for EV e and timestep k, in kW
    JuMP.@variable(cp_model, 0 <= cp_P_ek[e in EVs, k in K])
    # SoC for EV e at end of timestep k, in kWh
    JuMP.@variable(cp_model, 0 <= cp_E_ek[e in EVs, k in K] <= Emax_e[e])
end;


begin
    # for the first timestemp, use initial SoC E0_e
    JuMP.@constraint(cp_model, [e in EVs],
        cp_E_ek[e, 1] == E0_e[e] + D_k[1] * cp_P_ek[e, 1]
    )
    # for the other timestemps, use the SoC at the preceding timestep
    JuMP.@constraint(cp_model, [e in EVs, k in K[2:end]],
        cp_E_ek[e, k] == cp_E_ek[e, k-1] + D_k[k] * cp_P_ek[e, k]
    )
end;

JuMP.@constraint(cp_model, [e in EVs, k in K], cp_P_ek[e, k] <= Pmax_e[e]);

begin
    # define the dissatisfaction for each EV e at each timestep k
    cp_dissatisfaction_ek = [
        (Emax_e[e] - cp_E_ek[e, k]) / Emax_e[e]
        for e in EVs, k in K]
    # the objective is to minimize the total dissatisfaction across EVs and timesteps,
    # taking into account the duration of each step
    JuMP.@objective(cp_model, Min,
        sum(
            D_k[k] * cp_dissatisfaction_ek[e, k]
            for e in EVs, k in K)
    )
end;

begin
    # set the optimizer
    JuMP.set_optimizer(cp_model, Ipopt.Optimizer)
    # solve the problem
    JuMP.optimize!(cp_model)
    # inspect the termination status
    JuMP.termination_status(cp_model)
end
begin
    cp_E_ek_vals = JuMP.value.(cp_E_ek.data)
    Plots.plot(legend=:none, title="", xlabel="time [h]", ylabel="SoC[kWh]", ylim=[0, maximum(Emax_e)])
    for e in EVs
        Plots.plot!(timestamps, [E0_e[e], cp_E_ek_vals[e, :]...], markershape=:circle, markersize=3)
    end
    Plots.plot!()
end

EVs

# ╔═╡ 0be0abd5-f0ad-4d15-b6fb-d59d74e6058c
wd = pwd()
data_eng = PMD.parse_file(
    "$wd/resources/lvtestcase_notrans.dss",
    transformations=[remove_all_bounds!]
);

# ╔═╡ 04ef243c-2586-406e-9528-140d732c28f7
md"""
This network has about 900 lines. However, it is possible to obtain a reduced network model which is very similar and sometimes equivalent (when linecharging is negligible, which is often the case).
"""

# ╔═╡ 51d2dae6-a59c-4014-bdd6-886864847683
PMD.reduce_lines!(data_eng);

# ╔═╡ 2bd488f2-1a73-4c8e-a2bd-42a6fbd76d94
md"""
PMD uses two data models: a high-level `ENGINEERING` one, and a low-level `MATHEMATICAL` one. `ENGINEERING` is in SI-units, whilst `MATHEMATICAL` is in pu.

Since the optimization model is generated from the `MATHEMATICAL` model, we want to specify explicitly what the power base should be. Set it to 1 kW, so the unit is the same as in our EV charging model.
"""

# ╔═╡ 1d878788-13bd-4090-9ed0-b9eb77a8575d
data_eng["settings"]["sbase_default"] = 1.0 * 1E3 / data_eng["settings"]["power_scale_factor"];

# ╔═╡ 548ffacb-af94-46c1-bcea-3695f98b4516
md"""
We require that in per unit, the phase voltage magnitude $|U_p|$ and neutral voltage magnitude $|U_n|$ should obey

$0.9 \leq |U_p| \leq 1.1, \hspace{3em} |U_n| \leq 0.1$.

We can easily add these bounds to the data model with `PMD.add_bus_absolute_vbounds!`. Note that PMD can also constrain phase-to-neutral voltages instead of only absolute ones, but we ommit that here for simplicity.
"""

# ╔═╡ 0f97d7aa-cdbe-454c-83f0-978964c83b2a
PMD.add_bus_absolute_vbounds!(
    data_eng,
    phase_lb_pu=0.9,
    phase_ub_pu=1.1,
    neutral_ub_pu=0.1
);

# ╔═╡ 161c2052-7fbd-4993-a21f-3ed14b750619
md"""
So far, the network model contains only single-period data. Since we actually need a multi-period model, we add a time series to it which has the same amount of steps as our EV charging model.

Whilst we are at it, we apply this time series to modulate the consumption of the loads in the network.
"""

# ╔═╡ 8d24de5c-749c-4b0c-acaa-25d010a33844
begin
    # add a new time series to the data model
    data_eng["time_series"] = Dict{String,Any}()
    data_eng["time_series"]["normalized_load_profile"] = Dict{String,Any}(
        "replace" => false,
        "time" => K,
        "values" => 0.2 * cos.((pi / 2 / maximum(K)) .* K)
    )
    # attach a reference to each load, so that the consumption will be scaled
    # by the profile we created
    for (_, load) in data_eng["load"]
        load["time_series"] = Dict(
            "pd_nom" => "normalized_load_profile",
            "qd_nom" => "normalized_load_profile"
        )
    end
end

# ╔═╡ eb8c0d08-5ac0-446f-9447-944399eacdb0
md"""
We need to add a generator for each EV, and specify the connection settings. In the test case we imported, LVTestCase, each load represents a household with a single-phase connection. We now associate each EV with a household, and give it the same bus and phase connection.
"""

# ╔═╡ 8cf4316e-1cfa-4cf0-abff-1438edcb6d36
begin
    # load to which each EV belongs
    load_e = "load" .* string.(EVs)
    # bus to which each EV is connected (same as associated load)
    bus_e = [data_eng["load"][id]["bus"] for id in load_e]
    # phase terminal for each EV (same as associated load)
    phase_e = [data_eng["load"][id]["connections"][1] for id in load_e]
end;

# ╔═╡ f4344910-d95e-4f33-9b16-3f52c9ced4ac
md"""
Now we are ready to add the generators to the data model.
"""

# ╔═╡ 97017a8b-f86b-49b4-ac83-66303df1f63c
begin
    data_eng["generator"] = Dict{String,Any}()
    for e in EVs
        data_eng["generator"]["EV_gen_$e"] = Dict{String,Any}(
            "status" => ENABLED,
            "bus" => bus_e[e],
            "connections" => [phase_e[e], 4],
            "configuration" => WYE,
        )
    end
end;

# ╔═╡ 3a1abed7-d111-49e1-bcfb-ee9af48d4a6d
md"""
Transform the `ENGINEERING` data model to a `MATHEMATICAL ones`, and dont forget the `multinetwork=true` flag.
"""

# ╔═╡ 52c40603-42fe-45d3-840c-f530fc3951f2
data_math_mn = transform_data_model(data_eng, multinetwork=true);

# ╔═╡ b2e5c25e-e497-4080-ba74-2cfa8e6c05d4
md"""
Before solving the problem, it is important to add initialization values for the voltage variables. Failing to do so will almost always result in solver issues.
"""

# ╔═╡ 49e89e0f-480d-4dc1-850a-4ad9c4ae0ebd
add_start_vrvi!(data_math_mn);

# ╔═╡ 8ac591c5-e3d4-42bd-994c-7bc885536824
md"""
**Build PMD optimization model**

Generate the PMD optimization model based on the data model.
"""

# ╔═╡ 11ba8af0-50d3-4f5d-8c2b-4a1af9e5f5d5
pm = instantiate_mc_model(data_math_mn, IVRUPowerModel, build_mn_mc_opf);

# ╔═╡ 5ecc3cc4-0f8e-4ec5-8e2f-47a5871f1304
md"""
**Add EV charging model**

Start by extracting the JuMP model itself.
"""

# ╔═╡ bbba2730-31a7-4c27-ab88-c05f358b99b6
nc_model = pm.model;

# ╔═╡ ff40f2b8-1f77-4e2b-a0f8-27415a8e3978
md"""
Add the EV charging model to it. The code below is identical to the model in the previous section, except for the prefix `nc_` and the omission of the charge rate limit (`nc_P_ek[e,k]<=Pmax_e[e]`).
"""

# ╔═╡ 12d8fc45-a242-4c53-ac31-161af6331f0a
begin
    # charge for EV e and timestep k, in kW
    JuMP.@variable(nc_model, 0 <= nc_P_ek[e in EVs, k in K])
    # SoC for EV e at end of timestep k, in kWh
    JuMP.@variable(nc_model, 0 <= nc_E_ek[e in EVs, k in K] <= Emax_e[e])

    # relate SoC to charge
    # for the first timestemp, use initial SoC E0_e
    JuMP.@constraint(nc_model, [e in EVs],
        nc_E_ek[e, 1] == E0_e[e] + D_k[1] * nc_P_ek[e, 1]
    )
    # for the other timestemps, use the SoC at the preceding timestep
    JuMP.@constraint(nc_model, [e in EVs, k in K[2:end]],
        nc_E_ek[e, k] == nc_E_ek[e, k-1] + D_k[k] * nc_P_ek[e, k]
    )

    # define the dissatisfaction for each EV e at each timestep k
    nc_dissatisfaction_ek = [
        (Emax_e[e] - nc_E_ek[e, k]) / Emax_e[e]
        for e in EVs, k in K]
    # the objective is to minimize the total dissatisfaction across EVs and timesteps,
    # taking into account the duration of each step
    JuMP.@objective(nc_model, Min,
        sum(
            D_k[k] * nc_dissatisfaction_ek[e, k]
            for e in EVs, k in K)
    )
end;

# ╔═╡ 496df139-97ec-4d2a-9c13-1ecd61ffa64f
md"""
**Establish link between PMD and EV charging**
"""

# ╔═╡ 565a33a7-3789-4a22-b6e1-0333cc5b7324
gen_name2ind = Dict(gen["name"] => gen["index"] for (_, gen) in data_math_mn["nw"]["1"]["gen"]);


# ╔═╡ ea6237f8-d9f8-4cc5-a2f9-86f045254a6c
ev_gen_ind_e = [gen_name2ind["EV_gen_$e"] for e in EVs];

# ╔═╡ a64715f2-e8c3-480d-9f04-3897b96c5d4b
print(ev_gen_ind_e)

# ╔═╡ 3d42555f-99df-41d7-8726-9f8e990aedc0
begin
    nc_Pg_ek = [var(pm, k, :pg, ev_gen_ind_e[e]).data[1] for e in EVs, k in K]
    nc_Qg_ek = [var(pm, k, :qg, ev_gen_ind_e[e]).data[1] for e in EVs, k in K]
end;

# ╔═╡ 50fdfdd0-d361-4fb8-a04b-976526121b5e
nc_Pg_ek

# ╔═╡ f7b500a6-7dbf-44cb-9433-3b534f13cd6b
begin
    # link charge to generator models
    JuMP.@NLconstraint(nc_model, [e in EVs, k in K],
        nc_Pg_ek[e, k] == -nc_P_ek[e, k]
    )
    JuMP.@NLconstraint(nc_model, [e in EVs, k in K],
        nc_Qg_ek[e, k] == 0.0
    )
end;

# ╔═╡ 284972a8-0e9f-495d-b778-812d04febba1
md"""
**Solve**

As before, we could solve the JuMP model directly, i.e. `JuMP.optimize!(nc_model)`. However, it is better to use the PMD wrapper, because this will also generate a solution dictionary for the network variables.
"""

# ╔═╡ d07996ac-67de-41bd-a8b5-35c8e513b145
begin
    res = optimize_model!(pm, optimizer=Ipopt.Optimizer)
    res["termination_status"]
end

# ╔═╡ ef198d40-77c8-4aa4-8af7-f3a535c7c74b
md"""
**Inspect EV charging variables**

Finally, let's explore the new solution through a series of figures.

Below you can see how the SoC evolves for the EVs with the new charging schedule. By the end, all EVs are now fully charged.
"""

# ╔═╡ f39c50ef-2251-4249-b41b-0a2c87fe18e1
begin
    nc_E_ek_vals = JuMP.value.(nc_E_ek.data)
    Plots.plot(legend=:none, title="", xlabel="time [h]", ylabel="SoC[kWh]", ylim=[0, maximum(Emax_e)])
    for e in EVs
        Plots.plot!(timestamps, [E0_e[e], nc_E_ek_vals[e, :]...], markershape=:circle, markersize=3)
    end
    Plots.plot!()
end

# ╔═╡ 4cf28814-0a1c-4df6-9ab3-ecb4b89cd66f
md"""
Below you see the optimal charging rate setpoints for all EVs. These regularly exceed the conservative 5 kW limit (indicated with a gray line).
"""

# ╔═╡ 0d75f5db-16c0-4345-86ba-b72491ee82d1
begin
    nc_P_ek_vals = JuMP.value.(nc_P_ek.data)
    Plots.plot(legend=:none, title="", xlabel="time [h]", ylabel="charge [kW]")
    Plots.plot!([timestamps[1], timestamps[end-1]], [Pmax, Pmax], color=:gray, linewidth=2)
    for e in EVs
        Plots.scatter!(timestamps[1:end-1], nc_P_ek_vals[e, :], markershape=:circle, markersize=3)
    end
    Plots.plot!()
end

# ╔═╡ 37ab2c49-7016-417c-9715-a13a78ed50b8
md"""
**Inspect network variables**

First, we convert the `MATHEMATICAL` solution back to the `ENGINEERING` representation. This will allow us to inspect the results with the familiar component names and in SI-units. The `MATHEMATICAL` solution uses indices instead of string identifiers, and is in pu.
"""

# ╔═╡ 8795bf04-cdcb-445c-8916-f5ac6497cb79
begin
    sol_math = res["solution"]
    sol_eng = transform_solution(sol_math, data_math_mn)
end;

# ╔═╡ 19d23af9-27e6-4a54-bdfd-593835de0861
md"""
There are a total of 117x3 phase voltages. To keep the plot light, we extract below the voltage for only those terminals which have a load connected to them. Furthermore, we convert th valus again to pu, because that is how we specified the bounds as well.
"""

# ╔═╡ 4fdd43cd-dc38-4e42-b003-dfb9f2c87739
begin
    vm_pu_lk = fill(NaN, length(data_eng["load"]), length(K))
    for k in K, l in 1:length(data_eng["load"])
        bus_id = data_eng["load"]["load$l"]["bus"]
        bus_ind = data_math_mn["bus_lookup"][bus_id]
        sol_bus = sol_eng["nw"]["$k"]["bus"][bus_id]
        data_bus = data_eng["bus"][bus_id]
        vbase = data_math_mn["nw"]["$k"]["bus"]["$bus_ind"]["vbase"]
        phase = data_eng["load"]["load$l"]["connections"][1]
        ind = findfirst(data_bus["terminals"] .== phase)
        vm_pu_lk[l, k] = abs(sol_bus["vr"][ind] + im * sol_bus["vi"][ind]) / vbase
    end
end

# ╔═╡ da7d6303-490a-4073-9b10-440cb9e23097
md"""
Below you can find scatter plot of the load phase voltages at each timestep. The voltage bounds are indicated by the red lines.
"""

# ╔═╡ 4b41e9dc-5c29-4979-a43a-714b1eeb804d
begin
    Plots.plot(xlabel="time [h]", ylabel="load phase voltage [pu]", legend=:none)
    Plots.plot!([timestamps[K[1]], timestamps[K[end]]], [0.9, 0.9], color=:red, linewidth=3)
    Plots.plot!([timestamps[K[1]], timestamps[K[end]]], [1.1, 1.1], color=:red, linewidth=3)
    for k in K
        Plots.scatter!(fill(timestamps[k], length(data_eng["load"])), vm_pu_lk[:, k], markershape=:circle, markersize=3, label="")
    end
    Plots.
    Plots.plot!()
end