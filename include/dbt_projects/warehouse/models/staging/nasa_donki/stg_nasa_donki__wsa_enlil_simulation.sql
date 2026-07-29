-- WSA+Enlil model runs — typed 1:1 view over raw.wsa_enlil_simulation.
--
-- These are forecasts, not observations: each row is a model run predicting
-- how solar wind and any CMEs will propagate, including an estimated shock
-- arrival time at Earth and predicted Kp indices.
--
-- CAREFUL: the API also returns `kp_18`, but it was empty across the verified
-- window so dlt did not materialise the column. Referencing it here would
-- break this model. Add it only after confirming it exists in raw — dlt will
-- create it on the first load that actually carries a value.
--
-- The CME inputs and impact list are nested arrays landing in
-- raw.wsa_enlil_simulation__cme_inputs and __impact_list, not staged here.

select
    simulation_id,
    model_completion_time,
    estimated_shock_arrival_time,
    estimated_duration,
    au,
    rmin_re,
    kp_90,
    kp_135,
    kp_180,
    is_earth_gb                     as is_earth_glancing_blow,
    is_earth_minor_impact,
    link,
    _dlt_load_id,
    _dlt_id
from {{ source('nasa_donki', 'wsa_enlil_simulation') }}
