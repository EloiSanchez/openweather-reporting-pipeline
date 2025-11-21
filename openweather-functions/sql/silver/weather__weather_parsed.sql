select
  id                                                as weather_id,
  main                                              as weather_name,
  description                                       as weather_description,
  icon,
  parent_id                                         as path_and_dt,
  staged_id,
  staged_at::timestamp                              as staged_at,
  regexp_extract(path_and_dt, '.*/(\w*).json.*', 1) as location,
  regexp_extract(path_and_dt, '.*.json-(.*)', 1)    as dt,
  location || '-' || dt                             as parent_id
from weather__weather
