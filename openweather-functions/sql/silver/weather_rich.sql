with

weather as (
  select * from weather_parsed
),

weather_detail as (
  select
    parent_id,

    -- use max in order to remove randomness
    max(weather_name)        as weather_name,
    max(weather_description) as weather_description

  from weather__weather_parsed
  group by parent_id
),

weather_rich as (
  select
    weather.weather_id,
    weather.location,
    weather.recorded_at,
    weather.clouds,
    weather.temperature_feels_like,
    weather.avg_temperature,
    weather.max_temperature,
    weather.min_temperature,
    weather.humidity,
    weather.pressure,
    weather.degrees,
    weather.wind_gusts,
    weather.wind_speed,
    weather.rain,
    detail.weather_name,
    detail.weather_description
  from weather
  left join weather_detail as detail on
    weather.weather_id = detail.parent_id
)

select * from
  weather_rich
