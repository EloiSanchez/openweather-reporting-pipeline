with

weather as (
  select * from weather_rich
),

air_pollution as (
  select * from air_pollution_parsed
),

daily_general_report as (
  select
    weather.location,
    weather.recorded_at::date            as recorded_day,
    avg(weather.clouds)                  as clouds,
    avg(weather.temperature_feels_like)  as temperature_feels_like,
    avg(weather.avg_temperature)         as avg_temperature,
    max(weather.max_temperature)         as max_temperature,
    min(weather.min_temperature)         as min_temperature,
    avg(weather.humidity)                as humidity,
    avg(weather.pressure)                as pressure,
    avg(weather.degrees)                 as degrees,
    avg(weather.wind_gusts)              as wind_gusts,
    avg(weather.wind_speed)              as wind_speed,
    sum(weather.rain)                    as rain,
    max(weather.rain)                    as max_rain,
    avg(air_pollution.co)                as co,
    avg(air_pollution.nh3)               as nh3,
    avg(air_pollution.no)                as no,
    avg(air_pollution.no2)               as no2,
    avg(air_pollution.o3)                as o3,
    avg(air_pollution.pm10)              as pm10,
    avg(air_pollution.pm2_5)             as pm2_5,
    avg(air_pollution.so2)               as so2,
    avg(air_pollution.air_quality_index) as air_quality_index
  from weather
  left join air_pollution
    on weather.weather_id = air_pollution.air_pollution_id
  group by weather.location, recorded_day
)

select * from daily_general_report
