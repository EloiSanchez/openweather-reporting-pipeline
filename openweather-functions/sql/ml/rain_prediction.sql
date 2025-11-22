with daily_weather as (
  select
    recorded_day,
    location,
    clouds,
    temperature_feels_like,
    avg_temperature,
    max_temperature,
    min_temperature,
    humidity,
    pressure,
    degrees,
    wind_gusts,
    wind_speed,
    rain,
    max_rain,
    lead(rain)
      over (partition by location order by recorded_day asc)
      as next_day_rain,
    lead(max_rain)
      over (partition by location order by recorded_day asc)
      as next_day_max_rain,
    case
      when next_day_rain > 0.5
        then
          case
            when next_day_max_rain between 0.0 and 2.5 then 'light'
            when next_day_max_rain between 2.5 and 7.5 then 'moderate'
            when next_day_max_rain > 7.5 then 'intense'
          end
      else 'dry'
    end
      as rain_prediction
  from daily_general_report
)

select * exclude (next_day_rain, next_day_max_rain)
from daily_weather
