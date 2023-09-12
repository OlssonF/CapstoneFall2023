library(tidyverse) # if not installed run install.packages('tidyverse')
library(arrow) # install.packages('arrow')
library(lubridate) # install.packages('lubridate')
# Observed weather
obs_met <- read_csv("https://s3.flare-forecast.org/targets/fcre_v2/fcre/observed-met_fcre.csv")

# Forecasted weather
forecast_dir <- arrow::s3_bucket(bucket = "drivers/noaa/gefs-v12/stage2/parquet/0",
                                 endpoint_override =  "s3.flare-forecast.org", 
                                 anonymous = TRUE)
# create a vector of dates to subset
forecast_dates <- seq.Date(lubridate::as_date('2023-03-01'), lubridate::as_date('2023-03-05'), by = 'day')

# running this will show you what the column names are
arrow::open_dataset(forecast_dir) 

#this dataset is VERY large and needs to be filtered before collecting
forecasted_met <- arrow::open_dataset(forecast_dir) |> 
  filter(site_id == 'fcre', # Falling Creek Reservoir (site_id code)
         reference_datetime %in% forecast_dates) |> 
  # you can also filter/select based on other columns in the dataset
  # collect brings the data into your local environment
  collect()

#===================================#
# Do join
# Wrangle the data into the same formats
forecasted_met <- 
  forecasted_met |> 
  tidyr::pivot_wider(names_from = variable, 
                     id_cols = c(horizon, parameter, reference_datetime, datetime),
                     values_from = prediction) |>
  
  # calculate wind speed from eastward and northward directions
  dplyr::mutate(wind_speed = sqrt(eastward_wind^2 + northward_wind^2)) |> 
  dplyr::select(#'site_id', 
                #'height',
                'horizon',
                'parameter',
                'reference_datetime', 
                'datetime', 
                "air_temperature",
                "air_pressure",
                "relative_humidity",
                "surface_downwelling_longwave_flux_in_air", 
                "surface_downwelling_shortwave_flux_in_air",
                "precipitation_flux",
                "wind_speed") |> 
  tidyr::pivot_longer(cols = air_temperature:wind_speed,
                      names_to = 'variable', values_to = 'prediction') 

met_joined <- dplyr::inner_join(forecasted_met, 
                                obs_met, 
                                by = c('datetime', 'variable'))


p1 <- met_joined |> 
  filter(variable == 'air_temperature',
         parameter == 1) |>  # just one ensemble member
  ggplot(aes(x=datetime, group = reference_datetime)) +
  geom_line(aes(y=prediction, colour = 'forecast'), alpha = 0.5) +
  geom_point(aes(y=observation), alpha = 0.2) +
  theme_bw() +
  labs(title = 'comparing refernce_datetimes')


p2 <- met_joined |> 
  filter(variable == 'air_temperature',
         reference_datetime == forecast_dates[1]) |>  # just one forecast
  ggplot(aes(x=datetime, group = parameter)) +
  geom_line(aes(y=prediction, colour = 'forecast'), alpha = 0.5) +
  geom_point(aes(y=observation), alpha = 0.2) +
  theme_bw()  +
  labs(title = 'comparing ensemble members, 1 reference_datetime')


p3 <- met_joined |> 
  filter(variable == 'air_temperature', 
         horizon %in% c(24, 168, 366, 840)) |>  # compare 1,7, and 14 days ahead
  ggplot(aes(x=horizon)) +
  geom_line(aes(y=prediction, 
                # colour = as.factor(horizon),
                group = interaction(reference_datetime, parameter)),
            alpha = 0.2) +
  geom_point(aes(y=prediction, 
                colour = as.factor(horizon),
                group = interaction(reference_datetime, parameter)),
            alpha = 0.8)  +
  geom_point(aes(y=observation), alpha = 0.2) +
  theme_bw() +
  labs(title = 'comparing horizons')

cowplot::plot_grid(p1,p2,p3, nrow = 3, align = 'hv')
