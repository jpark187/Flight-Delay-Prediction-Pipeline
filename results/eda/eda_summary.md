# EDA: 2024 sample (flights joined to weather)


## 1. Shape

7,079,061 flights, 57 columns.

| first_day | last_day | carriers | origins | routes | aircraft | cancel_rate | divert_rate | late_rate_operated |
|---|---|---|---|---|---|---|---|---|
| 2024-01-01 00:00:00 | 2024-12-31 00:00:00 | 15 | 348 | 6,805 | 6,112 | 0.014 | 0.002 | 0.208 |


## 2. Arrival delay distribution

Quantiles (1, 5, 25, 50, 75, 95, 99%): -37, -27, -15, -6, 9, 83, 215 minutes.  
Mean 7.1, min -126, max 3803; 62.0% of operated flights arrive early.


## 3. Late rate by scheduled departure hour, month, day of week

| hour | n | late |
|---|---|---|
| 0 | 12,102 | 0.200 |
| 1 | 4,001 | 0.213 |
| 2 | 1,511 | 0.208 |
| 3 | 1,069 | 0.195 |
| 4 | 526 | 0.202 |
| 5 | 197,159 | 0.091 |
| 6 | 487,660 | 0.096 |
| 7 | 486,842 | 0.123 |
| 8 | 476,025 | 0.139 |
| 9 | 390,538 | 0.153 |
| 10 | 433,405 | 0.165 |
| 11 | 436,330 | 0.177 |
| 12 | 413,834 | 0.191 |
| 13 | 416,993 | 0.213 |
| 14 | 400,009 | 0.234 |
| 15 | 401,151 | 0.250 |
| 16 | 401,988 | 0.270 |
| 17 | 435,578 | 0.278 |
| 18 | 426,686 | 0.294 |
| 19 | 380,007 | 0.300 |
| 20 | 311,135 | 0.304 |
| 21 | 230,458 | 0.284 |
| 22 | 162,702 | 0.272 |
| 23 | 57,537 | 0.213 |
| 24 | 1 | 0.000 |


## 4. Carriers and airports

| carrier | n | late | cancelled |
|---|---|---|---|
| WN | 1,419,419 | 0.206 | 0.008 |
| DL | 1,009,174 | 0.173 | 0.009 |
| AA | 984,306 | 0.261 | 0.015 |
| UA | 760,451 | 0.199 | 0.016 |
| OO | 744,658 | 0.190 | 0.011 |
| YX | 301,465 | 0.141 | 0.018 |
| MQ | 279,955 | 0.209 | 0.014 |
| NK | 261,103 | 0.239 | 0.019 |
| AS | 245,819 | 0.221 | 0.020 |
| B6 | 240,282 | 0.255 | 0.016 |
| OH | 227,971 | 0.218 | 0.016 |
| F9 | 208,624 | 0.287 | 0.023 |
| 9E | 200,094 | 0.158 | 0.023 |
| G4 | 117,210 | 0.217 | 0.017 |
| HA | 78,530 | 0.155 | 0.010 |

| origin | n | late |
|---|---|---|
| ATL | 341,910 | 0.199 |
| DFW | 313,582 | 0.271 |
| DEN | 308,645 | 0.228 |
| ORD | 280,052 | 0.237 |
| CLT | 217,555 | 0.270 |
| LAX | 194,053 | 0.188 |
| PHX | 193,551 | 0.191 |
| LAS | 189,252 | 0.218 |
| SEA | 163,724 | 0.215 |
| LGA | 162,432 | 0.181 |
| MCO | 159,553 | 0.244 |
| BOS | 143,490 | 0.204 |
| DCA | 140,016 | 0.201 |
| SFO | 134,978 | 0.221 |
| DTW | 131,367 | 0.192 |
| EWR | 128,979 | 0.223 |
| MSP | 126,779 | 0.189 |
| JFK | 123,348 | 0.211 |
| IAH | 115,940 | 0.244 |
| SLC | 113,247 | 0.173 |

Routes with 500+ flights: 3,547; route late rate ranges from 13.5% (10th pct) to 27.2% (90th pct).


## 5. Missing values

| column | null_share |
|---|---|
| d_snow_depth_cm | 0.999 |
| o_snow_depth_cm | 0.998 |
| cancellation_code | 0.986 |
| d_pres_wx | 0.937 |
| o_pres_wx | 0.937 |
| d_wind_gust_ms | 0.888 |
| o_wind_gust_ms | 0.888 |
| carrier_delay_min | 0.795 |
| nas_delay_min | 0.795 |
| late_aircraft_delay_min | 0.795 |
| security_delay_min | 0.795 |
| weather_delay_min | 0.795 |
| d_wind_dir_deg | 0.435 |
| o_wind_dir_deg | 0.431 |
| d_precip_mm | 0.372 |
| o_precip_mm | 0.372 |
| o_sea_level_hpa | 0.357 |
| d_sea_level_hpa | 0.357 |
| o_ceiling_m | 0.355 |
| d_ceiling_m | 0.355 |
| o_visibility_km | 0.355 |
| d_wind_speed_ms | 0.355 |
| o_wind_speed_ms | 0.355 |
| d_visibility_km | 0.355 |
| o_temp_c | 0.355 |
| d_temp_c | 0.355 |
| o_dewpoint_c | 0.355 |
| d_dewpoint_c | 0.355 |
| o_rel_humidity | 0.354 |
| d_rel_humidity | 0.354 |
| o_station | 0.354 |
| d_station | 0.354 |
| arr_delay_min | 0.016 |
| arr_del15 | 0.016 |
| dep_delay_min | 0.013 |
| tail_num | 0.003 |
| flight_num | 0.000 |
| crs_elapsed_min | 0.000 |

Target missing for 1.61% of flights; 100.0% of those are cancelled or diverted.  
Delay-cause minutes are missing for on-time flights by design (100.0% of their nulls).


## 6. Outliers and data errors

| late_over_12h | early_over_90min | bad_sched_duration | zero_distance | bad_dep_time | same_origin_dest |
|---|---|---|---|---|---|
| 7,548 | 5.000 | 2.000 | 0.000 | 0.000 | 0.000 |

Duplicate keys (date, carrier, flight number, origin, scheduled time): 0 groups.


## 7. What causes the delay minutes (late flights)

| cause | minutes | share |
|---|---|---|
| carrier | 35,823,263 | 0.345 |
| weather | 6,195,873 | 0.060 |
| nas | 19,621,964 | 0.189 |
| security | 179,928 | 0.002 |
| late_aircraft | 41,973,684 | 0.404 |


## 8. Weather at the origin before departure

Flights with origin weather: 64.5% (sample has weather for 30 airports).

**visibility (km)**
| bin | n | late |
|---|---|---|
| <1 | 30,314 | 0.351 |
| 1-3 | 42,750 | 0.370 |
| 3-5 | 79,355 | 0.331 |
| 5-10 | 130,389 | 0.273 |
| 10+ | 4,207,619 | 0.215 |

**wind (m/s)**
| bin | n | late |
|---|---|---|
| 0-3 | 1,529,472 | 0.189 |
| 3-6 | 2,133,015 | 0.226 |
| 6-9 | 694,829 | 0.258 |
| 9-12 | 118,070 | 0.300 |
| 12+ | 15,243 | 0.428 |

**temperature (°C)**
| bin | n | late |
|---|---|---|
| <-10 | 15,650 | 0.428 |
| -10-0 | 146,137 | 0.263 |
| 0-10 | 767,590 | 0.175 |
| 10-20 | 1,459,061 | 0.189 |
| 20-30 | 1,589,064 | 0.240 |
| 30+ | 514,160 | 0.306 |

**precipitation, past hour (mm)**
| bin | n | late |
|---|---|---|
| none | 4,155,073 | 0.213 |
| <1 | 116,038 | 0.349 |
| 1+ | 103,432 | 0.437 |

**Present-weather codes, most common (format to be decoded):**
| o_pres_wx | n | late |
|---|---|---|
| RA:61 | 189,257 | 0.327 |
| BR:1 | 94,568 | 0.217 |
| SN:71 | 32,025 | 0.585 |
| RA:65 | 27,785 | 0.419 |
| DZ:51 | 21,363 | 0.236 |
| HZ:05 | 20,129 | 0.220 |
| FG:2 | 15,013 | 0.275 |
| RA:63 | 12,635 | 0.542 |
| TS | 7,038 | 0.651 |
| BR:10 | 5,326 | 0.196 |
| BCFG:2 | 4,586 | 0.172 |
| MIFG:2 | 2,262 | 0.128 |
| TS:91 | 2,154 | 0.680 |
| SN:73 | 2,058 | 0.904 |
| TS:92 | 1,653 | 0.768 |
