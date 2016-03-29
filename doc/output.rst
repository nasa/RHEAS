Outputs
=======

These outputs can be chosen under the ``save``: section of the nowcast INI file. For example, typing:

  * ``save``: rootmoist, runoff

Will create two new tables in the save location you've chosen, with each row representing a date, containing a raster of the study area.

Raw VIC outputs
---------------

VIC offers many raw outputs:

  * ``net_long``:  net downward longwave flux [W/m2] 
  * ``net_short``:  net downward shortwave flux [W/m2]

  * ``snow_cover``:  fractional area of snow cover [fraction] 
  * ``salbedo``:  snow pack albedo [fraction] 
  * ``snow_depth``:  depth of snow pack [cm] 
  * ``tdepth``:  depth of thawing fronts [cm] for each thawing front
  * ``fdepth``:  depth of freezing fronts [cm] for each freezing front
  * ``rootmoist``:  root zone soil moisture  [mm] 
  * ``smfrozfrac``:  fraction of soil moisture (by mass) that is ice, for each soil layer 
  * ``smliqfrac``:  fraction of soil moisture (by mass) that is liquid, for each soil layer 
  * ``snow_canopy``:  snow interception storage in canopy  [mm] 
  * ``soil_moist``:  soil total moisture content  [mm] for each soil layer 
  * ``soil_wet``:  vertical average of (soil moisture - wilting point)/(maximum soil moisture - wilting point) [mm/mm] 
  * ``surfstor``:  storage of liquid water and ice (not snow) on surface (ponding) [mm] 
  * ``swe``:  snow water equivalent in snow pack (including vegetation-intercepted snow)  [mm] 
  * ``wdew``:  total moisture interception storage in canopy [mm] 
  * ``zwt``:  water table position [cm] (zwt within lowest unsaturated layer) 
  * ``zwt_lumped``:  lumped water table position [cm] (zwt of total moisture across all layers, lumped together) 
  * ``baseflow``:  baseflow out of the bottom layer  [mm]
  * ``evap``:  total net evaporation [mm]
  * ``evap_bare``:  net evaporation from bare soil [mm]
  * ``evap_canop``:  net evaporation from canopy interception [mm]
  * ``inflow``:  moisture that reaches top of soil column [mm]
  * ``prec``:  incoming precipitation [mm]
  * ``rainf``:  rainfall  [mm]
  * ``refreeze``:  refreezing of water in the snow  [mm]
  * ``runoff``:  surface runoff [mm]
  * ``snow_melt``:  snow melt  [mm]
  * ``snowf``:  snowfall  [mm]
  * ``transp_veg``:  net transpiration from vegetation [mm]
  * ``albedo``:  average surface albedo [fraction] 
  * ``baresoilt``:  bare soil surface temperature [C]
  * ``rad_temp``:  average radiative surface temperature [K] 
  * ``snow_pack_temp``:  snow pack temperature [C]
  * ``snow_surf_temp``:  snow surface temperature [C]
  * ``soil_temp``:  soil temperature [C] for each soil layer 
  * ``soil_tnode``:  soil temperature [C] for each soil thermal node 
  * ``surf_temp``:  average surface temperature [C]
  * ``vegt``:  average vegetation canopy temperature [C]
  * ``advection``:  advected energy [W/m2] 
  * ``grnd_flux``:  net heat flux into ground [W/m2] 
  * ``in_long``:  incoming longwave at ground surface (under veg) [W/m2] 
  * ``latent``:  net upward latent heat flux [W/m2] 
  * ``melt_energy``:  energy of fusion (melting) in snowpack [W/m2] 
  * ``r_net``:  net downward radiation flux [W/m2] 
  * ``rfrz_energy``:  net energy used to refreeze liquid water in snowpack [W/m2] 
  * ``sensible``:  net upward sensible heat flux [W/m2] 
  * ``snow_flux``:  energy flux through snow pack [W/m2] 
  * ``aero_cond``:  "scene" aerodynamic conductance [m/s]
  * ``air_temp``:  air temperature [C]
  * ``longwave``:  incoming longwave [W/m2] 
  * ``pressure``:  near surface atmospheric pressure [kPa]
  * ``qair``:  specific humidity [kg/kg] 
  * ``rel_humid``:  relative humidity [%]
  * ``shortwave``:  incoming shortwave [W/m2] 
  * ``tskc``:  cloud cover fraction [fraction] 
  * ``vegcover``:  fractional area of plants [fraction] 
  * ``wind``:  near surface wind speed [m/s] 

Derived outputs
---------------
RHEAS also computes derived indices using the raw VIC outputs. Current options include:


  * ``drought``:
  Along with being able to select the following drought indices independently, simply typing 'drought' will return the entire set. **Note that without lengthy data entry (decades, preferably), these indices are meaningless**.
  
  * ``spi1``: 30-day Standardised Precipitation Index. # of standard deviations above or below the norm for the previous 30 days' precipitation, compared with the long-term average.
  * ``spi3``: 90-day Standardised Precipitation Index. Same as above, over a 90-day period.
  * ``spi6``: 180-day Standardised Precipitation Index.
  * ``spi12``: 365-day Standardised Precipitation Index.
  
  * ``sri1``: 30-day Standardised Runoff Index. # of standard deviations above or below the norm for the previous 30 days' runoff, compared with the long-term average.
  * ``sri3``: 90-day Standardised Runoff Index.
  * ``sri6``: 180-day Standardised Runoff Index.
  * ``sri12``: 365-day Standardised Runoff Index.
  
  * ``dryspells``: Cumulative 14-day periods without precipitation.
  
  * ``severity``: Expressed as percentile comparing current to historical conditions.
  
  * ``smdi``: Soil Moisture Deficit Index. A scaled measurement with -4 meaning exceptionally dry, and +4 meaning exceptionally wet.
  
