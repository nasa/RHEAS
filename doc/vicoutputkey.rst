KEY TO RHEAS VIC OUTPUT
=======================

Each file represents one 0.25 x 0.25 degree pixel, with the name representing the centre coordinates. 

Thus, eb_1.125_35.125 covers 1.00 N - 1.25 N; 35.00 E - 35.25 E.

Key to columns in each file:
----------------------------

* ``csp_``:

 * ``1``: Year
 * ``2``: Month
 * ``3``: Date
 * ``4``: Fractional area of snow cover (fraction)
 * ``5``: Depth of freezing fronts for each freezing front (cm)
 * ``6``: Depth of thawing fronts for each thawing front (cm)
 * ``7``: Snow pack albedo (fraction)
 * ``8``: Depth of snow pack (cm)

* ``eb_``:

 * ``1``: Year
 * ``2``: Month
 * ``3``: Date
 * ``4``: Net shortwave radiation at the surface (W/m2)
 * ``5``: Net longwave radiation at the surface (W/m2)
 * ``6``: Latent heat from the surface (W/m2)
 * ``7``: Sensible heat flux from the surface (W/m2)
 * ``8``: Ground heat flux plus heat storage in the top soil layer (W/m2)
 * ``9``: Energy of fusion (melting) in snowpack (W/m2)
 * ``10``: Net energy used to refreeze liquid water in snowpack (W/m2)
 * ``11``: Advected energy (W/m2)
 * ``12``: Rate of change in heat storage (W/m2)
 * ``13``: Rate of change in cold content in snow pack (W/m2)

* ``eva_``:

 * ``1``: Year
 * ``2``: Month
 * ``3``: Date
 * ``4``: Net evaporation from canopy interception (mm)
 * ``5``: Net transpiration from vegetation (mm)
 * ``6``: Net evaporation from bare soil (mm)
 * ``7``: Total soil moisture in layers that contain roots (mm)
 * ``8``: Total moisture interception storage in canopy (mm)
 * ``9``: Total net sublimation from snow pack (surface and blowing) (mm)
 * ``10``: Net sublimation from snow stored in canopy (mm)
 * ``11``: Scene aerodynamic conductance (tiles with overstory contribute overstory conductance; others contribute surface conductance) (m/s)

* ``sub_``:

 * ``1``: Year
 * ``2``: Month
 * ``3``: Date
 * ``4-6``: Total soil moisture content for each soil layer (mm)
 * ``7-9``: Soil temperature for each soil layer (C)
 * ``10-12``: Fraction of soil moisture (by mass) that is liquid, for each soil layer (fraction)
 * ``13-15``: Fraction of soil moisture (by mass) that is ice, for each soil layer (fraction)
 * ``16``: Vertical average of (soil moisture - wilting point)/(maximum soil moisture - wilting point) (mm/mm)

* ``sur_``:

 * ``1``: Year
 * ``2``: Month
 * ``3``: Date
 * ``4``: Snow surface temperature (C)
 * ``5``: Average vegetation canopy temperature (C)
 * ``6``: Bare soil surface temperature (C)
 * ``7``: Average surface temperature (C)
 * ``8``: Air temperature (C)
 * ``9``: Average surface albedo (fraction)
 * ``10``: Snow water equivalent in snow pack (including vegetation-intercepted snow) (mm)
 * ``11``: Snow interception storage in canopy (mm)
 * ``12``: Storage of liquid water and ice (not snow) on surface (ponding) (mm)
 * ``13``: Total moisture interception storage in canopy (mm)

* ``wb_``:

 * ``1``: Year
 * ``2``: Month
 * ``3``: Date
 * ``4``: Snowfall (mm)
 * ``5``: Rainfall (mm)
 * ``6``: Total net evaporation (mm)
 * ``7``: Surface runoff (mm)
 * ``8``: Baseflow out of the bottom layer (mm)
 * ``9``: Snow melt (mm)
 * ``10``: Refreezing of water in the snow (mm)
 * ``11``: Precipitation (mm)


.. _Source: http://vic.readthedocs.org/en/develop/Documentation/OutputVarList/
