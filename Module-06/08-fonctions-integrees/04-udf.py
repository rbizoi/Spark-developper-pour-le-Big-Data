from pyspark.sql.functions import *

@udf("double")
def celsiusKelvin(temp):
    return temp+273.15

@udf("double")
def celsiusFahrenheit(temp):
    return (temp*9.0/5.0)+32

meteoHexagone.select('temperature',
                     round(celsiusKelvin('temperature'),2)\
                           .alias('t_kelvin'),
                     round(celsiusFahrenheit('temperature'),2)\
                           .alias('t_fahrenheit')).show()

@udf("string")
def formatVille(ville):
    if ville in ['CLERMONT-FD','MONT-DE-MARSAN',
                                   'ST-PIERRE','ST-BARTHELEMY METEO'] :
        return ville.title()
    else :
        if ville.find('-') != -1 :
            return ville[0:ville.find('-')].title()
        else:
            return ville.title()

villes.select( col('Id').alias('id'),
               formatVille('ville').alias('ville'),
               'latitude',
               'longitude',
               'altitude')\
      .show(5,truncate=False)

import pandas as pd
from pyspark.sql.functions import *
from sklearn.preprocessing import scale

@pandas_udf(returnType=FloatType())
def normalisation(colonne: pd.Series)-> pd.Series:
    return pd.Series(scale(colonne))

meteo.where('id < 8000')\
     .na.drop()\
     .select('temperature',
             normalisation(meteo['temperature']).alias('normalisation'))\
     .show(10)

meteo.where('id < 8000')\
     .na.drop()\
     .count()


import pandas as pd
from pyspark.sql.functions import *

@pandas_udf(returnType=FloatType())
def correlation(col1: pd.Series,col2: pd.Series)-> pd.Series:
    return pd.Series(col1.corr(col2, method='pearson')).repeat(col1.size)

meteo.where('id < 8000')\
     .na.drop()\
     .select('temperature',
             'humidite',
              correlation( meteo['temperature'],
                          meteo['humidite'] ).alias('corr_h'),
              correlation( meteo['temperature'],
                          meteo['pression'] ).alias('corr_p'),
              correlation( meteo['temperature'],
                          meteo['visibilite'] ).alias('corr_v'),
              correlation( meteo['temperature'],
                          meteo['precipitations'] ).alias('corr_e'))\
     .show(5)
