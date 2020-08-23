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




modelStd = StandardScaler()



import pandas as pd
from pyspark.sql.functions import *

def normalisation(colonne: pd.Series) -> pd.Series:
    return (colonne - colonne.mean(axis=0))/colonne.std(axis=0)

normalisation = pandas_udf(normalisation,returnType=FloatType())

meteo.where('id < 8000')\
     .na.drop()\
     .select('temperature',
             normalisation(meteo['temperature']).alias('norm'))\
     .show()
