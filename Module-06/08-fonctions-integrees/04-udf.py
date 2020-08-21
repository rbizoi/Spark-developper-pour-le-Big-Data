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
