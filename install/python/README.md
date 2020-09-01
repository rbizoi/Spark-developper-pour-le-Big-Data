repGitHub=https://raw.githubusercontent.com/rbizoi/Spark-developper-pour-le-Big-Data/master/install/python

wget $repGitHub/GDAL-3.1.2-cp38-cp38-win_amd64.whl
wget $repGitHub/pyproj-2.6.1.post1-cp38-cp38-win_amd64.whl
wget $repGitHub/Fiona-1.8.13-cp38-cp38-win_amd64.whl
wget $repGitHub/Shapely-1.7.1-cp38-cp38-win_amd64.whl
wget $repGitHub/geopandas-0.8.1-py3-none-any.whl
wget $repGitHub/descartes-1.1.0-py2.py3-none-any.whl

pip install GDAL-3.1.2-cp38-cp38-win_amd64.whl
pip install pyproj-2.6.1.post1-cp38-cp38-win_amd64.whl
pip install Fiona-1.8.13-cp38-cp38-win_amd64.whl
pip install Shapely-1.7.1-cp38-cp38-win_amd64.whl
pip install .\geopandas-0.8.1-py3-none-any.whl
pip install descartes-1.1.0-py2.py3-none-any.whl

rm GDAL-3.1.2-cp38-cp38-win_amd64.whl
rm pyproj-2.6.1.post1-cp38-cp38-win_amd64.whl
rm Fiona-1.8.13-cp38-cp38-win_amd64.whl
rm Shapely-1.7.1-cp38-cp38-win_amd64.whl
rm geopandas-0.8.1-py3-none-any.whl
rm descartes-1.1.0-py2.py3-none-any.whl
