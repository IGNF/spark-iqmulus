# Spark SQL IQmulus Library

A library for reading and writing Lidar point cloud collections in PLY, LAS and XYZ formats from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/IGNF/spark-iqmulus.svg?branch=master)](https://travis-ci.org/IGNF/spark-iqmulus)
[![codecov.io](http://codecov.io/github/IGNF/spark-iqmulus/coverage.svg?branch=master)](http://codecov.io/github/IGNF/spark-iqmulus?branch=master)

## Requirements

This Spark package is for Spark 1.5+.

## Usage

The `spark-iqmulus` library is published as a [Spark package](http://spark-packages.org/package/IGNF/spark-iqmulus
), which can be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option. For example, to include it when starting the spark shell:

```sh
$ bin/spark-shell --packages IGNF:spark-iqmulus:0.1.0-s_2.10
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath. The `--packages` argument can also be used with `bin/spark-submit`. For now this library is only published for Scala 2.10.

## Scala API

Examples below require the creation of an SQLContext and the following imports, depending on which aspect of the library is being used:
```scala
val sqlContext = new SQLContext(sc)

import fr.ign.spark.iqmulus.ply._
import fr.ign.spark.iqmulus.las._
import fr.ign.spark.iqmulus.xyz._
```

Reading files as Spark dataframes:

```scala
val df1 = sqlContext.read.ply("myfile.ply")
val df2 = sqlContext.read.las("myfile.las")
val df3 = sqlContext.read.xyz("myfile.xyz")
```

Writing back Spark dataframes as files:


similarly, for XYZ and LAS :
```scala
val df = ...
df.write.ply("/tmp/ply")
df.write.las("/tmp/las")
df.write.xyz("/tmp/xyz")
```

## Acknowledgments

[<img alt="IGN France" align="right" src="http://ign.fr/sites/all/themes/ign_portail/logo.png">](http://www.ign.fr)
[<img alt="IQmulus" align="right" src="http://iqmulus.eu/img/logo.svg">](http://iqmulus.eu)
The development of this library is partly supported by:
* the EU FP7 Project [IQmulus](http://iqmulus.eu), which leverages the information hidden in large heterogeneous geospatial data sets and make them a practical choice to support reliable decision making (N. ICT- 2011-318787).
* [IGN](http://www.ign.fr), the French National Mapping Agency and its [MATIS](http://recherche.ign.fr/labos/matis) research lab.

## References
To our knowledge this library has been used in the following research:

* M. Brédif, B. Vallet, B. Ferrand. [Distributed dimensionality-based rendering of lidar point clouds](http://www.int-arch-photogramm-remote-sens-spatial-inf-sci.net/XL-3-W3/559/2015/isprsarchives-XL-3-W3-559-2015.html). Int. Arch. Photogramm. Remote Sens. Spatial Inf. Sci., XL-3/W3, 559-564, doi:10.5194/isprsarchives-XL-3-W3-559-2015, 2015.
[Paper](http://www.int-arch-photogramm-remote-sens-spatial-inf-sci.net/XL-3-W3/559/2015/isprsarchives-XL-3-W3-559-2015.pdf)
[Presentation](http://www.isprs-geospatialweek2015.org/workshops/geobigdata/data/p16_presentation.pdf)
[Poster](http://www.isprs-geospatialweek2015.org/workshops/geobigdata/data/p16_poster.pdf)
