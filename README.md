# Spark SQL IQmulus Library

A library for reading and writing Lidar point cloud collections in PLY, LAS and XYZ formats from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/IGNF/spark-iqmulus.svg?branch=master)](https://travis-ci.org/IGNF/spark-iqmulus)
[![codecov.io](http://codecov.io/github/IGNF/spark-iqmulus/coverage.svg?branch=master)](http://codecov.io/github/IGNF/spark-iqmulus?branch=master)

## Requirements

This Spark package is for Spark 1.5+.

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
