# Spark SQL IQmulus Library

A library for reading and writing Lidar point cloud collections in PLY, LAS and XYZ formats from [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html).

[![Build Status](https://travis-ci.org/IGNF/spark-iqmulus.svg?branch=master)](https://travis-ci.org/IGNF/spark-iqmulus)
[![codecov.io](http://codecov.io/github/IGNF/spark-iqmulus/coverage.svg?branch=master)](http://codecov.io/github/IGNF/spark-iqmulus?branch=master)

## Requirements

This Spark package is for Spark 1.5+.

## Scala API

```scala
// import needed for the .avro method to be added
import fr.ign.spark.iqmulus.ply._
		
val sqlContext = new SQLContext(sc)

// The Lidar points are read, filtered
// and saved back to a new ply file(s)
val df = sqlContext.read.ply("myfile.ply")
df.filter("x > 0").write.ply("/tmp/outdir")
```
