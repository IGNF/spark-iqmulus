resolvers += "Spark Package Main Repo" at "https://dl.bintray.com/spark-packages/maven"

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "5.2.1")

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

addSbtPlugin("org.spark-packages" % "sbt-spark-package" % "0.2.6")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.0")
