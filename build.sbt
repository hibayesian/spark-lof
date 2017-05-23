name := "spark-lof"

version := "1.0"

scalaVersion := "2.11.8"

spName := "hibayesian/spark-lof"

sparkVersion := "2.1.1"

sparkComponents += "mllib"

resolvers += Resolver.sonatypeRepo("public")

spShortDescription := "spark-lof"

spDescription := """A parallel implementation of local outlier factor based on Spark"""
  .stripMargin

credentials += Credentials(Path.userHome / ".ivy2" / ".sbtcredentials")

licenses += "Apache-2.0" -> url("http://opensource.org/licenses/Apache-2.0")
    