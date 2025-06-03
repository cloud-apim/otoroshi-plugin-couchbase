import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "1.0.0-dev"
ThisBuild / organization     := "com.cloud-apim"
ThisBuild / organizationName := "Cloud-APIM"

lazy val root = (project in file("."))
  .settings(
    name := "otoroshi-plugin-couchbase",
    resolvers += "jitpack" at "https://jitpack.io",
    assembly / test  := {},
    assembly / assemblyJarName := "otoroshi-plugin-couchbase-assembly_2.12-dev.jar",
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "17.3.0" % "provided",
      "com.couchbase.client" %% "scala-client" % "1.5.1",
      munit % Test
    )
  )
