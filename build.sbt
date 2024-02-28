import Dependencies._

ThisBuild / scalaVersion     := "2.12.13"
ThisBuild / version          := "1.0.0-dev"
ThisBuild / organization     := "com.cloud-apim"
ThisBuild / organizationName := "Cloud-APIM"

lazy val root = (project in file("."))
  .settings(
    name := "otoroshi-plugin-couchbase",
    resolvers += "jitpack" at "https://jitpack.io",
    test in assembly := {},
    assemblyJarName in assembly := "otoroshi-plugin-couchbase-assembly_2.12-dev.jar",
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "16.14.0" % "provided",
      "com.couchbase.client" %% "scala-client" % "1.5.1",
      munit % Test
    )
  )