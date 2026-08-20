import Dependencies._

ThisBuild / scalaVersion     := "3.8.4"
ThisBuild / version          := "1.0.0-dev"
ThisBuild / organization     := "com.cloud-apim"
ThisBuild / organizationName := "Cloud-APIM"

lazy val root = (project in file("."))
  .settings(
    name := "otoroshi-plugin-couchbase",
    resolvers += "jitpack" at "https://jitpack.io",
    scalacOptions ++= Seq(
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Wunused:all",
      // the wasm4s "bundle" jar (transitive, provided) vendors an older scala 3 stdlib where
      // `scala.caps` is an object while scala-library 3.8.4 declares it as a package. otoroshi
      // itself silences the very same warning.
      "-Wconf:msg=package scala contains object and package with same name:s",
    ),
    assembly / test  := {},
    assembly / assemblyJarName := "otoroshi-plugin-couchbase-assembly_3-dev.jar",
    // the couchbase scala-client jar vendors its own recompiled copy of the upickle classes it
    // already depends on (com.lihaoyi:upickle_3:3.3.1). both copies expose the exact same api,
    // so keeping a single one is enough — everything else keeps the default strategy
    assembly / assemblyMergeStrategy := {
      case PathList("upickle", _*) => MergeStrategy.first
      case path                    => MergeStrategy.defaultMergeStrategy(path)
    },
    // otoroshi already provides the exact same scala3-library at runtime, no need to ship a
    // second copy of the whole stdlib in the plugin jar
    assembly / assemblyPackageScala / assembleArtifact := false,
    libraryDependencies ++= Seq(
      "fr.maif" %% "otoroshi" % "18.0.0-preview2" % "provided",
      "com.couchbase.client" %% "scala-client" % "3.12.2",
      munit % Test
    )
  )
