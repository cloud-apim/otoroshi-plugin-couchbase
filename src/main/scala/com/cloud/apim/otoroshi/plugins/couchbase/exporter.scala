package otoroshi_plugins.com.cloud.apim.plugins.couchbase

import akka.stream.scaladsl.{Sink, Source}
import com.couchbase.client.scala.{AsyncCluster, AsyncCollection, Cluster, ClusterOptions}
import com.couchbase.client.scala.env.{ClusterEnvironment, SecurityConfig}
import otoroshi.env.Env
import otoroshi.events.DataExporter.DefaultDataExporter
import otoroshi.events.{CustomDataExporter, CustomDataExporterContext, ExportResult}
import otoroshi.models.DataExporterConfig
import otoroshi.next.plugins.api.{NgPluginCategory, NgPluginVisibility, NgStep}
import otoroshi.utils.syntax.implicits._
import play.api.libs.json.JsValue

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class InternalCouchbaseDataExporter(config: DataExporterConfig, internalConfig: JsValue)(implicit ec: ExecutionContext, env: Env) extends DefaultDataExporter(config)(ec, env) {

  private val clusterRef = new AtomicReference[AsyncCluster]()
  private val collectionRef = new AtomicReference[AsyncCollection]()
  private val tableRef = new AtomicReference[String]()

  private def withCollection(f: AsyncCollection => Future[Either[String, Unit]]): Future[Either[String, Unit]] = {
    Option(collectionRef.get()) match {
      case None => Future.successful(Left("collection not setup"))
      case Some(collection) => f(collection)
    }
  }

  override def send(events: Seq[JsValue]): Future[ExportResult] = {
    implicit val ec = env.analyticsExecutionContext
    implicit val mat = env.analyticsMaterializer
    val workers = internalConfig.select("workers").asOpt[Int].getOrElse(4)
    Source(events.toList)
      .mapAsync(workers) { event =>
        val id = event.select("@id").asString
        withCollection { collection =>
          collection.insert(id, event)
            .map(_ => Right(()))
            .recover {
              case e =>
                e.printStackTrace()
                Left(e.getMessage)
            }
        }
      }
      .runWith(Sink.seq)
      .map { seq =>
        val errors = seq.collect {
          case Left(err) => err
        }
        if (errors.nonEmpty) {
          ExportResult.ExportResultFailure(errors.mkString(", "))
        } else {
          ExportResult.ExportResultSuccess
        }
      }
  }

  def onStart(): Unit = {
    val endpoint = internalConfig.select("endpoint").asOpt[String].getOrElse("127.0.0.1")
    val username = internalConfig.select("username").asOpt[String].getOrElse("admin")
    val password = internalConfig.select("password").asOpt[String].getOrElse("password")
    val bucketName = internalConfig.select("bucket").asOpt[String].getOrElse("otoroshi")
    val scope = internalConfig.select("scope").asOpt[String].getOrElse("prod")
    val collectionName = internalConfig.select("collection").asOpt[String].getOrElse("entities")
    val tls = internalConfig.select("tls").asOpt[Boolean].getOrElse(false)
    val wanProfile = internalConfig.select("wan-profile").asOpt[Boolean].getOrElse(true)

    val couchbaseEnv = ClusterEnvironment.builder
      // TODO: add more config
      .securityConfig(
        SecurityConfig()
          .enableTls(tls)
      )
      .applyOnIf(wanProfile)(_.applyProfile(ClusterEnvironment.WanDevelopmentProfile))
      .build
      .get

    println(s"creating connection to couchbase: couchbase${if (tls) "s" else ""}://${endpoint}/${bucketName}/${scope}/${collectionName}")

    val rawCluster = Cluster
      .connect(
        s"couchbase${if (tls) "s" else ""}://${endpoint}",
        ClusterOptions
          .create(username, password)
          .environment(couchbaseEnv)
      )
      .get

    val schemaDotTable = s"`${bucketName}`.${scope}.${collectionName}"

    val cluster = rawCluster.async

    val bucket = cluster.bucket(bucketName)
    Await.result(bucket.waitUntilReady(5.seconds), 6.seconds)

    val collection = bucket.scope(scope).collection(collectionName)

    tableRef.set(schemaDotTable)
    clusterRef.set(cluster)
    collectionRef.set(collection)
  }

  def onStop(): Unit = {
    Option(clusterRef.get()).foreach(_.disconnect())
  }
}

class CouchbaseDataExporter extends CustomDataExporter {

  private val ref = new AtomicReference[InternalCouchbaseDataExporter]()

  override def categories: Seq[NgPluginCategory] = Seq(NgPluginCategory.Custom("Cloud APIM"))
  override def steps: Seq[NgStep]                = Seq.empty
  override def visibility: NgPluginVisibility    = NgPluginVisibility.NgUserLand
  override def core: Boolean                     = false

  override def name: String                                = "Couchbase"
  override def description: Option[String]                 = "This exporter send otoroshi event in the Couchbase bucket of your choice".some

  override def accept(event: JsValue, ctx: CustomDataExporterContext)(implicit env: Env): Boolean = {
    ref.get().accept(event)
  }

  override def project(event: JsValue, ctx: CustomDataExporterContext)(implicit env: Env): JsValue = {
    ref.get().project(event)
  }

  override def send(events: Seq[JsValue], ctx: CustomDataExporterContext)(implicit ec: ExecutionContext, env: Env): Future[ExportResult] = {
    ref.get().send(events)
  }

  override def startExporter(ctx: CustomDataExporterContext)(implicit ec: ExecutionContext, env: Env): Future[Unit] = {
    ref.set(new InternalCouchbaseDataExporter(ctx.exporter.configUnsafe, ctx.config)(ec, env))
    ref.get().onStart()
    ().vfuture
  }

  override def stopExporter(ctx: CustomDataExporterContext)(implicit ec: ExecutionContext, env: Env): Future[Unit] = {
    ref.get().onStop()
    ().vfuture
  }
}
