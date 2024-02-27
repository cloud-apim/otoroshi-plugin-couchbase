package otoroshi_plugins.com.cloud.apim.plugins.couchbase

import otoroshi.env.Env
import otoroshi.events.DataExporter.DefaultDataExporter
import otoroshi.events.{CustomDataExporter, CustomDataExporterContext, ExportResult}
import otoroshi.models.DataExporterConfig
import otoroshi.utils.syntax.implicits.BetterSyntax
import play.api.libs.json.JsValue

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

class InternalCouchbaseDataExporter(config: DataExporterConfig)(implicit ec: ExecutionContext, env: Env) extends DefaultDataExporter(config)(ec, env) {

  override def send(events: Seq[JsValue]): Future[ExportResult] = {
    ???
  }

  def onStart(): Unit = {

  }

  def onStop(): Unit = {

  }
}

class CouchbaseDataExporter extends CustomDataExporter {

  val ref = new AtomicReference[InternalCouchbaseDataExporter]()

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
    ref.set(new InternalCouchbaseDataExporter(ctx.exporter.configUnsafe)(ec, env))
    ref.get().onStart()
    ().vfuture
  }

  override def stopExporter(ctx: CustomDataExporterContext)(implicit ec: ExecutionContext, env: Env): Future[Unit] = {
    ref.get().onStop()
    ().vfuture
  }
}
