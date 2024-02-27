package otoroshi_plugins.com.cloud.apim.plugins.couchbase

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.Materializer
import akka.util.ByteString
import com.couchbase.client.core.diagnostics.ClusterState
import com.couchbase.client.scala._
import com.couchbase.client.scala.env._
import com.couchbase.client.scala.json.{JsonArray, JsonObject, JsonObjectSafe}
import com.couchbase.client.scala.kv.{GetResult, MutateInOptions, MutateInSpec}
import otoroshi.cluster.ClusterMode
import otoroshi.env.Env
import otoroshi.storage.{DataStoreHealth, DataStores, DataStoresBuilder}
import otoroshi.utils.SchedulerHelper
import otoroshi.utils.syntax.implicits.{BetterConfiguration, BetterString, BetterSyntax}
import play.api.inject.ApplicationLifecycle
import play.api.{Configuration, Environment, Logger}
import storage.drivers.generic.{GenericDataStores, GenericRedisLike, GenericRedisLikeBuilder}

import java.util
import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.seqAsJavaListConverter
import scala.util.{Failure, Success}

class CouchbaseRedisLike(env: Env, logger: Logger, actorSystem: ActorSystem) extends GenericRedisLike {

  import actorSystem.dispatcher

  val endpoint = env.configuration.getOptionalWithFileSupport[String]("otoroshi.couchbase.endpoint").getOrElse("127.0.0.1")
  val username = env.configuration.getOptionalWithFileSupport[String]("otoroshi.couchbase.username").getOrElse("admin")
  val password = env.configuration.getOptionalWithFileSupport[String]("otoroshi.couchbase.password").getOrElse("password")
  val bucketName = env.configuration.getOptionalWithFileSupport[String]("otoroshi.couchbase.bucket").getOrElse("otoroshi")
  val scope = env.configuration.getOptionalWithFileSupport[String]("otoroshi.couchbase.scope").getOrElse("prod")
  val collectionName = env.configuration.getOptionalWithFileSupport[String]("otoroshi.couchbase.collection").getOrElse("entities")
  val tls = env.configuration.getOptionalWithFileSupport[Boolean]("otoroshi.couchbase.tls").getOrElse(false)
  val wanProfile = env.configuration.getOptionalWithFileSupport[Boolean]("otoroshi.couchbase.wan-profile").getOrElse(true)

  val couchbaseEnv = ClusterEnvironment.builder
    // TODO: add more config
    .securityConfig(
      SecurityConfig()
        .enableTls(tls)
    )
    .applyOnIf(wanProfile)(_.applyProfile(ClusterEnvironment.WanDevelopmentProfile))
    .build
    .get

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
  Await.result(bucket.waitUntilReady(30.seconds), 31.seconds)

  // TODO: create bucket, scope and collection if ok with user
  Await.result(cluster.query(s"CREATE INDEX idx_key ON ${schemaDotTable}(`key`);").recover {
    case e => logger.info("primary index already exists")
  }, 30.seconds)
  Await.result(cluster.query(s"CREATE INDEX idx_key ON ${schemaDotTable}(`expired_at`);").recover {
    case e => logger.info("expired_at index already exists")
  }, 30.seconds)

  val collection = bucket.scope(scope).collection(collectionName)

  private val cancel = new AtomicReference[Cancellable]()

  def setupCleanup(): Unit = {
    implicit val ec = env.otoroshiExecutionContext
    cancel.set(env.otoroshiScheduler.scheduleAtFixedRate(10.seconds, 40.second)(SchedulerHelper.runnable {
      cluster.query(s"DELETE FROM $schemaDotTable WHERE expired_at < ${System.currentTimeMillis()};").andThen {
        case Failure(exception) => exception.printStackTrace()
      }
    }))
  }

  setupCleanup()

  @inline
  private def measure[A](what: String)(fut: => Future[A]): Future[A] = {
    env.metrics.withTimerAsync(what)(fut)
  }

  private def arrayList[A](seq: Seq[A]): java.util.ArrayList[A] = {
    val list: java.util.List[A] = seq.asJava
    val alist = new util.ArrayList[A]()
    alist.addAll(list)
    alist
  }

  private def innerGet[A](key: String)(f: GetResult => Option[A]): Future[Option[A]] = {
    collection.get(key).map { res =>
      f(res)
    }.recover {
      case e => None
    }
  }

  private def innerInsert(key: String, value: JsonObject): Future[Unit] = {
    collection.insert(key, value).map(_ => ()).recover { case e => () }
  }

  private def createDoc(key: String, typ: String): JsonObject = {
    JsonObject(
      "key" -> key,
      "type" ->  typ,
      "value" -> null,
      "hvalue" -> JsonObject(),
      "lvalue" -> JsonArray(),
      "svalue" -> JsonArray(),
      "counter" -> 0,
      "at" -> System.currentTimeMillis(),
      "ttl" -> null,
      "expired_at" -> null,
    )
  }

  override def rawGet(key: String): Future[Option[Any]] = {
    innerGet(key) { res =>
      res.contentAs[JsonObjectSafe].toOption.flatMap { doc =>
        doc.str("type") match {
          case Success("counter") => doc.numLong("counter").getOrElse(0L).some
          case Success("string") => doc.str("value").toOption
          case Success("hash") => doc.obj("hvalue").toOption.map(obj => new TrieMap[String, ByteString]() ++ obj.toMap.mapValues(_.asInstanceOf[String].byteString))
          case Success("list") => doc.arr("lvalue").toOption.map(obj => new scala.collection.mutable.MutableList[ByteString]() ++ obj.toSeq.map(_.asInstanceOf[String].byteString))
          case Success("set") => doc.arr("lsvalue").toOption.map(obj => new scala.collection.mutable.HashSet[ByteString]() ++ obj.toSeq.map(_.asInstanceOf[String].byteString))
          case _ => None
        }
      }
    }
  }

  override def flushall(): Future[Boolean] = measure("couchbase.ops.flushall") {
    cluster.query(s"delete from $schemaDotTable where true;").map(_ => true)
  }

  override def setCounter(key: String, value: Long): Future[Unit] = measure("couchbase.ops.set_counter") {
    collection.upsert(key, createDoc(key, "counter").put("counter", value)).map(_ => ())
  }

  override def typ(key: String): Future[String] =  measure("couchbase.ops.type") {
    innerGet(key) {
      _.contentAs[JsonObject].toOption.map(_.str("type"))
    }.map(_.getOrElse("none"))
  }

  override def health()(implicit ec: ExecutionContext): Future[DataStoreHealth] = measure("couchbase.ops.health") {
    cluster.diagnostics().map { diag =>
      diag.state() match {
        case ClusterState.ONLINE => otoroshi.storage.Healthy
        case ClusterState.DEGRADED => otoroshi.storage.Unhealthy
        case ClusterState.OFFLINE => otoroshi.storage.Unreachable
      }
    }.recover {
      case e => otoroshi.storage.Unreachable
    }
  }

  override def stop(): Unit = ()

  override def get(key: String): Future[Option[ByteString]] = measure(s"couchbase.ops.get") {
    innerGet(key) { res =>
      res.contentAs[JsonObject].toOption.flatMap { doc =>
        doc.get("value") match {
          case obj: JsonObject => obj.toString().byteString.some
          case str: String => ByteString(str).some
          case _ => None
        }
      }
    }
  }

  override def mget(keys: String*): Future[Seq[Option[ByteString]]] = measure("couchbase.ops.mget") {
    val inValues = keys.map(v => s""""${v}"""").mkString(", ")
    cluster.query(
      s"select `key`, `value`, `counter`, `type` from $schemaDotTable where `key` in [$inValues];"
    ).map { result =>
      result.rowsAs[JsonObjectSafe].getOrElse(Seq.empty).flatMap(_.str("value").toOption).map(_.byteString.some)
    }
  }

  override def set(key: String, value: String, exSeconds: Option[Long], pxMilliseconds: Option[Long]): Future[Boolean] = {
    setBS(key, ByteString(value), exSeconds, pxMilliseconds)
  }

  override def setBS(key: String, value: ByteString, exSeconds: Option[Long], pxMilliseconds: Option[Long]): Future[Boolean] = measure(s"couchbase.ops.set") {
    val ttl = exSeconds.map(_ * 1000).orElse(pxMilliseconds)
    val valueStr = value.utf8String
    val jsonValue = if (valueStr.startsWith("{")) {
      JsonObject.fromJson(valueStr)
    } else {
      valueStr
    }
    collection.upsert(key, createDoc(key, "string").put("value", jsonValue).put("ttl", ttl.orNull)).map { res =>
      true
    }
  }

  override def del(keys: String*): Future[Long] = measure("couchbase.ops.del") {
    Future.sequence(keys.map(k => collection.remove(k))).map(_ => keys.size)
  }

  override def incr(key: String): Future[Long] = {
    incrby(key, 1L)
  }

  override def incrby(key: String, increment: Long): Future[Long] = measure("couchbase.ops.incrby") {
    for {
      _ <- innerInsert(key, createDoc(key, "counter"))
      mut <- collection.mutateIn(key, Seq(
        MutateInSpec.increment("counter", increment)
      ))
    } yield {
      mut.contentAs[Long](0).getOrElse(-1L)
    }
  }

  override def exists(key: String): Future[Boolean] = measure("couchbase.ops.exists") {
    collection.exists(key).map(_.exists)
  }

  override def keys(pattern: String): Future[Seq[String]] = measure("couchbase.ops.keys") {
    val processed = pattern.replace("*", ".*")
    cluster.query(s"""select `key` from ${schemaDotTable} where REGEXP_CONTAINS(`key`, "${processed}");""").map { qr =>
      qr.rowsAs[JsonObjectSafe].getOrElse(Seq.empty).flatMap(_.str("key").toOption)
    }
  }

  override def hdel(key: String, fields: String*): Future[Long] = measure("couchbase.ops.hdel") {
    collection.mutateIn(key, fields.map { field =>
      MutateInSpec.remove(s"hvalue.${field}")
    }).map { res =>
      fields.size
    }
  }

  override def hgetall(key: String): Future[Map[String, ByteString]] = measure("couchbase.ops.hgetall") {
    innerGet(key) { result =>
      result.contentAs[JsonObjectSafe].toOption.flatMap { doc =>
        doc.obj("hvalue").toOption.map { hash =>
          hash.toMap.collect {
            case (key, str: String) => (key, str.byteString)
          }.toMap
        }
      }
    }.map(_.getOrElse(Map.empty))
  }

  override def hset(key: String, field: String, value: String): Future[Boolean] = {
    hsetBS(key, field, value.byteString)
  }

  override def hsetBS(key: String, field: String, value: ByteString): Future[Boolean] = measure("couchbase.ops.hset") {
    // TODO: ensure doc has been written ???
    for {
      _ <- innerInsert(key, createDoc(key, "hash"))
      _ <- collection.mutateIn(key, Seq(
        MutateInSpec.upsert(s"hvalue.${field}", value.utf8String)
      ))
    } yield {
      true
    }
  }

  override def llen(key: String): Future[Long] = measure("couchbase.ops.llen") {
    innerGet(key) { res =>
      res.contentAs[JsonObjectSafe].toOption.map { doc =>
        doc.arr("lvalue") match {
          case Failure(exception) => 0
          case Success(arr) => arr.size
        }
      }
    }.map(_.getOrElse(0).toLong)
  }

  override def lpush(key: String, values: String*): Future[Long] = {
    lpushBS(key, values.map(_.byteString):_*)
  }

  override def lpushLong(key: String, values: Long*): Future[Long] = {
    lpushBS(key, values.map(_.toString.byteString):_*)
  }

  override def lpushBS(key: String, values: ByteString*): Future[Long] = measure("couchbase.ops.lpush") {
    // TODO: ensure doc has been written ???
    for {
      _ <- innerInsert(key, createDoc(key, "list"))
      _ <- collection.mutateIn(key, Seq(
        MutateInSpec.arrayPrepend(s"lvalue", values.map(_.utf8String))
      ))
    } yield {
      values.size
    }
  }

  override def lrange(key: String, start: Long, stop: Long): Future[Seq[ByteString]] = measure("couchbase.ops.lrange") {
    innerGet(key) { res =>
      res.contentAs[JsonObjectSafe].toOption.map { doc =>
        doc.arr("lvalue") match {
          case Failure(e) => Seq.empty
          case Success(arr) =>
            arr.toSeq.slice(start.toInt, stop.toInt - start.toInt).map(_.asInstanceOf[String].byteString)
        }
      }
    }.map(_.getOrElse(Seq.empty))
  }

  override def ltrim(key: String, start: Long, stop: Long): Future[Boolean] = measure("couchbase.ops.ltrim") {
    // TODO: ensure doc has been written ???
    for {
      _ <- innerInsert(key, createDoc(key, "list"))
      all <- lrange(key, 0, 10000L) // lol
      newAll = all.map(_.utf8String).slice(start.toInt, stop.toInt - start.toInt)
      _ <- collection.mutateIn(key, Seq(
       MutateInSpec.upsert(s"lvalue", JsonArray.fromSeq(newAll))
      ))
    } yield {
      true
    }
  }

  override def pttl(key: String): Future[Long] = measure("couchbase.ops.pttl") {
    innerGet(key) { result =>
      result.contentAs[JsonObjectSafe].toOption.map { doc =>
        val noTTL = doc.numLong("ttl").toOption.isEmpty
        if (noTTL) {
          -1L
        } else {
          val at = doc.numLong("at").getOrElse(0L)
          val ttl = doc.numLong("ttl").getOrElse(0L)
          val expired_at = at + ttl
          val now = System.currentTimeMillis()
          if (expired_at > now) {
            expired_at - now
          } else {
            0L
          }
        }
      }
    }
    .map(_.getOrElse(-1L))
  }

  override def ttl(key: String): Future[Long] = {
    pttl(key).map(_ / 1000)
  }

  override def expire(key: String, seconds: Int): Future[Boolean] = {
    pexpire(key, seconds * 1000)
  }

  override def pexpire(key: String, milliseconds: Long): Future[Boolean] = measure("couchbase.ops.pexpire") {
    collection.mutateIn(key, Seq(
      MutateInSpec.upsert("ttl", milliseconds),
      MutateInSpec.upsert("at", System.currentTimeMillis()),
      MutateInSpec.upsert("expired_at", System.currentTimeMillis() + milliseconds)
    ), MutateInOptions().copy(expiry = milliseconds.millis)).map(_ => true)
  }

  override def sadd(key: String, members: String*): Future[Long] = {
    saddBS(key, members.map(_.byteString):_*)
  }

  override def saddBS(key: String, members: ByteString*): Future[Long] = measure("couchbase.ops.sadd") {
    // TODO: ensure doc has been written ???
    for {
      _ <- innerInsert(key, createDoc(key, "set"))
      _ <- collection.mutateIn(key, members.map(_.utf8String).map { member =>
        MutateInSpec.arrayAddUnique(s"lvalue", member)
      })
    } yield {
      members.size
    }
  }

  override def sismember(key: String, member: String): Future[Boolean] = {
    sismemberBS(key, member.byteString)
  }

  override def sismemberBS(key: String, member: ByteString): Future[Boolean] = measure("couchbase.ops.sismember") {
    smembers(key).map(seq => seq.contains(member))
  }

  override def smembers(key: String): Future[Seq[ByteString]] = measure("couchbase.ops.smembers") {
    innerGet(key) { res =>
      res.contentAs[JsonObjectSafe].toOption.map { doc =>
        doc.arr("svalue") match {
          case Failure(e) => Seq.empty
          case Success(arr) => arr.toSeq.map(_.asInstanceOf[String].byteString).distinct
        }
      }
    }.map(_.getOrElse(Seq.empty))
  }

  override def srem(key: String, members: String*): Future[Long] = {
    sremBS(key, members.map(_.byteString):_*)
  }

  override def sremBS(key: String, members: ByteString*): Future[Long] = measure("couchbase.ops.srem") {
    for {
      arr <- smembers(key)
      jarr = arr.filterNot(bs => members.contains(bs)).map(_.utf8String)
      _ <- collection.mutateIn(key, Seq(MutateInSpec.replace(key, JsonArray.fromSeq(jarr))))
    } yield members.size
  }

  override def scard(key: String): Future[Long] = measure("couchbase.ops.scard") {
    smembers(key).map(seq => seq.size)
  }
}

class CouchbaseRedisLikeBuilder(caLogger: Logger) extends GenericRedisLikeBuilder {
  override def build(
    configuration: Configuration,
    environment: Environment,
    lifecycle: ApplicationLifecycle,
    clusterMode: ClusterMode,
    redisStatsItems: Int,
    actorSystem: ActorSystem,
    mat: Materializer,
    logger: Logger,
    env: Env
  ): GenericRedisLike = {
    new CouchbaseRedisLike(env, caLogger, actorSystem)
  }
}

class CouchbaseDataStores(
  configuration: Configuration,
  environment: Environment,
  lifecycle: ApplicationLifecycle,
  clusterMode: ClusterMode,
  redisStatsItems: Int,
  env: Env,
  caLogger: Logger,
) extends GenericDataStores(
  configuration,
  environment,
  lifecycle,
  clusterMode,
  redisStatsItems,
  new CouchbaseRedisLikeBuilder(caLogger),
  env
) {

  caLogger.info(s"Now using Couchbase DataStores")

}

class CouchbaseDataStoresBuilder extends DataStoresBuilder {
  private val logger = Logger("cloud-apim-couchbase-datastore")
  override def build(configuration: Configuration, environment: Environment, lifecycle: ApplicationLifecycle, clusterMode: ClusterMode, env: Env): DataStores = {
    new CouchbaseDataStores(
      configuration,
      environment,
      lifecycle,
      clusterMode,
      99,
      env,
      logger,
    )
  }
}
