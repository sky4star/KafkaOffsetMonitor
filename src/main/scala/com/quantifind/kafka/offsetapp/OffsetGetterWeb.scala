package com.quantifind.kafka.offsetapp

import java.lang.reflect.Constructor
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.quantifind.kafka.OffsetGetter
import com.quantifind.kafka.OffsetGetter.KafkaInfo
import com.quantifind.kafka.offsetapp.sqlite.SQLiteOffsetInfoReporter
import com.quantifind.sumac.validation.Required
import com.quantifind.utils.UnfilteredWebApp
import com.quantifind.utils.Utils.retry
import com.twitter.util.Time
import kafka.consumer.ConsumerConnector
import kafka.utils.Logging
import org.I0Itec.zkclient.ZkClient
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write
import org.json4s.{CustomSerializer, JInt, NoTypeHints}
import org.reflections.Reflections
import unfiltered.filter.Plan
import unfiltered.request.{GET, Path, Seg}
import unfiltered.response.{JsonContent, Ok, ResponseString}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.control.NonFatal

class OWArgs extends OffsetGetterArgs with UnfilteredWebApp.Arguments {
  @Required
  var retain: FiniteDuration = _

  @Required
  var refresh: FiniteDuration = _

  var overwatchGroupList: String = _

  var overwatchTimes: Int = 3

  var overwatchMailSmtp : String = _

  var overwatchMailPassword : String = _

  var dbName: String = "offsetapp"

  lazy val db = new OffsetDB(dbName)

  var pluginsArgs : String = _
}

/**
 * A webapp to look at consumers managed by kafka and their offsets.
 * User: pierre
 * Date: 1/23/14
 */
object OffsetGetterWeb extends UnfilteredWebApp[OWArgs] with Logging {

  implicit def funToRunnable(fun: () => Unit) = new Runnable() { def run() = fun() }

  def htmlRoot: String = "/offsetapp"

  val  scheduler : ScheduledExecutorService = Executors.newScheduledThreadPool(2)

  var reporters: mutable.Set[OffsetInfoReporter] = null

  var problemGroup: ArrayBuffer[String] = ArrayBuffer[String]()

  def retryTask[T](fn: => T) {
    try {
      retry(3) {
        fn
      }
    } catch {
      case NonFatal(e) =>
        error("Failed to run scheduled task", e)
    }
  }

  def reportOffsets(args: OWArgs) {
    val groups = getGroups(args)
    val groupInfos = groups.map {
      g =>
        val inf = getInfo(g, args).offsets.toIndexedSeq
        info(s"reporting ${inf.size}")
        reporters.foreach( reporter => retryTask { reporter.report(inf) } )
        (g, inf)
    }

    // Overwatch and send mail if necessary
    info(s"start monitoring offsets for groups ${args.overwatchGroupList}")
    args.overwatchGroupList.split(",").foreach {
      group =>
        // Get topics
        val topics = getTopicList(group, args)
        val isTopicChanged = topics.map {
          topic =>
            val offsetHis = args.db.offsetHistory(group, topic)
            val offsetSumByTime = offsetHis.offsets.groupBy(_.timestamp).mapValues(offsets => offsets.map(_.offset).sum ).toArray.sortBy(_._1).reverse
            val lastOffsets = offsetSumByTime.take(args.overwatchTimes)
            lastOffsets.foreach(o => info(s"${o._1}, ${o._2}"))

            // Compare head and tail
            val head = lastOffsets.head
            val curr = lastOffsets.last
            if (head._2 == curr._2) {
              error(s"offset not move for group ${group}, topic ${topic}, timestamp ${curr._1}, offset ${curr._2}")
              (topic, false)
            } else {
              (topic, true)
            }
        }

        // 决定是否报警
        val problemTopics = isTopicChanged.filter(_._2 == false)
        if (problemTopics.size > 0) {
          // 为了降低噪音，我们会忽略已经出问题的group
          if (problemGroup.contains(group))
            return

          // Send email
          problemGroup += group
          info(s"will send email for group ${group} topic ${problemTopics.mkString(",")}")
        } else {
          // 从出问题的列表中删除这个组
          if (problemGroup.contains(group))
            problemGroup -= group
        }
    }
  }

  def schedule(args: OWArgs) {

    scheduler.scheduleAtFixedRate( () => { reportOffsets(args) }, 0, args.refresh.toMillis, TimeUnit.MILLISECONDS )
    scheduler.scheduleAtFixedRate( () => { reporters.foreach(reporter => retryTask({reporter.cleanupOldData()})) }, args.retain.toMillis, args.retain.toMillis, TimeUnit.MILLISECONDS )

  }

  def withOG[T](args: OWArgs)(f: OffsetGetter => T): T = {
    var og: OffsetGetter = null
    try {
      og = OffsetGetter.getInstance(args)
      f(og)
    } finally {
      if (og != null) og.close()
    }
  }

  def getTopicList(group: String, args: OWArgs) = withOG(args) {
    _.getTopicList(group)
  }

  def getInfo(group: String, args: OWArgs): KafkaInfo = withOG(args) {
    _.getInfo(group)
  }

  def getGroups(args: OWArgs) = withOG(args) {
    _.getGroups
  }

  def getActiveTopics(args: OWArgs) = withOG(args) {
    _.getActiveTopics
  }
  def getTopics(args: OWArgs) = withOG(args) {
    _.getTopics
  }

  def getTopicDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicDetail(topic)
  }

  def getTopicAndConsumersDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicAndConsumersDetail(topic)
  }

  def getClusterViz(args: OWArgs) = withOG(args) {
    _.getClusterViz
  }

  override def afterStop() {

    scheduler.shutdown()
  }

  class TimeSerializer extends CustomSerializer[Time](format => (
    {
      case JInt(s)=>
        Time.fromMilliseconds(s.toLong)
    },
    {
      case x: Time =>
        JInt(x.inMilliseconds)
    }
    ))

  override def setup(args: OWArgs): Plan = new Plan {
    implicit val formats = Serialization.formats(NoTypeHints) + new TimeSerializer
    args.db.maybeCreate()

    reporters = createOffsetInfoReporters(args)

    schedule(args)

    def intent: Plan.Intent = {
      case GET(Path(Seg("group" :: Nil))) =>
        JsonContent ~> ResponseString(write(getGroups(args)))
      case GET(Path(Seg("group" :: group :: Nil))) =>
        val info = getInfo(group, args)
        JsonContent ~> ResponseString(write(info)) ~> Ok
      case GET(Path(Seg("group" :: group :: topic :: Nil))) =>
        val offsets = args.db.offsetHistory(group, topic)
        JsonContent ~> ResponseString(write(offsets)) ~> Ok
      case GET(Path(Seg("topiclist" :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopics(args)))
      case GET(Path(Seg("clusterlist" :: Nil))) =>
        JsonContent ~> ResponseString(write(getClusterViz(args)))
      case GET(Path(Seg("topicdetails" :: topic :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopicDetail(topic, args)))
      case GET(Path(Seg("topic" :: topic :: "consumer" :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopicAndConsumersDetail(topic, args)))
      case GET(Path(Seg("activetopics" :: Nil))) =>
        JsonContent ~> ResponseString(write(getActiveTopics(args)))
    }
  }

  def createOffsetInfoReporters(args: OWArgs) = {

    val reflections = new Reflections()

    val reportersTypes: java.util.Set[Class[_ <: OffsetInfoReporter]] = reflections.getSubTypesOf(classOf[OffsetInfoReporter])

    val reportersSet: mutable.Set[Class[_ <: OffsetInfoReporter]] = scala.collection.JavaConversions.asScalaSet(reportersTypes)

    // SQLiteOffsetInfoReporter as a main storage is instantiated explicitly outside this loop so it is filtered out
    reportersSet
      .filter(!_.equals(classOf[SQLiteOffsetInfoReporter]))
      .map((reporterType: Class[_ <: OffsetInfoReporter]) =>  createReporterInstance(reporterType, args.pluginsArgs))
      .+(new SQLiteOffsetInfoReporter(argHolder.db, args))
  }

  def createReporterInstance(reporterClass: Class[_ <: OffsetInfoReporter], rawArgs: String): OffsetInfoReporter = {
    val constructor: Constructor[_ <: OffsetInfoReporter] = reporterClass.getConstructor(classOf[String])
    constructor.newInstance(rawArgs)
  }
}
