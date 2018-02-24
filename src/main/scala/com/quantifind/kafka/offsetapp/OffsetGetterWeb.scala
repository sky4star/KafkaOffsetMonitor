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

import org.joda.time.{Seconds, Period, LocalDateTime}
import courier._
import Defaults._


class OWArgs extends OffsetGetterArgs with UnfilteredWebApp.Arguments {
  @Required
  var retain: FiniteDuration = _

  @Required
  var refresh: FiniteDuration = _

  var watchGroupList: String = _

  var watchIntervalInSeconds: Int = 3

  var watchMailPassword : String = _

  var dbName: String = "offsetapp"

  var emailSender: String = _

  var emailSenderPassword: String = _

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
    try{
    info(s"reportOffsets")
    info(getGroups(args).mkString(","))
    info(s"start monitoring offsets for groups ${args.watchGroupList}")

    val groups = getGroups(args)
    var groupInfos = groups.map {
      g =>
          try {
            val inf = getInfo(g, args).offsets.toIndexedSeq
            info(s"reporting ${inf.size}")
            reporters.foreach(reporter => retryTask {
              reporter.report(inf)
            })
            (g, inf)
          } catch {
            case e: Exception =>
              info(s"get offset throws exception: ${e.getMessage} when getting")
              (g, null)
          }
    }
    groupInfos = groupInfos.filter(_._2 != null)

    // Overwatch and send mail if necessary
    info(s"reportOffsets monitoring offsets")
    info(s"start monitoring offsets for groups ${args.watchGroupList}")
    info(s"groups ${args.watchGroupList}")

    info(args.watchGroupList.split(",").mkString(":"))


    args.watchGroupList.split(",").foreach { group =>
      info("watch " + group)
    }

    info("1" + args.watchGroupList.split(",").mkString(":"))
    args.watchGroupList.split(",").foreach {
      group =>
          // Get topics
          info("getting topic")
          val topics = getTopicList(group, args)
          info(topics.mkString(","))
          val isTopicChanged = topics.map {
            topic =>
               try {
              val offsetHis = args.db.offsetHistory(group, topic)
              info("offsethis " + offsetHis.offsets.size)
              val offsetSumByTime = offsetHis.offsets.groupBy(_.timestamp).mapValues(offsets => offsets.map(_.offset).sum).toArray.sortBy(_._1).reverse

              // 检查时间，需要找到时间间隔在x内的
              val currentTime = new LocalDateTime()
              val lastOffsets = offsetSumByTime.filter {
                case (time, offset) =>
                  val jodaTime = new LocalDateTime(time)
                  Seconds.secondsBetween(jodaTime, currentTime).getSeconds <= args.watchIntervalInSeconds
              }
              lastOffsets.foreach(o => info(s"${o._1}, ${o._2}"))

              // 检查收尾两个值，offset有没有移动
              if (lastOffsets.size < 2) {
                warn(s"only get ${lastOffsets.size} offsets in watch interval ${args.watchIntervalInSeconds}")
                (topic, true)
              } else {
                val head = lastOffsets.head
                val curr = lastOffsets.last
                if (head._2 == curr._2) {
                  error(s"offset not move for group ${group}, topic ${topic}, timestamp ${curr._1}, offset ${curr._2}")
                  (topic, false)
                } else {
                  (topic, true)
                }
              }
               } catch {
                 case e: Exception =>
                   info(s"get offset throws exception: ${e.getMessage} when getting")
                   (topic, true)
               }
          }

          // 决定是否报警
          if (args.emailSender != null && args.emailSenderPassword != null) {
            val problemTopics = isTopicChanged.filter(_._2 == false).map(_._1)
            if (problemTopics.size > 0) {
              // 为了降低噪音，我们会忽略已经出问题的group
              if (problemGroup.contains(group))
                return

              // 发送邮件
              problemGroup += group
              info(s"will send email for group ${group} topic ${problemTopics.mkString(",")}")
              val mailer = Mailer("smtp.exmail.qq.com", 587)
                           .auth(true)
                           .as(args.emailSender, args.emailSenderPassword)
                           .startTtls(true)()
              mailer(Envelope.from("lei.yang" `@` "gaeamobile.com")
                     .to("lei.yang" `@` "gaeamobile.com")
                     .cc("lei.yang" `@` "gaeamobile.com")
                     .subject(s"zk offset monitor alert group[${group}] topics[${problemTopics.mkString(",")}]")
                     .content(Text("FYI"))).onSuccess {
                case _ => info("message delivered")
              }
            } else {
              // 从出问题的列表中删除这个组
              if (problemGroup.contains(group))
                problemGroup -= group
            }
          } else {
            info("email sender or password is null, will not check status nor send alert")
          }
    }
    } catch {
      case e:Exception =>
        info(s"parseLogs throws exception: ${e.getMessage} when parsing")
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

    info("create offset reporter")

    reporters = createOffsetInfoReporters(args)

    info("schedule jobs")
    info(args.toString())

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
