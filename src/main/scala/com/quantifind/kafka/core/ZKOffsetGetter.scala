package com.quantifind.kafka.core

import com.quantifind.kafka.OffsetGetter
import OffsetGetter.OffsetInfo
import com.quantifind.utils.ZkUtilsWrapper
import com.twitter.util.Time
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.zookeeper.data.Stat

import scala.collection._
import scala.util.control.NonFatal

/**
 * 简单起见，修改了这个默认的OffsetGetter
 * 用来监管spark streaming (direct stream)打印到zk中的offset
 * 路径即标准的consumer路径："/consumers" + "/" + group + "/offsets" + "/" + topic + "/" + partition, offset.toString
 */
class ZKOffsetGetter(theZkClient: ZkClient, zkUtils: ZkUtilsWrapper = new ZkUtilsWrapper) extends OffsetGetter {

  override val zkClient = theZkClient

  override def processPartition(group: String, topic: String, pid: Int): Option[OffsetInfo] = {
      val (offset, stat: Stat) = zkUtils.readData(zkClient, s"${zkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      val offsetInfo = OffsetInfo(group = group,
        topic = topic,
        partition = pid,
        offset = offset.toLong,
      logSize = 0L,  // logSize忽略不统计
      owner = Option(""),
      creation = Time.fromMilliseconds(stat.getCtime),
      modified = Time.fromMilliseconds(stat.getMtime))
      Option(offsetInfo)
  }

  override def getGroups: Seq[String] = {
    try {
      zkUtils.getChildren(zkClient, zkUtils.ConsumersPath)
    } catch {
      case NonFatal(t) =>
        error(s"could not get groups because of ${t.getMessage}", t)
        Seq()
    }
  }

  override def getTopicList(group: String): List[String] = {
    try {
      zkUtils.getChildren(zkClient, s"${zkUtils.ConsumersPath}/$group/offsets").toList
    } catch {
      case _: ZkNoNodeException => List()
    }
  }

  /**
   * Returns a map of topics -> list of consumers, including non-active
   */
  override def getTopicMap: Map[String, Seq[String]] = {
    try {
      zkUtils.getChildren(zkClient, zkUtils.ConsumersPath).flatMap {
        group => {
          getTopicList(group).map(topic => topic -> group)
        }
      }.groupBy(_._1).mapValues {
        _.unzip._2
      }
    } catch {
      case NonFatal(t) =>
        error(s"could not get topic maps because of ${t.getMessage}", t)
        Map()
    }
  }

  override def getActiveTopicMap: Map[String, Seq[String]] = {
    // Active这个定义暂时对我们没有意义，这里直接使用上面的getTopicMap
    getTopicMap
  }
}
