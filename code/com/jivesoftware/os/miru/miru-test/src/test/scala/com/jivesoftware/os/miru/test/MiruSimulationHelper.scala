package com.jivesoftware.os.miru.test

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.jivesoftware.os.miru.api.MiruBackingStorage
import com.jivesoftware.os.jive.utils.logger.{MetricLogger, MetricLoggerFactory}
import io.gatling.core.Predef._
import io.gatling.core.util.IOHelper.withCloseable
import io.gatling.http.Predef._
import io.gatling.http.config.HttpProtocolBuilder
import org.apache.commons.lang.builder.HashCodeBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._

import scala.util.Random
import scala.util.control.Breaks._

/**
 * My scala is bad and I should feel bad.
 */
class MiruSimulationHelper {
}

// See properties in pom.xml
object SimulationParams {
  val hostPorts = System.getProperty("hostports", "localhost:49600")
  val httpHostsAndPorts: Array[String] = hostPorts.split(",") map (f => "http://" + f)

  val miruAddToIndexEndpoint = "/miru/writer/add/activities"
  val miruAddToWALEndpoint = "/miru/writer/wal/activities"
  val miruElectEndpoint = "/miru/config/elect/TENANT_ID/PARTITION_ID"
  val miruCheckEndpoint = "/miru/config/check/TENANT_ID/PARTITION_ID/STATE/STORAGE"
  val miruWarmEndpoint = "/miru/reader/warm"
  val miruQueryAggregateCustomEndpoint = "/miru/reader/aggregate/custom"
  val miruQueryAggregateInboxAllEndpoint = "/miru/reader/aggregate/inboxAll"
  val miruQueryAggregateInboxUnreadEndpoint = "/miru/reader/aggregate/inboxUnread"
  val miruQueryDistinctCustomEndpoint = "/miru/reader/distinct/custom"
  val miruQueryDistinctInboxAllEndpoint = "/miru/reader/distinct/inboxAll"
  val miruQueryDistinctInboxUnreadEndpoint = "/miru/reader/distinct/inboxUnread"
  val miruQueryTrendyEndpoint = "/miru/reader/trending/custom"

  val dataFilePrefix = System.getProperty("datafileprefix", "test")

  val numReplicas = System.getProperty("numreplicas", "3").toInt
  val numUsers = System.getProperty("numusers", "1").toInt
  val rampSeconds = System.getProperty("rampseconds", "10").toInt
  val walBatchSize = System.getProperty("batchsize", "100").toInt

  val onlineOpen = System.getProperty("onlineopen", MiruBackingStorage.hybrid.name)
  val onlineClosed = System.getProperty("onlineclosed", MiruBackingStorage.mem_mapped.name)

  val queryLoops = System.getProperty("queryloops", "1").toInt
  val queryPause = System.getProperty("querypause", "1000").toInt

  val bgBatchSize = System.getProperty("bgbatchsize", "10").toInt
  val bgPause = System.getProperty("bgpause", "1000").toLong
  val bgOffset = System.getProperty("bgoffset", "0").toInt

  def sequenceFile(suffix: String): Path = {
    new Path(dataFilePrefix + "." + suffix)
  }
}

object MiruHttpProtocol {

  def apply(hostsAndPorts: Array[String]): HttpProtocolBuilder = {
    if (hostsAndPorts.length == 1) {
      http.baseURL(hostsAndPorts(0))
    }
    else {
      http.baseURLs(hostsAndPorts(0), hostsAndPorts(1), hostsAndPorts slice(2, hostsAndPorts.length): _*)
    }
  }
}

object HadoopConfiguration {

  def apply(): Configuration = {
    val hadoopConfiguration = new Configuration
    hadoopConfiguration.set("fs.hdfs.impl", "org.apache.hadoop.fs.LocalFileSystem")
    hadoopConfiguration.set("dfs.block.size", (128 * 1024 * 1024).toString)
    hadoopConfiguration
  }
}

object MetaSequenceReader {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  def apply(metaSequenceFile: Path, hadoopConfiguration: Configuration): Map[String, String] = {
    val sequenceReader = new SequenceFile.Reader(hadoopConfiguration, SequenceFile.Reader.file(metaSequenceFile))
    withCloseable(sequenceReader) { reader =>
      val key: WritableComparable[_] = new Text()
      val value: Writable = new Text()
      var meta = Map[String, String]()
      while (reader.next(key, value)) {
        LOG.info("Meta {} -> {}", key.toString, value.toString, null)
        meta = meta + (key.toString -> value.toString)
      }
      meta
    }
  }
}

object ActivitySequenceFeeder {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val objectMapper: ObjectMapper = new ObjectMapper()

  def apply(activitySequenceFile: Path, activityBatchSize: Int, sequenceOffset: Int, hadoopConfiguration: Configuration): Feeder[JsonNode] = {
    val sequenceReader = new SequenceFile.Reader(hadoopConfiguration, SequenceFile.Reader.file(activitySequenceFile))
    val key: WritableComparable[_] = new LongWritable()
    val value: Writable = new Text()
    var readerHasNext: Boolean = sequenceReader.next(key, value)
    var readEmpty: Boolean = false

    val feeder = new Feeder[JsonNode] {
      override def hasNext: Boolean = {
        !readEmpty
      }

      override def next(): Map[String, JsonNode] = {
        sequenceReader.synchronized {
          var arrayNode: ArrayNode = new ArrayNode(objectMapper.getNodeFactory)
          try {
            while (readerHasNext && arrayNode.size < activityBatchSize) {
              arrayNode.add(objectMapper.readValue(value.toString, classOf[ObjectNode]))
              readerHasNext = sequenceReader.next(key, value)
              if (!readerHasNext) {
                LOG.info("Reader finished")
                sequenceReader.close()
              }
            }
          }
          catch {
            case e: Exception => {
              readerHasNext = false
              sequenceReader.close()
              throw e
            }
          }
          if (arrayNode.size() == 0) {
            readEmpty = true
          }
          Map("activities" -> arrayNode)
        }
      }
    }

    LOG.info("Fast forwarding to index " + sequenceOffset)
    breakable {
      for (p <- 0 until sequenceOffset) {
        if (!feeder.hasNext) {
          break()
        }
        feeder.next()
      }
    }

    feeder
  }
}

object QuerySequenceFeeder {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val objectMapper: ObjectMapper = new ObjectMapper()

  def apply(querySequenceFile: Path, queryLoops: Int, hadoopConfiguration: Configuration): Feeder[JsonNode] = {
    var sequenceReader = new SequenceFile.Reader(hadoopConfiguration, SequenceFile.Reader.file(querySequenceFile))
    val key: WritableComparable[_] = new LongWritable()
    val value: Writable = new Text()
    var readerHasNext: Boolean = sequenceReader.next(key, value)
    var loop: Int = 1

    new Feeder[JsonNode] {
      override def hasNext: Boolean = {
        this.synchronized {
          readerHasNext
        }
      }

      override def next(): Map[String, JsonNode] = {
        this.synchronized {
          try {
            val objectNode: ObjectNode = objectMapper.readValue(value.toString, classOf[ObjectNode])
            readerHasNext = sequenceReader.next(key, value)
            if (!readerHasNext) {
              LOG.info("Reader finished " + loop + " -> " + queryLoops + ": " + querySequenceFile.getName)
              sequenceReader.close()
              if (loop < queryLoops) {
                sequenceReader = new SequenceFile.Reader(hadoopConfiguration, SequenceFile.Reader.file(querySequenceFile))
                readerHasNext = sequenceReader.next(key, value)
                loop = loop + 1
                LOG.info("Loop " + loop + ": " + querySequenceFile.getName)
              }
            }
            Map("query" -> objectNode)
          }
          catch {
            case e: Exception => {
              readerHasNext = false
              sequenceReader.close()
              throw e
            }
          }
        }
      }
    }
  }
}

object PseudoRandomHosts {
  def apply(hostsAndPorts: Array[String], partitionId: Int, numReplicas: Int): Set[String] = {
    val r: Random = new Random(new HashCodeBuilder().append(numReplicas).append(partitionId).toHashCode)
    val shuffled: List[String] = r.shuffle(hostsAndPorts.toList) take numReplicas
    shuffled.toSet
  }
}
