package com.jivesoftware.os.miru.test

import com.fasterxml.jackson.databind.JsonNode
import com.jivesoftware.os.miru.test.SimulationParams._
import com.jivesoftware.os.jive.utils.logger.{MetricLogger, MetricLoggerFactory}
import io.gatling.core.Predef._
import io.gatling.core.Predef.bootstrap._
import io.gatling.core.structure.{ChainBuilder, ScenarioBuilder}
import io.gatling.http.Predef._
import io.gatling.http.config.HttpProtocolBuilder
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration._

/**
 * Runs trending queries against a populated index. Queries are read from the trending "query" sequence files.
 *
 * My scala is bad and I should feel bad.
 */
class MiruTrendySimulation extends Simulation {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val hadoopConfiguration: Configuration = HadoopConfiguration()

  val meta: Map[String, String] = MetaSequenceReader(sequenceFile("meta"), hadoopConfiguration)
  if (!meta.get("tenantId").isDefined || !meta.get("numPartitions").isDefined || !meta.get("closeFinalPartition").isDefined) {
    throw new IllegalStateException("Missing meta: " + meta)
  }

  val tenantId: String = meta.get("tenantId").orNull
  val numPartitions: Int = meta.get("numPartitions").orNull.toInt
  val closeFinalPartition: Boolean = meta.get("closeFinalPartition").orNull.toBoolean

  // trendy scenarios will use the protocol baseURLs, so restrict to the hosts with the latest partition (mimics client behavior)
  val queryPartitionId: Int = if (closeFinalPartition) numPartitions else numPartitions - 1
  val queryHostsAndPorts: Set[String] = PseudoRandomHosts(httpHostsAndPorts, queryPartitionId, numReplicas)
  val httpProtocol: HttpProtocolBuilder = MiruHttpProtocol(queryHostsAndPorts.toArray)

  // Query Global Trending
  val globalTrendyFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.global-trendy"), queryLoops, hadoopConfiguration)
  val scnQueryGlobalTrendy: ScenarioBuilder = TrendyScenario("global", globalTrendyFeeder, miruQueryTrendyEndpoint)

  setUp(scnQueryGlobalTrendy.inject(atOnce(1 users)))
    .protocols(httpProtocol) //supposedly 2M4 will put this back on the individual scenarios
}

object TrendyScenario {

  def apply(name: String, feeder: Feeder[JsonNode], endpoint: String): ScenarioBuilder = {
    scenario("miru trendy scenario: " + name).asLongAs(session => feeder.hasNext) {
      feed(feeder)
        .exec(
          http("miru query http: " + name: String)
            .post(endpoint)
            .body(StringBody("${query}"))
            .asJSON)
        .pause(queryPause millis)
    }
  }
}
