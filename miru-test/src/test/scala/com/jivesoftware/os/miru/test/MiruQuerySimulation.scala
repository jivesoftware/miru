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
 * Runs queries against a populated index. Queries are read from the "query" sequence files.
 * Simultaneously writes new activities to the WAL and index. Activities are read from the "background" sequence file.
 * Simulates client queries with background ingress.
 *
 * My scala is bad and I should feel bad.
 */
class MiruQuerySimulation extends Simulation {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val hadoopConfiguration: Configuration = HadoopConfiguration()

  val meta: Map[String, String] = MetaSequenceReader(sequenceFile("meta"), hadoopConfiguration)
  if (!meta.get("tenantId").isDefined || !meta.get("numPartitions").isDefined || !meta.get("closeFinalPartition").isDefined) {
    throw new IllegalStateException("Missing meta: " + meta)
  }

  val tenantId: String = meta.get("tenantId").orNull
  val numPartitions: Int = meta.get("numPartitions").orNull.toInt
  val closeFinalPartition: Boolean = meta.get("closeFinalPartition").orNull.toBoolean

  // query scenarios will use the protocol baseURLs, so restrict to the hosts with the latest partition (mimics client behavior)
  val queryPartitionId: Int = if (closeFinalPartition) numPartitions else numPartitions - 1
  val queryHostsAndPorts: Set[String] = PseudoRandomHosts(httpHostsAndPorts, queryPartitionId, numReplicas)
  val httpProtocol: HttpProtocolBuilder = MiruHttpProtocol(queryHostsAndPorts.toArray)

  // Query Aggregate Custom Scenario
  val aggregateCustomFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.aggregate-custom"), queryLoops, hadoopConfiguration)
  val scnQueryAggregateCustom: ScenarioBuilder = QueryScenario("aggregate custom", aggregateCustomFeeder, miruQueryAggregateCustomEndpoint)

  // Query Aggregate Inbox All Scenario
  val aggregateInboxAllFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.aggregate-inbox"), queryLoops, hadoopConfiguration)
  val scnQueryAggregateInboxAll: ScenarioBuilder = QueryScenario("aggregate inbox all", aggregateInboxAllFeeder, miruQueryAggregateInboxAllEndpoint)

  // Query Aggregate Inbox Unread Scenario
  val aggregateInboxUnreadFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.aggregate-inbox"), queryLoops, hadoopConfiguration)
  val scnQueryAggregateInboxUnread: ScenarioBuilder = QueryScenario("aggregate inbox unread", aggregateInboxUnreadFeeder, miruQueryAggregateInboxUnreadEndpoint)

  // Query Distinct Custom Scenario
  val distinctCustomFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.distinct-custom"), queryLoops, hadoopConfiguration)
  val scnQueryDistinctCustom: ScenarioBuilder = QueryScenario("distinct custom", distinctCustomFeeder, miruQueryDistinctCustomEndpoint)

  // Query Distinct Inbox All Scenario
  val distinctInboxAllFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.distinct-inbox"), queryLoops, hadoopConfiguration)
  val scnQueryDistinctInboxAll: ScenarioBuilder = QueryScenario("distinct inbox all", distinctInboxAllFeeder, miruQueryDistinctInboxAllEndpoint)

  // Query Distinct Inbox Unread Scenario
  val distinctInboxUnreadFeeder: Feeder[JsonNode] = QuerySequenceFeeder(sequenceFile("query.distinct-inbox"), queryLoops, hadoopConfiguration)
  val scnQueryDistinctInboxUnread: ScenarioBuilder = QueryScenario("distinct inbox unread", distinctInboxUnreadFeeder, miruQueryDistinctInboxUnreadEndpoint)

  // Background Index Scenario
  val backgroundFeeder: Feeder[JsonNode] = ActivitySequenceFeeder(sequenceFile("background"), bgBatchSize, bgOffset, hadoopConfiguration)
  val backgroundPartitionId: Int = if (closeFinalPartition) numPartitions else numPartitions - 1
  val scnIndex: ScenarioBuilder = BackgroundScenario(backgroundPartitionId, backgroundFeeder, List(
    aggregateCustomFeeder, aggregateInboxAllFeeder, aggregateInboxUnreadFeeder, distinctCustomFeeder, distinctInboxAllFeeder, distinctInboxUnreadFeeder))

  val usersPerScenario = Math.max(numUsers / 6, 1) //TODO weight query types
  setUp(
    scnQueryAggregateCustom.inject(ramp(usersPerScenario users) over (rampSeconds seconds)),
    scnQueryAggregateInboxAll.inject(ramp(usersPerScenario users) over (rampSeconds seconds)),
    scnQueryAggregateInboxUnread.inject(ramp(usersPerScenario users) over (rampSeconds seconds)),
    scnQueryDistinctCustom.inject(ramp(usersPerScenario users) over (rampSeconds seconds)),
    scnQueryDistinctInboxAll.inject(ramp(usersPerScenario users) over (rampSeconds seconds)),
    scnQueryDistinctInboxUnread.inject(ramp(usersPerScenario users) over (rampSeconds seconds)),
    scnIndex.inject(atOnce(1 users)))
    .protocols(httpProtocol) //supposedly 2M4 will put this back on the individual scenarios
}

object QueryScenario {

  def apply(name: String, feeder: Feeder[JsonNode], endpoint: String): ScenarioBuilder = {
    scenario("miru query scenario: " + name).asLongAs(session => feeder.hasNext) {
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

object BackgroundScenario {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  def apply(partitionId: Int, backgroundFeeder: Feeder[JsonNode], queryFeeders: List[Feeder[JsonNode]]): ScenarioBuilder = {
    val hostsAndPorts: Set[String] = PseudoRandomHosts(httpHostsAndPorts, partitionId, numReplicas)

    scenario("miru background scenario").asLongAs(session => {
      var queriesRemaining: Boolean = false
      queryFeeders.foreach(f => queriesRemaining |= f.hasNext)
      backgroundFeeder.hasNext && queriesRemaining
    }) {
      var chainBuilder: ChainBuilder = feed(backgroundFeeder)
        .exec(
          http("miru background wal http")
            .post(miruAddToWALEndpoint)
            .body(StringBody("${activities}"))
            .asJSON)

      hostsAndPorts.foreach(f => {
        val endpoint = f + miruAddToIndexEndpoint
        chainBuilder = chainBuilder.exec(
          http("miru background index http")
            .post(endpoint)
            .body(StringBody("${activities}"))
            .asJSON)
      })

      chainBuilder.pause(bgPause millis)
    }
  }
}
