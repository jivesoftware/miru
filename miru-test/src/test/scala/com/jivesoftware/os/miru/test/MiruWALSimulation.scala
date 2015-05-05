package com.jivesoftware.os.miru.test

import com.fasterxml.jackson.databind.JsonNode
import com.jivesoftware.os.miru.test.SimulationParams._
import com.jivesoftware.os.jive.utils.logger.{MetricLogger, MetricLoggerFactory}
import io.gatling.core.Predef._
import io.gatling.core.Predef.bootstrap._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import io.gatling.http.config.HttpProtocolBuilder
import org.apache.hadoop.conf.Configuration

/**
 * Writes activity to the WAL, but does not trigger any indexing. Partitions need to sip/rebuild to catch up.
 * Simulates cluster client WAL writer.
 *
 * My scala is bad and I should feel bad.
 */
class MiruWALSimulation extends Simulation {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val hadoopConfiguration: Configuration = HadoopConfiguration()
  val httpProtocol: HttpProtocolBuilder = MiruHttpProtocol(httpHostsAndPorts)

  // WAL scenario
  val feeder: Feeder[JsonNode] = ActivitySequenceFeeder(sequenceFile("activity"), walBatchSize, 0, hadoopConfiguration)
  val scnWAL = WALScenario(feeder)

  setUp(scnWAL.inject(atOnce(1 users)))
    .protocols(httpProtocol) //supposedly 2M4 will put this back on the individual scenarios
}

object WALScenario {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  def apply(feeder: Feeder[JsonNode]): ScenarioBuilder = {
    scenario("miru WAL scenario")
      .asLongAs(session => feeder.hasNext) {
        feed(feeder)
        .exec(
          http("miru WAL http")
            .post(miruAddToWALEndpoint)
            .body(StringBody("${activities}"))
            .asJSON)
      }
  }
}
