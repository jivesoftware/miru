package com.jivesoftware.os.miru.test

import com.jivesoftware.os.miru.test.SimulationParams._
import com.jivesoftware.os.jive.utils.logger.{MetricLogger, MetricLoggerFactory}
import io.gatling.core.Predef._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import io.gatling.http.config.HttpProtocolBuilder
import org.apache.hadoop.conf.Configuration

/**
 * For each partition (as defined by meta), elects random hosts to the replica set.
 * Simulates host election.
 *
 * My scala is bad and I should feel bad.
 */
class MiruElectorSimulation extends Simulation {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val hadoopConfiguration: Configuration = HadoopConfiguration()
  val httpProtocol: HttpProtocolBuilder = MiruHttpProtocol(httpHostsAndPorts)

  val meta: Map[String, String] = MetaSequenceReader(sequenceFile("meta"), hadoopConfiguration)
  if (!meta.get("tenantId").isDefined || !meta.get("numPartitions").isDefined || !meta.get("closeFinalPartition").isDefined) {
    throw new IllegalStateException("Missing meta: " + meta)
  }

  val tenantId: String = meta.get("tenantId").orNull
  val numPartitions: Int = meta.get("numPartitions").orNull.toInt
  val closeFinalPartition: Boolean = meta.get("closeFinalPartition").orNull.toBoolean

  // Elector Scenario
  val electPartitions: Int = if (closeFinalPartition) numPartitions + 1 else numPartitions
  var scnElector: ScenarioBuilder = ElectorScenario(tenantId, electPartitions)

  setUp(scnElector.inject(atOnce(1 users)))
    .protocols(httpProtocol) //supposedly 2M4 will put this back on the individual scenarios
}

object ElectorScenario {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  def apply(tenantId: String, numPartitions: Int): ScenarioBuilder = {
    var scn = scenario("miru elector scenario")

    for (p <- 0 until numPartitions) {
      val electEndpoint = miruElectEndpoint.replaceFirst("TENANT_ID", tenantId).replaceFirst("PARTITION_ID", p.toString)
      val randomHosts: Set[String] = PseudoRandomHosts(httpHostsAndPorts, p, numReplicas)

      randomHosts.foreach(f => {
        val url = f + electEndpoint
        scn = scn.exec(
          http("miru elector http")
            .post(url))
      })
    }

    scn
  }
}
