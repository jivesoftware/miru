package com.jivesoftware.os.miru.test

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.jivesoftware.os.miru.api.base.MiruTenantId
import com.jivesoftware.os.miru.api.{MiruBackingStorage, MiruPartitionState}
import com.jivesoftware.os.miru.test.SimulationParams._
import com.jivesoftware.os.jive.utils.logger.{MetricLogger, MetricLoggerFactory}
import io.gatling.core.Predef._
import io.gatling.core.Predef.bootstrap._
import io.gatling.core.structure.ScenarioBuilder
import io.gatling.http.Predef._
import io.gatling.http.config.HttpProtocolBuilder
import org.apache.hadoop.conf.Configuration

import scala.concurrent.duration._

/**
 * My scala is bad and I should feel bad.
 */
class MiruOnlineSimulation extends Simulation {
  val LOG: MetricLogger = MetricLoggerFactory.getLogger

  val hadoopConfiguration: Configuration = HadoopConfiguration()
  var httpProtocol: HttpProtocolBuilder = MiruHttpProtocol(httpHostsAndPorts)

  val meta: Map[String, String] = MetaSequenceReader(sequenceFile("meta"), hadoopConfiguration)
  if (!meta.get("tenantId").isDefined || !meta.get("numPartitions").isDefined || !meta.get("closeFinalPartition").isDefined) {
    throw new IllegalStateException("Missing meta: " + meta)
  }

  val tenantId: String = meta.get("tenantId").orNull
  val numPartitions: Int = meta.get("numPartitions").orNull.toInt
  val closeFinalPartition: Boolean = meta.get("closeFinalPartition").orNull.toBoolean

  // Warm Scenario
  val scnWarm: ScenarioBuilder = WarmScenario(new MiruTenantId(tenantId.getBytes(Charsets.UTF_8)), httpHostsAndPorts)

  // Check Online Scenario
  val scnCheck: ScenarioBuilder = CheckScenario(tenantId, numPartitions, closeFinalPartition)

  setUp(scnWarm.inject(atOnce(1 users)), scnCheck.inject(atOnce(1 users)))
    .protocols(httpProtocol) //supposedly 2M4 will put this back on the individual scenarios
}

object WarmScenario {
  val objectMapper = new ObjectMapper()

  def apply(tenantId: MiruTenantId, hostsAndPorts: Array[String]): ScenarioBuilder = {
    val tenantIdJson: String = objectMapper.writeValueAsString(tenantId)

    var builder: ScenarioBuilder = scenario("miru warm scenario")
    hostsAndPorts.foreach(f => {
      builder = builder.exec(
        http("miru warm http")
          .post(miruWarmEndpoint)
          .body(StringBody(tenantIdJson))
          .asJSON)
    })

    builder
  }
}

object CheckScenario {
  def apply(tenantId: String, numPartitions: Int, closeFinalPartition: Boolean): ScenarioBuilder = {
    var builder = scenario("miru check scenario")

    for (p <- 0 until numPartitions) {
      val storage: String = if (!closeFinalPartition && p == numPartitions - 1) {
        onlineOpen
      } else {
        onlineClosed
      }
      val checkEndpoint = miruCheckEndpoint
        .replaceFirst("TENANT_ID", tenantId)
        .replaceFirst("PARTITION_ID", p.toString)
        .replaceFirst("STATE", MiruPartitionState.online.name())
        .replaceFirst("STORAGE", storage)

      val randomHosts: Set[String] = PseudoRandomHosts(httpHostsAndPorts, p, numReplicas)

      randomHosts.foreach(f => {
        val endpoint: String = f + checkEndpoint
        val attr: String = "online" + f

        builder = builder.asLongAs(session => !session.contains(attr)) {
          pause(1 seconds)
            .exec(
              http("miru check http")
                .post(endpoint)
                .check(status.is(204).saveAs(attr)))
        }
      })
    }

    builder
  }
}
