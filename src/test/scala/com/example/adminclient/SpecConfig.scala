package com.example.adminclient

import java.util.Properties

import org.apache.kafka.clients.admin.AdminClientConfig
import pureconfig.ConfigReader.Result
import pureconfig._
import pureconfig.generic.ProductHint
import pureconfig.generic.auto._

case class SaslConfig(user: String, password: String, accountId: Int){
  val saslString: String = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${user}" password="${password}";""".stripMargin
  val principal = s"User:$accountId"
}

case class CloudClusterCoords( endpoint: String, name: String, lkc: String)

case class SpecConfig(configSource: ConfigObjectSource = ConfigSource.default) {

  private val CREDENTIALS = "credentials"
  private val CLUSTERS = "clusters"

  implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, CamelCase))

  private val getUserConfigs: Result[Map[String, SaslConfig]] = configSource.at(CREDENTIALS).load[Map[String, SaslConfig]]
  println(getUserConfigs)
  val userConfigMap: Map[String, SaslConfig] = getUserConfigs.getOrElse(Map.empty)

  val clustersMap: Map[String, CloudClusterCoords] =configSource.at(CLUSTERS).load[Map[String, CloudClusterCoords]].getOrElse(Map.empty)
  println(clustersMap)

  val propertiesBase: Properties = {
    val p = new Properties()
    p.setProperty("ssl.endpoint.identification.algorithm", "https")
    p.setProperty("sasl.mechanism", "PLAIN")
    p.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    p.setProperty(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "500")
    p.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    p
  }

  def propertiesFor(user: String, endpointName: String): Properties = {
    val newProps = new Properties()
    newProps.putAll(propertiesBase)
    newProps.setProperty("sasl.jaas.config", userConfigMap(user).saslString)
    newProps.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clustersMap(endpointName).endpoint)
    newProps
  }


}
