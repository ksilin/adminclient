/*
 * Copyright 2020 ksilin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.adminclient

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, DescribeClusterResult, ListTopicsResult}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._

class AdminClientCloudSpec extends AnyFreeSpec with Matchers with FutureConverter {

  val endpoint = "pkc-lq8v7.eu-central-1.aws.confluent.cloud:9092"

  val props = new Properties()
  props.setProperty("ssl.endpoint.identification.algorithm", "https")
  props.setProperty("sasl.mechanism", "PLAIN")
  props.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
  props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint)
  props.setProperty(AdminClientConfig.RETRY_BACKOFF_MS_CONFIG, "500")
  props.setProperty(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")

  "cluster admin functions must work for cluster-level api-key" - {

    // its for the cluster lkc-8yjzq

    val api_key= "GTVUUTDLNRS2VFDC"
    val secret  = "ftkrkMdns17iod1W+gu+K7FG14KP5RwMefLVgSfuhL6Wyby9D65ipX9ACLT1kXq4"
    val saslString =
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${api_key}" password="${secret}";""".stripMargin
    props.setProperty("sasl.jaas.config", saslString)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to list topics" in {
      val res: ListTopicsResult               = client.listTopics() // describeCluster()
      val getn: KafkaFuture[util.Set[String]] = res.names()
      val getItDone                           = getn.get()
      println(getItDone)
    }

    "must be able to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val sf                         = toScalaFuture(getn)
      val clusterId                  = Await.result(sf, 10.seconds)
      println(clusterId)
    }
  }

  "cluster admin functions must not be available for cloud-level api-key" - {

    // cloud scope
    val api_key = "JRMQFLXLI75J6Q6X"
    val secret  = "BuvdVykfqf0qeJMia25v1lRTdY1hUQRJPhZT4LlvkNoITenSqqYfqSlghSE5Rr6c"
    val saslString =
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${api_key}" password="${secret}";"""
    props.setProperty("sasl.jaas.config", saslString)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val sf                         = toScalaFuture(getn)
      val ex                  = intercept[ExecutionException]{Await.result(sf, 10.seconds)}
      val cause = ex.getCause
      cause mustBe a[SaslAuthenticationException]
      cause.getMessage mustBe "Authentication failed: Invalid username or password"
    }
  }

  "cluster admin functions are not available with UI user/pw " - {

    // user scope
    val api_key = "konstantin.silin+copstam@confluent.io"
    val secret  = "@pPp=@8q3q!Kj!7LMJ"
    val saslString = s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${api_key}" password="${secret}";"""
    props.setProperty("sasl.jaas.config", saslString)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val sf                         = toScalaFuture(getn)
      val ex                  = intercept[ExecutionException]{Await.result(sf, 10.seconds)}
      val cause = ex.getCause
      cause mustBe a[SaslAuthenticationException]
      cause.getMessage mustBe "Authentication failed: Invalid username or password"
    }

  }

}
