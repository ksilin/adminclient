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
import org.apache.kafka.clients.admin.{AdminClient, CreatePartitionsResult, CreateTopicsResult, DescribeClusterResult, ListTopicsResult, NewPartitions, NewTopic}
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.errors.SaslAuthenticationException
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

class AdminClientCloudSpec extends AnyFreeSpec with Matchers with FutureConverter {

  val config: SpecConfig = SpecConfig()
  val userKafkaConfigName = "userKafkaApiKey" //
  val userCloudConfigName = "userCloudApiKey" //
  val userUiConfigName = "userUi" //
  val saConfigName = "serviceAccountKafkaApiKey"

  "cluster admin functions must work for cluster-level api-key" - {

    // ❯ ccloud kafka acl list --service-account 110947 --cluster lkc-8yjzq
    //
    //  ServiceAccountId | Permission | Operation | Resource |  Name   |  Type
    //+------------------+------------+-----------+----------+---------+---------+
    //  User:110947      | ALLOW      | READ      | GROUP    | acldemo | LITERAL
    //  User:110947      | ALLOW      | READ      | TOPIC    | acldemo | LITERAL
    //  User:110947      | ALLOW      | WRITE     | TOPIC    | acldemo | LITERAL

    // ❯ ccloud kafka acl create --allow --operation DESCRIBE --service-account 110947 --cluster-scope --cluster lkc-8yjzq
    //
    //  ServiceAccountId | Permission | Operation | Resource |     Name      |  Type
    //+------------------+------------+-----------+----------+---------------+---------+
    //  User:110947      | ALLOW      | DESCRIBE  | CLUSTER  | kafka-cluster | LITERAL

    val clusterName = "copsTamAwsKsiTst"
    val props: Properties = config.propertiesFor(userKafkaConfigName, clusterName)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to list topics" in {
      val res: ListTopicsResult               = client.listTopics() // describeCluster()
      val getn: KafkaFuture[util.Set[String]] = res.names()
      val getItDone: util.Set[String] = getn.get()
      println(getItDone)
    }

    "must be able to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val sf                         = toScalaFuture(getn)
      val clusterId                  = Await.result(sf, 10.seconds)
      println(clusterId)
    }

    val testTopicName = "adminClient_test"

    "must be able to create a topic" in {
      // TODO - delete the topic first
      val topic = new NewTopic(testTopicName, 1, 3.shortValue())
      val res: CreateTopicsResult               = client.createTopics(List(topic).asJava) // describeCluster()
      val getn: KafkaFuture[Void] = res.all()
      val getItDone: Void = getn.get()
    }

    "must be able to add partitions" in {
      val partitionIncrease = NewPartitions.increaseTo(2)
      val res: CreatePartitionsResult               = client.createPartitions(Map(testTopicName -> partitionIncrease).asJava) // describeCluster()
      val getn: KafkaFuture[Void] = res.all()
      val getItDone: Void = getn.get()
    }

  }

  "cluster admin functions must not be available for cloud-level api-key" - {

    val clusterName = "copsTamAwsKsiTst"
    val props: Properties = config.propertiesFor(userCloudConfigName, clusterName)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must not be able to authenticate to list topics" in {
      val res: ListTopicsResult               = client.listTopics() // describeCluster()
      val getn: KafkaFuture[util.Set[String]] = res.names()
      val sf                           = toScalaFuture(getn)
      val ex                         = intercept[ExecutionException](Await.result(sf, 10.seconds))
      val cause                      = ex.getCause
      cause mustBe a[SaslAuthenticationException]
      cause.getMessage mustBe "Authentication failed"
    }

    "must not be able to authenticate to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val sf                         = toScalaFuture(getn)
      val ex                         = intercept[ExecutionException](Await.result(sf, 10.seconds))
      val cause                      = ex.getCause
      cause mustBe a[SaslAuthenticationException]
      cause.getMessage mustBe "Authentication failed"
    }
  }

  "cluster admin functions must be available for cluster-level api-key for a service account" - {

    val clusterName = "copsTamAwsKsiTst"
    val props: Properties = config.propertiesFor(saConfigName, clusterName)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to list topics" in {
      val res: ListTopicsResult               = client.listTopics() // describeCluster()
      val getn: KafkaFuture[util.Set[String]] = res.names()
      val getItDone                           = getn.get()
      println(getItDone)
    }

    "must not be able to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val getItDone                           = getn.get()
      println(getItDone)
    }
  }

  "cluster admin functions are not available with UI user/pw " - {

    val clusterName = "copsTamAwsKsiTst"
    val props: Properties = config.propertiesFor(userUiConfigName, clusterName)
    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to get cluster id" in {
      val res: DescribeClusterResult = client.describeCluster()
      val getn: KafkaFuture[String]  = res.clusterId()
      val sf                         = toScalaFuture(getn)
      val ex                         = intercept[ExecutionException](Await.result(sf, 10.seconds))
      val cause                      = ex.getCause
      cause mustBe a[SaslAuthenticationException]
      cause.getMessage mustBe "Authentication failed"
    }

  }

}
