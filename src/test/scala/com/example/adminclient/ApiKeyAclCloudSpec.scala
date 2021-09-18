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
import org.apache.kafka.clients.admin.{AdminClient, CreateAclsResult}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter, AclOperation, AclPermissionType}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourcePatternFilter, ResourceType}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.must.Matchers

import scala.jdk.CollectionConverters._
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ApiKeyAclCloudSpec extends AnyFreeSpec with Matchers with FutureConverter {

  val config: SpecConfig = SpecConfig()
  val userConfigName = "userKafkaApiKey" //
  val saConfigName = "serviceAccountKafkaApiKey"

  "check and adjust ACLs" - {

    val clusterName = "copsTamAwsKsiTst"
    val props: Properties = config.propertiesFor(userConfigName, clusterName)

    // !!! requires kafka dependency, otherwise, future resolution fails with a 'Timeout'
    val client: AdminClient = AdminClient.create(props)

    "must be able to get all ACLs in cluster" in {
      val getAcls = client.describeAcls(AclBindingFilter.ANY).values()
      val sf: Future[util.Collection[AclBinding]] = toScalaFuture(getAcls)
      val acls: Iterable[AclBinding] = Await.result(sf, 10.seconds).asScala

      println(s"all acls in cluster:")
      acls foreach println
    }

    "must be able to get ACLs of current user" in {

      val user = config.userConfigMap(userConfigName)

      val currentUserACEntryFilter = new AccessControlEntryFilter(user.principal, "*", AclOperation.ANY, AclPermissionType.ANY)
      val currentUserAclBindingFilter: AclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, currentUserACEntryFilter)

      val getAcls = client.describeAcls(currentUserAclBindingFilter).values()
      val sf: Future[util.Collection[AclBinding]] = toScalaFuture(getAcls)
      val acls: Iterable[AclBinding] = Await.result(sf, 10.seconds).asScala
      println(s"acls for ${user.accountId}:")
      acls foreach println
    }

    "must be able to set ACL for SA user" in {

      val saSaslConfig: SaslConfig = config.userConfigMap(saConfigName)

      // The only valid name for the CLUSTER resource is kafka-cluster
      val resourcePattern = new ResourcePattern(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL)
      val accessControlEntry = new AccessControlEntry(saSaslConfig.principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)

      // The only valid name for the CLUSTER resource is kafka-cluster
      val resourcePatternFilter = new ResourcePatternFilter(ResourceType.CLUSTER, "kafka-cluster", PatternType.LITERAL)
      val accessControlEntryFilter = new AccessControlEntryFilter(saSaslConfig.principal, "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)

      val aclBindingFilter = new AclBindingFilter(resourcePatternFilter, accessControlEntryFilter)
      val deleteAcls = client.deleteAcls(List(aclBindingFilter).asJava)
      val aclsDeleted: List[AclBinding] = Await.result(toScalaFuture(deleteAcls.all()), 10.seconds).asScala.toList
      //println("acls deleted: ")
      //aclsDeleted foreach println

      val aclBindingForSA = new AclBinding(resourcePattern, accessControlEntry)

      val createAcls: CreateAclsResult = client.createAcls(List(aclBindingForSA).asJava)
      val aclsCreated: Void = Await.result(toScalaFuture(createAcls.all()), 10.seconds)

      val currentUserACEntryFilter = new AccessControlEntryFilter(saSaslConfig.principal, "*", AclOperation.ANY, AclPermissionType.ANY)
      val currentUserAclBindingFilter: AclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, currentUserACEntryFilter)

      val getAcls = client.describeAcls(currentUserAclBindingFilter).values()
      val sf: Future[util.Collection[AclBinding]] = toScalaFuture(getAcls)
      val foundAcls: Iterable[AclBinding] = Await.result(sf, 10.seconds).asScala
      //println(s"acls for ${saSaslConfig.accountId}:")
      //foundAcls foreach println
      foundAcls.size mustBe 1
      foundAcls.head must be(aclBindingForSA)

    }

  }

}
