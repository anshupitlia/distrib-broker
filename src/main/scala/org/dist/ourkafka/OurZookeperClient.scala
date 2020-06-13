package org.dist.ourkafka

import java.util.Random

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.{ZkNoNodeException, ZkNodeExistsException}
import org.dist.simplekafka.{ControllerExistsException, PartitionReplicas}
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.collection.mutable
import scala.jdk.CollectionConverters._

class OurZookeperClient(zkClient:ZkClient){
  def readData(path: String): String = {
     zkClient.readData(path)
  }


  def getAllBrokerIds() = {
    zkClient.getChildren(s"/brokers/ids").asScala.map(_.toInt).toSet
  }
  def getAllBrokers(): Set[Broker] = {
    zkClient.getChildren(s"/brokers/ids").asScala.map(brokerId => {
      val data: String = zkClient.readData(s"/brokers/ids/$brokerId")
      JsonSerDes.deserialize(data.getBytes, classOf[Broker])
    }).toSet
  }

  def createTopic(topicName:String, noOfPartitions:Int, replicationFactor:Int) = {
    val brokerIds = getAllBrokerIds()
    //get list of brokers
    //assign replicas to partition
    val partitionReplicas: Set[PartitionReplicas] = assignReplicasToBrokers(brokerIds.toList, noOfPartitions, replicationFactor)
    // register topic with partition assignments to zookeeper
    val topicsPath =  s"/brokers/topics/$topicName"
    val topicsData = JsonSerDes.serialize(partitionReplicas)
    createPersistentPath(topicsPath, topicsData)
  }

  def tryCreatingControllerPath(controllerPath: String, controllerId: String): Unit = {
    try {
      createEphemeralPath(controllerPath, controllerId)
    } catch {
      case e: ZkNodeExistsException => {
        val existingControllerId: String = zkClient.readData(controllerPath)
        throw new ControllerExistsException(existingControllerId)
      }
    }
  }

  def createEphemeralPath(path: String, data: String): Unit = {
    try {
      zkClient.createEphemeral(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(path)
        zkClient.createEphemeral(path, data)
      }
    }
  }

  def createPersistentPath(path: String, data: String): Unit = {
    try {
      zkClient.createPersistent(path, data)
    } catch {
      case e: ZkNoNodeException => {
        createParentPath(path)
        zkClient.createPersistent(path, data)
      }
    }
  }

  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning 10 partitions 3 replicas to 5 brokers
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   */
  def assignReplicasToBrokers(brokerList: List[Int], nPartitions: Int, replicationFactor: Int)  = {
    val rand = new Random
    val ret = new mutable.HashMap[Int, List[Int]]()
    val startIndex = rand.nextInt(brokerList.size)
    var currentPartitionId = 0

    var nextReplicaShift = rand.nextInt(brokerList.size)
    for (partitionId <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(getWrappedIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    val partitionIds = ret.toMap.keySet
    partitionIds.map(id => PartitionReplicas(id, ret(id)))
  }

  private def getWrappedIndex(firstReplicaIndex: Int, secondReplicaShift: Int, replicaIndex: Int, nBrokers: Int): Int = {
    val shift = 1 + (secondReplicaShift + replicaIndex) % (nBrokers - 1)
    (firstReplicaIndex + shift) % nBrokers
  }

  def getBrokerInfo(brokerId: Int): Broker = {
    val data: String = zkClient.readData(s"/brokers/ids/$brokerId")
    JsonSerDes.deserialize(data.getBytes, classOf[Broker])
  }

  def subscribeBrokerChangeListener(listener: OurBrokerChangeListener) = {
    val result = zkClient.subscribeChildChanges("/brokers/ids", listener)
    Option(result).map(_.asScala.toList)
  }

  private def createParentPath(path: String): Unit = {
    val parentDir = path.substring(0, path.lastIndexOf('/'))
    if (parentDir.length != 0)
      zkClient.createPersistent(parentDir, true)
  }

  def registerBroker(broker: ZkUtils.Broker) = {
    val brokerId = broker.id
    createParentPath("/brokers/ids/")
    zkClient.createEphemeral(s"/brokers/ids/$brokerId", JsonSerDes.serialize(broker))
  }

  def getBrokersCount() = {
    zkClient.getChildren("/brokers/ids").size();
  }
}
