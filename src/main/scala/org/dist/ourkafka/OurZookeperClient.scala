package org.dist.ourkafka

import org.I0Itec.zkclient.ZkClient
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils
import org.dist.simplekafka.util.ZkUtils.Broker

import scala.jdk.CollectionConverters._

class OurZookeperClient(zkClient:ZkClient){

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
