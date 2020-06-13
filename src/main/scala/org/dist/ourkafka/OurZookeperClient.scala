package org.dist.ourkafka

import org.I0Itec.zkclient.ZkClient
import org.dist.simplekafka.common.JsonSerDes
import org.dist.simplekafka.util.ZkUtils

class OurZookeperClient(zkClient:ZkClient){
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
