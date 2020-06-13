package org.dist.ourkafka

import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.dist.simplekafka.common.Logging
import org.dist.simplekafka.util.ZkUtils.Broker

class MYZKController(ourZKClient: OurZookeperClient, brokerId: Int) extends Logging{
  var liveBrokers: Set[Broker] = Set()
  var currentLeader = "-1"

  def elect() = {
    info("elect is called for %s".format(brokerId))
    val leaderId = s"${brokerId}"
    try {

      ourZKClient.createEphemeralPath("/controller", s"${leaderId}")
      this.currentLeader = leaderId;
      info("currentLeader %s".format(currentLeader))
    }catch {
      case e: ZkNodeExistsException => {
        val existingControllerId: String = ourZKClient.readData("/controller")
        this.currentLeader = existingControllerId
        info("currentLeader %s".format(currentLeader))
      }
    }
  }
}
