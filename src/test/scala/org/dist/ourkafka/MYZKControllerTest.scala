package org.dist.ourkafka

import org.dist.common.ZookeeperTestHarness
import org.dist.simplekafka.util.ZkUtils.Broker

class MYZKControllerTest extends ZookeeperTestHarness {
  test("should send LeaderAndFollower requests to all leader and follower brokers for given topicandpartition") {
    val ourZKClient = new OurZookeperClient(zkClient);
    ourZKClient.registerBroker(Broker(10, "120.10.10.10", 8000))
    ourZKClient.registerBroker(Broker(11, "120.10.10.11", 8080))
    ourZKClient.registerBroker(Broker(12, "120.10.10.12", 8090))

    ourZKClient.createTopic("topic1", 2, 1)
    val controller1 = new MYZKController(ourZKClient, 10)
    val controller2 = new MYZKController(ourZKClient, 11)
    val controller3 = new MYZKController(ourZKClient, 12)
    controller1.elect()
    controller2.elect()
    controller3.elect()
  }
}
