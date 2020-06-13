package org.dist.ourkafka

import org.dist.common.ZookeeperTestHarness
import org.dist.simplekafka.util.ZkUtils.Broker

class OurZookeperClientTest extends ZookeeperTestHarness{
  test("should register brokers with zookeeper") {
    val ourZKClient = new OurZookeperClient(zkClient);
    ourZKClient.registerBroker(Broker(10, "120.10.10.10", 8000))
    ourZKClient.registerBroker(Broker(11, "120.10.10.11", 8080))

    assert(2 == ourZKClient.getBrokersCount())
  }
}
