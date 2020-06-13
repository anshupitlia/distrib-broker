package org.dist.ourkafka

import org.dist.common.{TestUtils, ZookeeperTestHarness}
import org.dist.simplekafka.util.ZkUtils.Broker

class OurZookeperClientTest extends ZookeeperTestHarness{
  test("should register brokers with zookeeper") {
    val ourZKClient = new OurZookeperClient(zkClient);
    ourZKClient.registerBroker(Broker(10, "120.10.10.10", 8000))
    ourZKClient.registerBroker(Broker(11, "120.10.10.11", 8080))

    assert(2 == ourZKClient.getBrokersCount())
  }

  test("should get notified when broker is registered") {
    val ourZKClient = new OurZookeperClient(zkClient);
    val listener = new OurBrokerChangeListener(ourZKClient)
    ourZKClient.subscribeBrokerChangeListener(listener);

    ourZKClient.registerBroker(Broker(10, "120.10.10.10", 8000))

    ourZKClient.registerBroker(Broker(11, "120.10.10.11", 8080))

    ourZKClient.registerBroker(Broker(12, "120.10.10.12", 8090))

    TestUtils.waitUntilTrue(() => {
      listener.liveBrokers.size == 3
    }, "Waiting for all brokers to get added", 1000)
  }
}
