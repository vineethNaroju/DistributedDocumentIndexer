package zk;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZkConnection {

    private final ZooKeeper zooKeeper;
    private final CountDownLatch latch = new CountDownLatch(1);


    ZkConnection() throws IOException {
        zooKeeper = new ZooKeeper(Config.server, 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    latch.countDown();
                }
            }
        });
    }

    public ZooKeeper get() {
        return zooKeeper;
    }

    public void close() throws InterruptedException {
        zooKeeper.close();

    }
}
