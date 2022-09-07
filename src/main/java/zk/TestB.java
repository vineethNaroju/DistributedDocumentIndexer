package zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class TestB implements Runnable {

    final ZkConnection conn = new ZkConnection();
    final ZooKeeper zk = conn.get();

    final CountDownLatch latch;

    public TestB(CountDownLatch latch) throws IOException {
        this.latch = latch;
    }


    @Override
    public void run() {
        try {

            Thread.sleep(5000);

            String str = "Hello World !";

            Stat stat = new Stat();

            for(int i=0; i<5; i++) {
                zk.create("/peow/cat" + i, str.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
                Thread.sleep(5000);
            }

            latch.countDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
