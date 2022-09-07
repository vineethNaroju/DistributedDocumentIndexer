package zk;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.SLF4JServiceProvider;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class TestA implements Runnable {


    final ZkConnection conn = new ZkConnection();
    final ZooKeeper zk = conn.get();

    final CountDownLatch latch;

    public TestA(CountDownLatch latch) throws IOException {
        this.latch = latch;
    }


    @Override
    public void run() {
        try {

            String str = "Hello World !";

            Stat stat = new Stat();


            if(zk.exists("/peow", null) == null) {
                zk.create("/peow", str.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, stat);
            }

            zk.addWatch("/peow", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    print(event);
                }
            }, AddWatchMode.PERSISTENT);


            print(stat);

            Thread.sleep(50000);

            latch.countDown();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }
}
