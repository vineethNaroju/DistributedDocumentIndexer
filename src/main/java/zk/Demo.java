package zk;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

public class Demo {

    CountDownLatch latch = new CountDownLatch(2);

    void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public void run() throws IOException {
        Thread a = new Thread(new TestA(latch));
        Thread b = new Thread(new TestB(latch));

        a.start();
        b.start();

        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
        Demo demo = new Demo();
        demo.run();
    }
}
