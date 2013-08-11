package kissmydisc.repricer;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RepricerMainThreadPool {

    private static RepricerMainThreadPool threadPool = new RepricerMainThreadPool();

    private ThreadPoolExecutor executor;

    private RepricerMainThreadPool() {
        executor = new ThreadPoolExecutor(10, 10, 1L, TimeUnit.HOURS, new ArrayBlockingQueue<Runnable>(100));
    }

    public static RepricerMainThreadPool getInstance() {
        return threadPool;
    }

    public void submit(Runnable r) {
        executor.submit(r);
    }
}
