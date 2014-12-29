package fileape.io;

import clojure.lang.IFn;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

/**
 */
public class Actor implements Runnable {

    private static final Logger LOG = Logger.getLogger(Actor.class);

    private final BlockingQueue<IFn> queue;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);


    Object state;

    public Actor(Object state, int queueSize) {
        this.state = state;
        queue = new ArrayBlockingQueue<IFn>(queueSize);
    }

    public void send(IFn fn) throws InterruptedException {
        if (shutdown.get())
            throw new IllegalStateException("actor is shutdown");

        //we shouldn't be blocking longer than this, and anthying longs than 5 seconds should result in error
        //messages for debugging purposes
        if (!queue.offer(fn, 5, TimeUnit.SECONDS)) {
            LOG.error("The Actor queue (mail box) is full, blocking till a slot becomes available: " + this);
            queue.put(fn);
            LOG.info("Unblocked: " + this);
        }

    }

    public void shutdown() {
        shutdown.set(true);
    }

    public void run() {
        IFn f = null;

        while (!Thread.interrupted() && !shutdown.get()) {
            try {
                f = queue.poll(5, TimeUnit.SECONDS);
                if (f != null)
                    f.invoke(state);
                else if (LOG.isDebugEnabled())
                    LOG.debug("Actor has an empty message queue or read a null message");
            } catch (InterruptedException exc) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                t.printStackTrace();
                LOG.error(t);
            }
        }

        //a grace period
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {

        }

        shutdown.set(true);
        try {
            while ((f = queue.poll(1, TimeUnit.SECONDS)) != null) {
                try {
                    f.invoke(state);
                } catch (Throwable t) {
                    if (t instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    t.printStackTrace();
                    LOG.error(t);
                }
            }
        } catch (InterruptedException ie) {
            LOG.error("Shutdown was interrupted");
            return;
        }

    }
}
