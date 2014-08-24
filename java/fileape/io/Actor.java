package fileape.io;

import clojure.lang.IFn;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 */
public class Actor implements Runnable{

    private final BlockingQueue<IFn> queue;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    Object state;

    public Actor(Object state, int queueSize){
        this.state = state;
        queue = new ArrayBlockingQueue<IFn>(queueSize);
    }

    public void send(IFn fn) throws InterruptedException{
        queue.put(fn);
    }

    public void shutdown(){
        shutdown.set(true);
    }

    public void run(){
        IFn f = null;

        while(!Thread.interrupted() && !shutdown.get()){
            try {
                f = queue.take();
                if(f != null)
                    f.invoke(state);
            }catch (InterruptedException exc){
                Thread.currentThread().interrupt();
                break;
            }catch(Throwable t){
                t.printStackTrace();
            }
        }

        shutdown.set(true);
        while((f = queue.poll()) != null){
            try{
                f.invoke(state);
            }catch(Throwable t){
                if(t instanceof InterruptedException){
                    Thread.currentThread().interrupt();
                    return;
                }
                t.printStackTrace();
            }
        }

    }
}
