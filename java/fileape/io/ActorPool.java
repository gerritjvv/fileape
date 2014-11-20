package fileape.io;

import clojure.lang.IFn;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ActorPool creates and sends functions to Actor instances based on key values.<br/>
 * Actor(s) can be dynamically removed after a function call by using commands.
 */
public class ActorPool implements Runnable{

    private static final String EMPTY_KEY = "NONE";

    private final ExecutorService service;
    private final ExecutorService masterService = Executors.newSingleThreadExecutor();

    private final BlockingQueue<Object> masterQueue = new ArrayBlockingQueue<Object>(100);
    private final Map<String, Actor> actorMap = new ConcurrentHashMap<String, Actor>(100);

    private AtomicBoolean shutdown = new AtomicBoolean(false);

    public enum Command {DELETE, CHECK_ROLL}

    private final int actorBufferSize;
    private final int masterBufferSize;

    /**
     * @param masterBufferSize The master actor that sends to other actors' size
     * @param actorButterSize  The size for each actor's internal queue
     */
    private ActorPool(int masterBufferSize, int actorButterSize){
        this.masterBufferSize = masterBufferSize;
        this.actorBufferSize = actorButterSize;
        //cached thread pool with an upper bound
        service =  Executors.newCachedThreadPool();
    }

    private ActorPool(){
        this(100, 1000);
    }

    public static ActorPool newInstance(){
        return newInstance(100, 1000);
    }

    public static ActorPool newInstance(int masterBufferSize, int actorBufferSize){
        ActorPool pool = new ActorPool(masterBufferSize, actorBufferSize);
        pool.masterService.submit(pool);
        return pool;
    }

    public void sendCommandAll(Command cmd, IFn createStateFn, final IFn fn){
        try { // String key, IFn val, IFn createStateFn)
            masterQueue.put(new SendAllQueuedItem(new QueuedItem(cmd, EMPTY_KEY, fn, createStateFn)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
    }
    public void sendCommand(Command cmd, IFn createStateFn, String key, IFn fn) throws InterruptedException {
        if(shutdown.get())
            throw new RuntimeException("ActorPool is shutdown");

        masterQueue.put(new QueuedItem(cmd, key, fn, createStateFn));
    }

    public final void send(final IFn createStateFn, final String key, final IFn fn) throws InterruptedException {
        if(shutdown.get())
            throw new RuntimeException("ActorPool is shutdown");

        masterQueue.put(new QueuedItem(key, fn, createStateFn));
    }

    public void shutdown(long timeout) throws InterruptedException {
        shutdown.set(true);
        masterService.shutdown();
        boolean didShutdown = masterService.awaitTermination(timeout, TimeUnit.MILLISECONDS);
        if(!didShutdown)
            masterService.shutdownNow();
    }

    public void run(){

        while(!Thread.interrupted() && !shutdown.get()){
            try{
                processQueuedItem(masterQueue.take());
            }catch(InterruptedException t){
                Thread.currentThread().interrupt();
                break;
            }catch(Throwable t){
                t.printStackTrace();
            }
        }

        Object obj = null;

        while((obj = masterQueue.poll()) != null){
            try{
                processQueuedItem(obj);
            }catch(Throwable t){
                t.printStackTrace();
            }
        }

        service.shutdown();
        try {
            service.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
           //ignore
        }
        service.shutdownNow();
    }

    private final void processQueuedItem(Object obj) throws InterruptedException {
        if(obj instanceof QueuedItem)
            processItem((QueuedItem)obj);
        else if(obj instanceof SendAllQueuedItem){
            QueuedItem item = ((SendAllQueuedItem)obj).item;
            for(Map.Entry<String, Actor> entry : actorMap.entrySet())
                processItem(new QueuedItem(item.cmd, entry.getKey(), item.val, item.createStateFn));
        }
    }

    private final void processItem(QueuedItem item) throws InterruptedException {
        if(item != null){
            Actor actor = actorMap.get(item.key);
            if(actor == null){
                //nothing to roll, exit
                if(item.cmd != null && item.cmd == Command.CHECK_ROLL || item.cmd == Command.DELETE)
                    return;

                actor = new Actor(item.createStateFn.invoke(item.key), 1000);
                actorMap.put(item.key, actor);
                service.submit(actor);
            }

            actor.send(item.val);

            if(item.cmd != null && item.cmd == Command.DELETE){
                Actor removedActor = actorMap.remove(item.key);
                if(removedActor != null)
                    removedActor.shutdown();
            }
        }
    }

    public static class SendAllQueuedItem{
        final QueuedItem item;
        public SendAllQueuedItem(QueuedItem item){
            this.item = item;
        }
    }

    public static class QueuedItem{
        final String key;
        final IFn val;
        final Command cmd;
        final IFn createStateFn;

        public QueuedItem(Command cmd, String key, IFn val, IFn createStateFn){
            if(createStateFn == null || key == null || val == null)
                throw new NullPointerException("createStateFN[" + createStateFn + "], val[" + val + "], and key[" + key + "] cannot be null");

            this.cmd = cmd;
            this.key = key;
            this.val = val;
            this.createStateFn = createStateFn;
        }
        public QueuedItem(String key, IFn val, IFn createStateFn){
            this(null, key, val, createStateFn);
        }
    }
}
