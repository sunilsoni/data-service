package com.example.data.util;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class NotifyingBlockingExecutor extends ThreadPoolExecutor {
    private AtomicInteger tasksInProcess = new AtomicInteger();
    private Synchronizer synchronizer = new Synchronizer();

    public NotifyingBlockingExecutor(int poolSize, int queueSize, long keepAliveTime, TimeUnit keepAliveTimeUnit, long maxBlockingTime, TimeUnit maxBlockingTimeUnit, Callable<Boolean> blockingTimeCallback) {
        super(poolSize,
                poolSize,
                keepAliveTime,
                keepAliveTimeUnit,
                new ArrayBlockingQueue<>(Math.max(poolSize, queueSize)),
                new BlockThenRunPolicy(maxBlockingTime, maxBlockingTimeUnit, blockingTimeCallback));
        super.allowCoreThreadTimeOut(true);
    }

    public NotifyingBlockingExecutor(int poolSize, int queueSize, long keepAliveTime, TimeUnit unit) {

        super(poolSize,
                poolSize,
                keepAliveTime,
                unit,
                new ArrayBlockingQueue<>(Math.max(poolSize, queueSize)),
                new BlockThenRunPolicy());

        super.allowCoreThreadTimeOut(true);
    }

    @Override
    public void execute(Runnable task) {
        tasksInProcess.incrementAndGet();
        try {
            super.execute(task);
        } catch (RuntimeException | Error e) {
            tasksInProcess.decrementAndGet();
            throw e;
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        synchronized (this) {
            tasksInProcess.decrementAndGet();
            if (tasksInProcess.intValue() == 0) {
                synchronizer.signalAll();
            }
        }
    }

    @Override
    public void setCorePoolSize(int corePoolSize) {
        super.setCorePoolSize(corePoolSize);
        super.setMaximumPoolSize(corePoolSize);
    }

    @Override
    public void setMaximumPoolSize(int maximumPoolSize) {
        throw new UnsupportedOperationException("setMaximumPoolSize is not supported.");
    }

    @Override
    public void setRejectedExecutionHandler(RejectedExecutionHandler handler) {
        throw new UnsupportedOperationException("setRejectedExecutionHandler is not allowed on this class.");
    }

    public void await() throws InterruptedException {
        synchronizer.await();
    }

    public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return synchronizer.await(timeout, timeUnit);
    }

    private static class BlockThenRunPolicy implements RejectedExecutionHandler {

        private long maxBlockingTime;
        private TimeUnit maxBlockingTimeUnit;
        private Callable<Boolean> blockingTimeCallback;

        public BlockThenRunPolicy(long maxBlockingTime, TimeUnit maxBlockingTimeUnit, Callable<Boolean> blockingTimeCallback) {
            this.maxBlockingTime = maxBlockingTime;
            this.maxBlockingTimeUnit = maxBlockingTimeUnit;
            this.blockingTimeCallback = blockingTimeCallback;
        }

        public BlockThenRunPolicy() {
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {

            BlockingQueue<Runnable> workQueue = executor.getQueue();
            boolean taskSent = false;

            while (!taskSent) {

                if (executor.isShutdown()) {
                    throw new RejectedExecutionException(
                            "ThreadPoolExecutor has shutdown while attempting to offer a new task.");
                }

                try {
                    if (blockingTimeCallback != null) {
                        if (workQueue.offer(task, maxBlockingTime, maxBlockingTimeUnit)) {
                            taskSent = true;
                        } else {
                            Boolean result;
                            try {
                                result = blockingTimeCallback.call();
                            } catch (Exception e) {
                                throw new RejectedExecutionException(e);
                            }
                            if (!result) {
                                throw new RejectedExecutionException("User decided to stop waiting for task insertion");
                            } else {
                            }
                        }

                    } else {
                        workQueue.put(task);
                        taskSent = true;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private static class Synchronizer {

        private final Lock lock = new ReentrantLock();
        private final Condition done = lock.newCondition();
        private boolean isDone = false;

        private void signalAll() {

            lock.lock();
            try {
                isDone = true;
                done.signalAll();
            } finally {
                lock.unlock();
            }
        }

        public void await() throws InterruptedException {

            lock.lock();
            try {
                while (!isDone) {
                    done.await();
                }
            } finally {
                isDone = false;
                lock.unlock();
            }
        }

        public boolean await(long timeout, TimeUnit timeUnit) throws InterruptedException {

            boolean await_result;
            lock.lock();
            boolean localIsDone;
            try {
                await_result = done.await(timeout, timeUnit);
            } finally {
                localIsDone = isDone;
                isDone = false;
                lock.unlock();
            }
            return await_result && localIsDone;
        }
    }

}