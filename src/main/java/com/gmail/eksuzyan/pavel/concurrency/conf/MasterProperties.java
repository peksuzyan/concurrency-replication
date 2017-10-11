package com.gmail.eksuzyan.pavel.concurrency.conf;

import java.util.Properties;

/**
 * @author Pavel Eksuzian.
 *         Created: 11.10.2017.
 */
@SuppressWarnings("WeakerAccess")
public final class MasterProperties extends Properties {

    public static final String DISPATCHER_THREAD_POOL_NAME = "master.thread.pool.dispatcher.name";
    public static final String REPEATER_THREAD_POOL_NAME = "master.thread.pool.repeater.name";
    public static final String PREPARATOR_THREAD_POOL_NAME = "master.thread.pool.preparator.name";
    public static final String LISTENER_THREAD_POOL_NAME = "master.thread.pool.listener.name";
    public static final String SCHEDULER_THREAD_POOL_NAME = "master.thread.pool.scheduler.name";

    private static final String DISPATCHER_THREAD_POOL_SIZE = "master.thread.pool.dispatcher.size";
    private static final String REPEATER_THREAD_POOL_SIZE = "master.thread.pool.repeater.size";
    private static final String PREPARATOR_THREAD_POOL_SIZE = "master.thread.pool.preparator.size";
    private static final String LISTENER_THREAD_POOL_SIZE = "master.thread.pool.listener.size";
    private static final String SCHEDULER_THREAD_POOL_SIZE = "master.thread.pool.scheduler.size";

    private final static MasterProperties properties = new MasterProperties();

    public static synchronized MasterProperties getProperties() {
        return properties;
    }

    public static synchronized void reset() {
        properties.clear();
    }

    private MasterProperties() {
    }

    private static int getIntOrDefault(String key, int defaultValue) {
        String value = getProperties().getProperty(key);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    public static void setDispatcherThreadPoolSize(int nThreads) {
        getProperties().setProperty(DISPATCHER_THREAD_POOL_SIZE, String.valueOf(nThreads));
    }

    public static void setRepeaterThreadPoolSize(int nThreads) {
        getProperties().setProperty(REPEATER_THREAD_POOL_SIZE, String.valueOf(nThreads));
    }

    public static void setPreparatorThreadPoolSize(int nThreads) {
        getProperties().setProperty(PREPARATOR_THREAD_POOL_SIZE, String.valueOf(nThreads));
    }

    public static void setListenerThreadPoolSize(int nThreads) {
        getProperties().setProperty(LISTENER_THREAD_POOL_SIZE, String.valueOf(nThreads));
    }

    public static void setSchedulerThreadPoolSize(int nThreads) {
        getProperties().setProperty(SCHEDULER_THREAD_POOL_SIZE, String.valueOf(nThreads));
    }

    public static int getDispatcherThreadPoolSize() {
        return getIntOrDefault(DISPATCHER_THREAD_POOL_SIZE, 16);
    }

    public static int getRepeaterThreadPoolSize() {
        return getIntOrDefault(REPEATER_THREAD_POOL_SIZE, 16);
    }

    public static int getPreparatorThreadPoolSize() {
        return getIntOrDefault(PREPARATOR_THREAD_POOL_SIZE, 16);
    }

    public static int getListenerThreadPoolSize() {
        return getIntOrDefault(LISTENER_THREAD_POOL_SIZE, 8);
    }

    public static int getSchedulerThreadPoolSize() {
        return getIntOrDefault(SCHEDULER_THREAD_POOL_SIZE, 8);
    }

    public static String getDispatcherThreadPoolName() {
        return getProperties().getProperty(DISPATCHER_THREAD_POOL_NAME, "dispatcher");
    }

    public static String getRepeaterThreadPoolName() {
        return getProperties().getProperty(REPEATER_THREAD_POOL_NAME, "repeater");
    }

    public static String getPreparatorThreadPoolName() {
        return getProperties().getProperty(PREPARATOR_THREAD_POOL_NAME, "preparator");
    }

    public static String getListenerThreadPoolName() {
        return getProperties().getProperty(LISTENER_THREAD_POOL_NAME, "listener");
    }

    public static String getSchedulerThreadPoolName() {
        return getProperties().getProperty(SCHEDULER_THREAD_POOL_NAME, "scheduler");
    }
}
