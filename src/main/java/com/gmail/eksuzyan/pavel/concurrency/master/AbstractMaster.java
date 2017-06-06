package com.gmail.eksuzyan.pavel.concurrency.master;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Presents a base implementation of master interface.
 *
 * @author Pavel Eksuzian.
 *         Created: 04.04.2017.
 */
public abstract class AbstractMaster implements Master {

    /**
     * Ordinary logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(AbstractMaster.class);

    /**
     * Counter serves to generate unique master ID.
     */
    private static long masterCounter = 0L;

    /**
     * Default master name.
     */
    private static final String DEFAULT_NAME = "Master";

    /**
     * Indicates either master closed or not.
     */
    private volatile boolean closed = false;

    /**
     * Master name.
     */
    private String name;

    private static final int DISPATCHER_THREAD_POOL_SIZE = 4;
    private static final int PREPARATOR_THREAD_POOL_SIZE = 4;
    private static final int RESTORER_THREAD_POOL_SIZE = 4;

    private final Map<String, Slave> slaves;

    // Must be concurrent!!!
    private final Map<String, Project> projects = new ConcurrentHashMap<>();

    private final Map<Slave, ConcurrentMap<String, Long>> newestVersionTracker = new HashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final ExecutorService dispatcher =
            Executors.newFixedThreadPool(
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("dispatcher"));

    private final ScheduledExecutorService repeater =
            Executors.newScheduledThreadPool(
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("repeater"));

    private final ExecutorService preparator =
            Executors.newFixedThreadPool(
                    PREPARATOR_THREAD_POOL_SIZE,
                    new NamedThreadFactory("preparator"));

    private final BlockingQueue<Runnable> failedRequestsQueue =
            new LinkedBlockingQueue<>();

    private final ExecutorService restorer =
            new ThreadPoolExecutor(
                    RESTORER_THREAD_POOL_SIZE,
                    RESTORER_THREAD_POOL_SIZE,
                    10L,
                    TimeUnit.SECONDS,
                    failedRequestsQueue,
                    new NamedThreadFactory("restorer"));

    /**
     * Creates thread factory instance which assigns a thread name according to the given name.
     */
    private class NamedThreadFactory implements ThreadFactory {

        private static final String POOL_NAME_MASK = "pool-[\\d]-thread";

        private final String poolName;

        private final ThreadFactory defaultThreadFactory =
                Executors.defaultThreadFactory();

        NamedThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = defaultThreadFactory.newThread(r);
            t.setName(t.getName().replaceFirst(POOL_NAME_MASK, poolName));
            return t;
        }
    }

    /**
     * Single constructor.
     *
     * @param name   master name
     * @param slaves slaves
     */
    protected AbstractMaster(String name, Slave... slaves) {
        long id = ++masterCounter;
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", DEFAULT_NAME, id) : name;

        try {
            this.slaves =
                    (slaves == null || slaves.length == 0)
                            ? Collections.emptyMap()
                            : Collections.unmodifiableMap(
                            Arrays.stream(slaves)
                                    .peek(Objects::requireNonNull)
                                    .collect(Collectors.toMap(Slave::getName, slave -> slave)));
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Slaves have the same names!", e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Slave mustn't be null!", e);
        }

        LOG.info("{} initialized.", getName());
    }

    /**
     * Posts project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    protected void postProjectDefault(String projectId, String data) {
        long startTime = System.currentTimeMillis();

        if (closed)
            throw new IllegalStateException(getName() + " closed already.");

        if (projectId == null)
            throw new IllegalArgumentException("Project ID mustn't be null.");

        final Runnable task = new PreparationTask(projectId, data);

        if (!preparator.isShutdown())
            preparator.execute(task);
        else
            LOG.debug("{} can't be processed because preparator is shutdown.", new Project(projectId, data));

        LOG.trace("postProjectDefault: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProject(String projectId, String data) {
        long startTime = System.currentTimeMillis();

        Project newProject = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : projects.get(projectId).setDataAndIncVersion(data);

        projects.put(projectId, newProject);

        prepareProjectToSlaves(newProject);

        LOG.trace("prepareProject: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProjectToSlaves(final Project project) {
        long startTime = System.currentTimeMillis();

        slaves.values().forEach(slave ->
                deliverToDispatcher(new Request(slave, project)));

//        for (Slave slave : slaves.values())
//            deliverToDispatcher(new Request(slave, project));

        LOG.trace("prepareProjectToSlaves: {}ms", System.currentTimeMillis() - startTime);
    }

    private void deliverToDispatcher(Request request) {
        long startTime = System.currentTimeMillis();

        final Runnable sendingTask = new SendingTask(request);

        if (!dispatcher.isShutdown())
            dispatcher.execute(sendingTask);
        else
            LOG.debug("{} can't be delivered because dispatcher is shutdown.", request);

        LOG.trace("deliverToDispatcher: {}ms", System.currentTimeMillis() - startTime);
    }

    private void deliverToRepeater(Request request) {
        long startTime = System.currentTimeMillis();

        final Runnable sendingTask = new SendingTask(request);

        long delay = request.repeatDate - System.currentTimeMillis();

        if (!repeater.isShutdown())
            repeater.schedule(sendingTask, delay, TimeUnit.MILLISECONDS);
        else
            LOG.debug("{} can't be delivered again because repeater is shutdown.", request);

        LOG.trace("deliverToRepeater: {}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * Returns stored projects.
     *
     * @return projects
     */
    protected Collection<Project> getProjectsDefault() {
        return new ArrayList<>(projects.values());
    }

    /**
     * Returns failed requests.
     *
     * @return requests
     */
    protected Collection<Request> getFailedDefault() {
        return failedRequestsQueue.stream()
                .map(t -> ((RestoringTask) t).request)
                .collect(Collectors.toList());
    }

    /**
     * Stops workers and thread pools.
     */
    protected void shutdownDefault() {

        if (closed)
            throw new IllegalStateException(getName() + " closed already.");

        closed = true;

        preparator.shutdownNow();
        dispatcher.shutdownNow();
        restorer.shutdownNow();
        repeater.shutdownNow();

        try {
            while (!dispatcher.awaitTermination(15, TimeUnit.SECONDS)) {
                LOG.info("{} Dispatcher tasks haven't been yet completed.", getName());
            }
        } catch (InterruptedException e) {
            LOG.error("Main thread has been interrupted unexpectedly!", e);
        }

        LOG.info("{} closed.", getName());
    }

    /**
     * Sets master name.
     *
     * @param name master name
     */
    @Override
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns master name.
     *
     * @return master name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Returns slaves which are related to this master instance.
     *
     * @return a set of slaves
     */
    @Override
    public Collection<Slave> getSlaves() {
        return new ArrayList<>(slaves.values());
    }

    /**
     * Prepares a project to be spread among slaves.
     */
    private class PreparationTask implements Runnable {
        private final String projectId;
        private final String data;

        PreparationTask(String projectId, String data) {
            this.projectId = projectId;
            this.data = data;
        }

        @Override
        public void run() {
            AbstractMaster.this.prepareProject(projectId, data);
        }
    }

    /**
     * Restores a request when it has failed.
     */
    private class RestoringTask implements Runnable {
        final Request request;

        RestoringTask(Request request) {
            this.request = request;
        }

        private ConcurrentMap<String, Long> getOrCreateSlaveTracker(Slave slave) {

            lock.readLock().lock();
            try {
                if (!newestVersionTracker.containsKey(slave)) {
                    lock.readLock().unlock();
                    lock.writeLock().lock();
                    try {
                        if (!newestVersionTracker.containsKey(slave)) {
                            ConcurrentMap<String, Long> slaveTracker = new ConcurrentHashMap<>();
                            newestVersionTracker.put(slave, slaveTracker);
                            return slaveTracker;
                        } else {
                            return newestVersionTracker.get(slave);
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                } else {
                    return newestVersionTracker.get(slave);
                }
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void run() {

            // A request shouldn't be sent if a request version more than one from main map.
            // Restorer can be renamed on registrator :)

            ConcurrentMap<String, Long> slaveTracker = getOrCreateSlaveTracker(request.slave);

            Long lastVersion = slaveTracker.putIfAbsent(request.project.id, request.project.version);

            boolean isActual = false;
            if (lastVersion != null && lastVersion <= request.project.version)
                isActual = slaveTracker.replace(request.project.id, lastVersion, request.project.version);

            if (isActual)
                AbstractMaster.this.deliverToRepeater(request);
        }
    }

    /**
     * Posts a request into a slave.
     */
    private class SendingTask implements Runnable {
        private final Request request;

        SendingTask(Request request) {
            this.request = request;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            try {
                request.slave.postProject(
                        request.project.id,
                        request.project.version,
                        request.project.data);

                LOG.debug("[+] => {} => {}.", request.slave.getName(), request);
            } catch (Throwable e) {
                LOG.debug("[-] => {} => {}.", request.slave.getName(), request);

                RestoringTask restoringTask =
                        new RestoringTask(request.setCodeAndIncAttempt(1));

                if (!restorer.isShutdown())
                    restorer.execute(restoringTask);
                else
                    LOG.debug("{} can't be restored because restorer is shutdown.", request);
            }

            LOG.trace("sendingTask: {}ms", System.currentTimeMillis() - startTime);
        }
    }
}
