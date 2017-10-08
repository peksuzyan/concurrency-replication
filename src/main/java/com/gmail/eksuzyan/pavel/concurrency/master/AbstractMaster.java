package com.gmail.eksuzyan.pavel.concurrency.master;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private static AtomicLong masterCounter = new AtomicLong();

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

    private final Map<String, Slave> slaves;

    private final ConcurrentMap<String, Project> projects = new ConcurrentHashMap<>();

    private final Set<Request> failed = ConcurrentHashMap.newKeySet();

    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    private static final int MAX_ATTEMPTS = 3;

    private static final int DISPATCHER_THREAD_POOL_SIZE = 16;
    private static final int REPEATER_THREAD_POOL_SIZE = 16;
    private static final int PREPARATOR_THREAD_POOL_SIZE = 16;
    private static final int LISTENER_THREAD_POOL_SIZE = 8;
    private static final int SCHEDULER_THREAD_POOL_SIZE = 8;

    private final ExecutorService dispatcher =
            Executors.newFixedThreadPool(
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("dispatcher"));

    private final ExecutorService repeater =
            Executors.newFixedThreadPool(
                    REPEATER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("repeater"));

    private final ExecutorService preparator =
            Executors.newFixedThreadPool(
                    PREPARATOR_THREAD_POOL_SIZE,
                    new NamedThreadFactory("preparator"));

    private final ExecutorService listener =
            Executors.newFixedThreadPool(
                    LISTENER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("listener"));

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    SCHEDULER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("scheduler"));

    private final CompletionService<Request> dispatcherService =
            new ExecutorCompletionService<>(dispatcher);

    private final CompletionService<Request> repeaterService =
            new ExecutorCompletionService<>(repeater);

    /**
     * Creates thread factory instance which assigns a thread name according to the given name.
     */
    private class NamedThreadFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger();
        private final String poolName;

        NamedThreadFactory(String poolName) {
            this.poolName = poolName;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName(String.format("%s-%d", poolName, counter.incrementAndGet()));
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
        long id = masterCounter.incrementAndGet();
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", DEFAULT_NAME, id) : name;

        try {
            this.slaves = getUnmodifiableSlavesMap(slaves);
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Slaves have the same names!", e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Slave mustn't be null!", e);
        }

        LOG.info("{} initialized.", getName());
    }

    private static Map<String, Slave> getUnmodifiableSlavesMap(Slave[] slaves) {
        if (slaves == null || slaves.length == 0) return Collections.emptyMap();

        return Collections.unmodifiableMap(
                Stream.of(slaves)
                        .peek(Objects::requireNonNull)
                        .collect(Collectors.toMap(Slave::getName, slave -> slave)));
    }

    /**
     * Posts project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    protected void postProjectDefault(String projectId, String data) {
        long startTime = System.currentTimeMillis();

        if (projectId == null)
            throw new IllegalArgumentException("Project ID mustn't be null.");

        closeLock.readLock().lock();
        try {
            if (!closed)
                preparator.execute(new PreparationTask(projectId, data));
            else
                throw new IllegalStateException(getName() + " closed already.");
        } finally {
            closeLock.readLock().unlock();
        }

        LOG.trace("postProjectDefault: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProject(String projectId, String data) {
        long startTime = System.currentTimeMillis();

        Project newProject = new Project(projectId, data);

        Project oldProject = projects.putIfAbsent(projectId, newProject);

        boolean isAdded = true;
        if (oldProject != null)
            isAdded = projects.replace(projectId, oldProject, newProject = oldProject.setDataAndIncVersion(data));

        if (!isAdded) {
            LOG.warn("Project{id='{}', data='{}'} is being tried to insert into master's store one more time.", projectId, data);
            prepareProject(projectId, data);
            return;
        }

        if (slaves.isEmpty()) return;

        prepareProjectToSlaves(newProject);

        LOG.trace("prepareProject: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProjectToSlaves(final Project project) {
        long startTime = System.currentTimeMillis();

        Collection<Request> requests = slaves.values().stream()
                .map(slave -> new Request(slave, project))
                .collect(Collectors.toSet());

        boolean registered = register(requests);

        if (!registered)
            LOG.warn("{} aren't registered on failed requests set.", requests);

        requests.forEach(request -> {
            final Runnable listenerTask = new ListenerTask(dispatcherService);
            final Callable<Request> sendingTask = new SendingTask(request);

            closeLock.readLock().lock();
            try {
                if (!closed) {
                    listener.execute(listenerTask);
                    dispatcherService.submit(sendingTask);
                }
            } finally {
                closeLock.readLock().unlock();
            }
        });

        LOG.trace("prepareProjectToSlaves: {}ms", System.currentTimeMillis() - startTime);
    }

    private boolean register(Request request) {
        return register(Collections.singleton(request));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private boolean register(Collection<Request> requests) {
        long startTime = System.currentTimeMillis();

        int attempts = 0;
        boolean registered;
        while (!(registered = failed.addAll(requests)) && ++attempts <= MAX_ATTEMPTS) ;

        LOG.trace("register: {}ms", System.currentTimeMillis() - startTime);

        return registered;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private boolean unregister(Request request) {
        long startTime = System.currentTimeMillis();

        if (!failed.contains(request)) return true;

        int attempts = 0;
        boolean unregistered;
        while (!(unregistered = failed.remove(request)) && ++attempts <= MAX_ATTEMPTS) ;

        LOG.trace("unregister: {}ms", System.currentTimeMillis() - startTime);

        return unregistered;
    }

    /**
     * Returns stored projects.
     *
     * @return projects
     */
    protected Collection<Project> getProjectsDefault() {
        return projects.values().stream()
                .sorted(Comparator.comparing(project -> project.id))
                .collect(Collectors.toList());
    }

    /**
     * Returns slaves which are related to this master instance.
     *
     * @return a set of slaves
     */
    @Override
    public Collection<Slave> getSlaves() {
        return slaves.values().stream()
                .sorted(Comparator.comparing(Slave::getName))
                .collect(Collectors.toList());
    }

    /**
     * Returns failed requests.
     *
     * @return requests
     */
    protected Collection<Request> getFailedDefault() {
        return new ArrayList<>(failed);
    }

    private void shutdownAndAwait(ExecutorService executor, String executorName) throws InterruptedException {
        long startTime = System.currentTimeMillis();

        final int waitingTime = 5;

        executor.shutdown();
        if (!executor.awaitTermination(waitingTime, TimeUnit.SECONDS)) {
            List<Runnable> list = executor.shutdownNow();
            LOG.debug("Total {} {}'s tasks have been cancelled.", list.size(), executorName);
            if (!executor.awaitTermination(waitingTime, TimeUnit.SECONDS)) {
                LOG.warn("Executor '{}' didn't terminate correctly!", executorName);
            }
        }

        LOG.trace("shutdownAndAwait: {}ms", System.currentTimeMillis() - startTime);
    }

    /**
     * Stops workers and thread pools.
     */
    protected void shutdownDefault() {
        long startTime = System.currentTimeMillis();

        closeLock.readLock().lock();
        try {
            if (closed)
                throw new IllegalStateException(getName() + " closed already.");
        } finally {
            closeLock.readLock().unlock();
        }

        closeLock.writeLock().lock();
        try {
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }

        try {
            shutdownAndAwait(preparator, "preparator");
            shutdownAndAwait(dispatcher, "dispatcher");
            shutdownAndAwait(repeater, "repeater");
            shutdownAndAwait(scheduler, "scheduler");
            shutdownAndAwait(listener, "listener");
        } catch (InterruptedException e) {
            LOG.error("Main thread has been interrupted unexpectedly!", e);
        }

        LOG.info("{} closed.", getName());

        LOG.trace("shutdownDefault: {}ms", System.currentTimeMillis() - startTime);
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
            long startTime = System.currentTimeMillis();

            AbstractMaster.this.prepareProject(projectId, data);

            LOG.trace("preparationTask: {}ms", System.currentTimeMillis() - startTime);
        }
    }

    private class ListenerTask implements Runnable {

        private final CompletionService<Request> service;

        ListenerTask(CompletionService<Request> service) {
            this.service = service;
        }

        private boolean hasYoungestProjectVersion(Request oldRequest) {
            return failed.stream()
                    .filter(r -> r.slave == oldRequest.slave)
                    .filter(r -> Objects.equals(r.project.id, oldRequest.project.id))
                    .allMatch(r -> r.project.version < oldRequest.project.version);
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            try {
                final Request oldRequest = service.take().get();

                boolean unregistered = unregister(oldRequest);
                if (!unregistered)
                    LOG.warn("{} isn't unregistered from failed requests set.", oldRequest);

                if (oldRequest.code == Request.Code.REJECTED) {
                    final Request newRequest = oldRequest.incAttemptAndReturn();

                    boolean youngestVersion = hasYoungestProjectVersion(oldRequest);

                    long delay = newRequest.repeatDate - System.currentTimeMillis();

                    Request logRequest;
                    boolean registered;

                    if (youngestVersion) {
                        final Runnable task = new ScheduledTask(newRequest);
                        closeLock.readLock().lock();
                        try {
                            if (!closed) {
                                registered = register(logRequest = newRequest);
                                scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);
                            } else
                                registered = register(
                                        logRequest = oldRequest.setCodeAndReturn(Request.Code.UNDELIVERED));
                        } finally {
                            closeLock.readLock().unlock();
                        }
                    } else
                        registered = register(
                                logRequest = oldRequest.setCodeAndReturn(Request.Code.OUTDATED));

                    if (!registered)
                        LOG.warn("{} isn't registered on failed requests set.", logRequest);
                }
            } catch (InterruptedException e) {
                LOG.debug("Listener task is interrupted correctly.");
            } catch (ExecutionException e) {
                LOG.error("Exception is occurred unexpectedly. ", e);
            }

            LOG.trace("listenerTask: {}ms", System.currentTimeMillis() - startTime);
        }
    }

    private class ScheduledTask implements Runnable {
        private final Request request;

        ScheduledTask(Request request) {
            this.request = request;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            closeLock.readLock().lock();
            try {
                if (!closed) {
                    listener.execute(new ListenerTask(repeaterService));
                    repeaterService.submit(new SendingTask(request));
                }
            } finally {
                closeLock.readLock().unlock();
            }

            LOG.trace("scheduledTask: {}ms", System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Posts a request into a slave.
     */
    private class SendingTask implements Callable<Request> {
        private final Request request;

        SendingTask(Request request) {
            this.request = request;
        }

        @Override
        public Request call() throws Exception {
            long startTime = System.currentTimeMillis();

            try {
                request.slave.postProject(
                        request.project.id,
                        request.project.version,
                        request.project.data);

                return request.setCodeAndReturn(Request.Code.DELIVERED);
            } catch (Exception e) {
                return request.setCodeAndReturn(Request.Code.REJECTED);
            } finally {
                LOG.trace("sendingTask: {}ms", System.currentTimeMillis() - startTime);
            }
        }
    }
}
