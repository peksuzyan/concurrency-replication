package com.gmail.eksuzyan.pavel.concurrency.logic.master;

import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.util.config.MasterProperties;
import com.gmail.eksuzyan.pavel.concurrency.util.jmx.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Presents a default implementation of master interface.
 *
 * @author Pavel Eksuzian.
 *         Created: 04.04.2017.
 */
public class DefaultMaster<T extends Serializable> implements Master<T> {

    /**
     * Ordinary logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(DefaultMaster.class);

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

    private final Map<String, Slave<T>> slaves;

    private final ConcurrentMap<String, Project<T>> projects = new ConcurrentHashMap<>();

    private final Set<Request<T>> failed = ConcurrentHashMap.newKeySet();

    private final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();

    private static final int MAX_ATTEMPTS = 3;

    private final Status status = new Status();

    private final ListenerTaskFactory factory;

    private final ExecutorService dispatcher =
            Executors.newFixedThreadPool(
                    MasterProperties.getDispatcherThreadPoolSize(),
                    new NamedThreadFactory(MasterProperties.getDispatcherThreadPoolName()));

    private final ExecutorService repeater =
            Executors.newFixedThreadPool(
                    MasterProperties.getRepeaterThreadPoolSize(),
                    new NamedThreadFactory(MasterProperties.getRepeaterThreadPoolName()));

    private final ExecutorService preparator =
            Executors.newFixedThreadPool(
                    MasterProperties.getPreparatorThreadPoolSize(),
                    new NamedThreadFactory(MasterProperties.getPreparatorThreadPoolName()));

    private final ExecutorService listener =
            Executors.newFixedThreadPool(
                    MasterProperties.getListenerThreadPoolSize(),
                    new NamedThreadFactory(MasterProperties.getListenerThreadPoolName()));

    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    MasterProperties.getSchedulerThreadPoolSize(),
                    new NamedThreadFactory(MasterProperties.getSchedulerThreadPoolName()));

    private final CompletionService<Request<T>> dispatcherService =
            new ExecutorCompletionService<>(dispatcher);

    private final CompletionService<Request<T>> repeaterService =
            new ExecutorCompletionService<>(repeater);

    @SafeVarargs
    public DefaultMaster(Slave<T>... slaves) {
        this(null, Mode.SELECTING, slaves);
    }

    @SafeVarargs
    public DefaultMaster(String name, Slave<T>... slaves) {
        this(name, Mode.SELECTING, slaves);
    }

    @SafeVarargs
    public DefaultMaster(Mode mode, Slave<T>... slaves) {
        this(null, mode, slaves);
    }

    /**
     * Extended constructor.
     *
     * @param name   master name
     * @param mode   strategy mode
     * @param slaves slaves
     */
    @SafeVarargs
    public DefaultMaster(String name, Mode mode, Slave<T>... slaves) {
        long id = masterCounter.incrementAndGet();
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", DEFAULT_NAME, id) : name;

        this.factory = new ListenerTaskFactory(mode);

        try {
            this.slaves = getUnmodifiableSlavesMap(slaves);
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Slaves have the same names!", e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Slave mustn't be null!", e);
        }

        LOG.info("{} initialized.", getName());
    }

    private Map<String, Slave<T>> getUnmodifiableSlavesMap(Slave<T>[] slaves) {
        if (slaves == null || slaves.length == 0) return Collections.emptyMap();

        return Collections.unmodifiableMap(
                Stream.of(slaves)
                        .peek(Objects::requireNonNull)
                        .sorted(Comparator.comparing(Slave::getName))
                        .collect(Collectors.toMap(Slave::getName, slave -> slave)));
    }

    /**
     * Posts project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    public void postProject(String projectId, T data) {
        long startTime = System.currentTimeMillis();

        if (projectId == null)
            throw new IllegalArgumentException("Project ID mustn't be null.");

        closeLock.readLock().lock();
        try {
            if (!closed) {
                preparator.execute(new PreparationTask(projectId, data));

                LOG.debug("Project{id='{}', data='{}'} is posted.", projectId, data);
            } else
                throw new IllegalStateException(getName() + " closed already.");
        } finally {
            closeLock.readLock().unlock();
        }

        LOG.trace("postProjectDefault: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProject(String projectId, T data) {
        long startTime = System.currentTimeMillis();

        Project<T> newProject = new Project<>(projectId, data);

        Project<T> oldProject = projects.putIfAbsent(projectId, newProject);

        boolean isAdded = true;
        if (oldProject != null)
            isAdded = projects.replace(projectId, oldProject, newProject = oldProject.setDataAndIncVersion(data));

        if (!isAdded) {
            LOG.warn("Project{id='{}', data='{}'} is being tried to insert into master's store one more time.", projectId, data);
            prepareProject(projectId, data);
            return;
        }

        LOG.debug("Project{id='{}', data='{}'} is stored.", projectId, data);

        if (slaves.isEmpty()) return;

        prepareProjectToSlaves(newProject);

        LOG.trace("prepareProject: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProjectToSlaves(final Project<T> project) {
        long startTime = System.currentTimeMillis();

        Collection<Request<T>> requests = slaves.values().stream()
                .map(slave -> new Request<>(slave, project))
                .collect(Collectors.toSet());

        boolean registered = register(requests);

        if (!registered)
            LOG.warn("{} aren't registered on failed requests set.", requests);

        requests.forEach(request -> {
            final Runnable listenerTask = factory.make(dispatcherService);
            final Callable<Request<T>> sendingTask = new SendingTask(request);

            closeLock.readLock().lock();
            try {
                if (!closed) {
                    listener.execute(listenerTask);
                    dispatcherService.submit(sendingTask);

                    LOG.debug("{} is prepared to deliver.", request);
                }
            } finally {
                closeLock.readLock().unlock();
            }
        });

        LOG.trace("prepareProjectToSlaves: {}ms", System.currentTimeMillis() - startTime);
    }

    private boolean register(Request<T> request) {
        return register(Collections.singleton(request));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private boolean register(Collection<Request<T>> requests) {
        long startTime = System.currentTimeMillis();

        int attempts = 0;
        boolean registered;
        while (!(registered = failed.addAll(requests)) && ++attempts <= MAX_ATTEMPTS) ;

        if (registered) status.addAndGetRegisteredRequests(requests.size());

        LOG.trace("register: {}ms", System.currentTimeMillis() - startTime);

        return registered;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private boolean unregister(Request<T> request) {
        long startTime = System.currentTimeMillis();

        if (!failed.contains(request)) return true;

        int attempts = 0;
        boolean unregistered;
        while (!(unregistered = failed.remove(request)) && ++attempts <= MAX_ATTEMPTS) ;

        if (unregistered) status.decAndGetRegisteredRequests();

        LOG.trace("unregister: {}ms", System.currentTimeMillis() - startTime);

        return unregistered;
    }

    /**
     * Returns stored projects.
     *
     * @return projects
     */
    @Override
    public Collection<Project<T>> getProjects() {
        return new ArrayList<>(projects.values());
    }

    /**
     * Returns slaves which are related to this master instance.
     *
     * @return a set of slaves
     */
    @Override
    public Collection<Slave<T>> getSlaves() {
        return new ArrayList<>(slaves.values());
    }

    /**
     * Returns repeat delivery mode is used by this master instance.
     *
     * @return repeat delivery mode
     */
    @Override
    public Mode getDeliveryMode() {
        return factory.mode;
    }

    /**
     * Returns failed requests.
     *
     * @return requests
     */
    @Override
    public Collection<Request<T>> getFailed() {
        return new ArrayList<>(failed);
    }

    /**
     * Shutdowns and awaits specified executor with given name.
     *
     * @param executor executor
     * @param executorName executor name
     * @throws InterruptedException if thread is interrupted while executor is shutting down
     */
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
    @Override
    public void close() {
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
            shutdownAndAwait(preparator, MasterProperties.getPreparatorThreadPoolName());
            shutdownAndAwait(dispatcher, MasterProperties.getDispatcherThreadPoolName());
            shutdownAndAwait(repeater, MasterProperties.getRepeaterThreadPoolName());
            shutdownAndAwait(scheduler, MasterProperties.getSchedulerThreadPoolName());
            shutdownAndAwait(listener, MasterProperties.getListenerThreadPoolName());
        } catch (InterruptedException e) {
            LOG.error("Main thread has been interrupted unexpectedly!", e);
        }

        status.close();

        LOG.info("{} closed. The rest of registered requests: {}. Total replicated projects: {}.",
                getName(), status.getRegisteredRequests(), status.getReplicatedProjects());

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
        private final T data;

        PreparationTask(String projectId, T data) {
            this.projectId = projectId;
            this.data = data;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            DefaultMaster.this.prepareProject(projectId, data);

            LOG.trace("preparationTask: {}ms", System.currentTimeMillis() - startTime);
        }
    }

    /**
     * Specific broadcast task to send projects.
     */
    @SuppressWarnings("Duplicates")
    private class BroadcastingListenerTask extends ListenerTask {
        BroadcastingListenerTask(CompletionService<Request<T>> service) {
            super(service);
        }

        @Override
        protected void scheduleRejectedRequest(Request<T> oldRequest) {
            final Request<T> newRequest = oldRequest.incAttemptAndReturn();

            long delay = newRequest.repeatDate - System.currentTimeMillis();

            Request<T> logRequest;
            boolean registered;

            final Runnable task = new ScheduledTask(newRequest);
            closeLock.readLock().lock();
            try {
                if (!closed) {
                    registered = register(logRequest = newRequest);
                    scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);

                    LOG.debug("{} is scheduled to be sent again.", logRequest);
                } else
                    registered = register(
                            logRequest = oldRequest.setCodeAndReturn(Request.Code.UNDELIVERED));
            } finally {
                closeLock.readLock().unlock();
            }

            if (!registered)
                LOG.warn("{} isn't registered on failed requests set.", logRequest);
        }
    }

    /**
     * Specific broadcast task to send projects.
     */
    @SuppressWarnings("Duplicates")
    private class SelectingListenerTask extends ListenerTask {
        SelectingListenerTask(CompletionService<Request<T>> service) {
            super(service);
        }

        private boolean hasYoungestProjectVersion(Request<T> oldRequest) {
            return failed.stream()
                    .filter(r -> r.slave == oldRequest.slave)
                    .filter(r -> Objects.equals(r.project.id, oldRequest.project.id))
                    .allMatch(r -> r.project.version < oldRequest.project.version);
        }

        @Override
        protected void scheduleRejectedRequest(Request<T> oldRequest) {
            final Request<T> newRequest = oldRequest.incAttemptAndReturn();

            boolean youngestVersion = hasYoungestProjectVersion(oldRequest);

            long delay = newRequest.repeatDate - System.currentTimeMillis();

            Request<T> logRequest;
            boolean registered;

            if (youngestVersion) {
                final Runnable task = new ScheduledTask(newRequest);
                closeLock.readLock().lock();
                try {
                    if (!closed) {
                        registered = register(logRequest = newRequest);
                        scheduler.schedule(task, delay, TimeUnit.MILLISECONDS);

                        LOG.debug("{} is scheduled to be sent again.", logRequest);
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
    }

    /**
     * Abstract listener task encapsulating common behaviour when project is sent.
     */
    private abstract class ListenerTask implements Runnable {

        private final CompletionService<Request<T>> service;

        ListenerTask(CompletionService<Request<T>> service) {
            this.service = service;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            try {
                final Request<T> oldRequest = service.take().get();

                boolean unregistered = unregister(oldRequest);
                if (!unregistered)
                    LOG.warn("{} isn't unregistered from failed requests set.", oldRequest);

                if (oldRequest.code == Request.Code.REJECTED) {
                    scheduleRejectedRequest(oldRequest);
                } else {
                    status.incAndGetReplicatedProjects();
                }
            } catch (InterruptedException e) {
                LOG.debug("Listener task is interrupted correctly.");
            } catch (ExecutionException e) {
                LOG.error("Exception is occurred unexpectedly. ", e);
            }

            LOG.trace("listenerTask: {}ms", System.currentTimeMillis() - startTime);
        }

        protected abstract void scheduleRejectedRequest(Request<T> oldRequest);
    }

    /**
     * Contains request till it'll be ready to be sent again.
     */
    private class ScheduledTask implements Runnable {
        private final Request<T> request;

        ScheduledTask(Request<T> request) {
            this.request = request;
        }

        @Override
        public void run() {
            long startTime = System.currentTimeMillis();

            closeLock.readLock().lock();
            try {
                if (!closed) {
                    listener.execute(factory.make(repeaterService));
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
    private class SendingTask implements Callable<Request<T>> {
        private final Request<T> request;

        SendingTask(Request<T> request) {
            this.request = request;
        }

        @Override
        public Request<T> call() throws Exception {
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

    /**
     * Factory to generate a new listener task having proper repeat delivery mode.
     */
    private class ListenerTaskFactory {

        final Mode mode;

        private final ListenerTaskGenerator generator;

        ListenerTaskFactory(Mode mode) {
            switch (mode) {
                case BROADCASTING:
                    generator = new ListenerTaskGenerator() {
                        @Override
                        ListenerTask make(CompletionService<Request<T>> service) {
                            return new BroadcastingListenerTask(service);
                        }
                    };
                    break;
                case SELECTING:
                    generator = new ListenerTaskGenerator() {
                        @Override
                        ListenerTask make(CompletionService<Request<T>> service) {
                            return new SelectingListenerTask(service);
                        }
                    };
                    break;
                default:
                    throw new IllegalArgumentException("Repeat delivery mode [" + mode + "] isn't allowed.");
            }

            this.mode = mode;
        }

        ListenerTask make(CompletionService<Request<T>> service) {
            return generator.make(service);
        }
    }

    /**
     * Generates listener task having type in according to master delivery mode.
     */
    private abstract class ListenerTaskGenerator {

        /**
         * Makes a new listener task.
         *
         * @param service service
         * @return listener task
         */
        abstract ListenerTask make(CompletionService<Request<T>> service);
    }

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
}
