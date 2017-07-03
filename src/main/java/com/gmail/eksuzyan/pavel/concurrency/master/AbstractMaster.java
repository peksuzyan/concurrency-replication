package com.gmail.eksuzyan.pavel.concurrency.master;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
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
    private static final int LISTENER_THREAD_POOL_SIZE = 4;

    private final Map<String, Slave> slaves;

    private final ConcurrentMap<String, Project> projects = new ConcurrentHashMap<>();

    private final Set<Request> failed = new CopyOnWriteArraySet<>();

    private final ExecutorService dispatcher =
            Executors.newFixedThreadPool(
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("dispatcher"));

    private final ExecutorService repeater =
            Executors.newFixedThreadPool(
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("repeater"));

    private final CompletionService<Request> dispatcherService =
            new ExecutorCompletionService<>(dispatcher);

    private final CompletionService<Request> repeaterService =
            new ExecutorCompletionService<>(repeater);

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
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("scheduler"));

    private static final int MAX_ATTEMPTS = 3;

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

        if (!preparator.isShutdown())
            preparator.execute(new PreparationTask(projectId, data));
        else
            LOG.debug("{} can't be processed because preparator is shutdown.", new Project(projectId, data));

        LOG.trace("postProjectDefault: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProject(String projectId, String data) {
        long startTime = System.currentTimeMillis();

        Project newProject = new Project(projectId, data);

        Project oldProject = projects.putIfAbsent(projectId, newProject);

        boolean isReplaced = true;
        if (oldProject != null)
            isReplaced = projects.replace(projectId, oldProject, newProject = oldProject.setDataAndIncVersion(data));

        if (!isReplaced) LOG.info("{} isn't inserted into master store.", newProject);

        prepareProjectToSlaves(newProject);

        LOG.trace("prepareProject: {}ms", System.currentTimeMillis() - startTime);
    }

    // TODO: 03.07.2017 Make checking on slaves presence (see logs)
    private void prepareProjectToSlaves(final Project project) {
        long startTime = System.currentTimeMillis();

        Collection<Request> requests = slaves.values().stream()
                .map(slave -> deliverToDispatcher(new Request(slave, project)))
                .collect(Collectors.toList());

        boolean isRegistered = register(requests);

        if (isRegistered)
            requests.forEach(r -> {
                if (!listener.isShutdown()) {
                    listener.execute(new ListenerTask(dispatcherService));
                } else {
                    LOG.debug("{} can't be processed because listener is shutdown.", r);
                }
            });
        else
            LOG.info("{} aren't registered on failed requests set.", requests);

        LOG.trace("prepareProjectToSlaves: {}ms", System.currentTimeMillis() - startTime);
    }

    private Request deliverToDispatcher(Request request) {
        long startTime = System.currentTimeMillis();

        if (!closed)
            dispatcherService.submit(new SendingTask(request));
        else
            LOG.debug("{} can't be delivered because dispatcher is shutdown.", request);

        LOG.trace("deliverToDispatcher: {}ms", System.currentTimeMillis() - startTime);

        return request;
    }

    private boolean register(Request request) {
        return register(Collections.singleton(request));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private boolean register(Collection<Request> requests) {
        long startTime = System.currentTimeMillis();

        int attempts = 0;
        boolean isRegistered;
        while (!(isRegistered = failed.addAll(requests)) && ++attempts <= MAX_ATTEMPTS) ;

        LOG.trace("register: {}ms", System.currentTimeMillis() - startTime);

        return isRegistered;
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private boolean unregister(Request request) {
        long startTime = System.currentTimeMillis();

        int attempts = 0;
        boolean isUnregistered;
        while (!(isUnregistered = failed.remove(request)) && ++attempts <= MAX_ATTEMPTS) ;

        LOG.trace("unregister: {}ms", System.currentTimeMillis() - startTime);

        return isUnregistered;
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
        return new ArrayList<>(failed);
    }

    /**
     * Stops workers and thread pools.
     */
    protected void shutdownDefault() {

        if (closed)
            throw new IllegalStateException(getName() + " closed already.");

        closed = true;

        preparator.shutdown();
        dispatcher.shutdown();
        repeater.shutdown();
        scheduler.shutdown();

        List<Runnable> tasks = listener.shutdownNow();
        if (tasks.isEmpty())
            LOG.debug("Total {} listener's tasks that never commenced execution.", tasks.size());

        try {
            while (!dispatcher.awaitTermination(5, TimeUnit.SECONDS)
                    && !repeater.awaitTermination(5, TimeUnit.SECONDS)) {
                LOG.info("{} dispatcher's and repeater's tasks haven't been yet completed.", getName());
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

    private class ListenerTask implements Runnable {

        private final CompletionService<Request> service;

        ListenerTask(CompletionService<Request> service) {
            this.service = service;
        }

        @Override
        public void run() {
            try {
                final Request oldRequest = service.take().get();

                boolean isUnregistered = unregister(oldRequest);
                if (!isUnregistered) LOG.info("{} isn't unregistered from failed requests set.", oldRequest);

                if (oldRequest.code == Request.Code.REJECTED) {
                    final Request newRequest = oldRequest.incAttemptAndReturn();

                    boolean isYoungestVersion = failed.stream()
                            .filter(r -> r.slave == oldRequest.slave && Objects.equals(r.project.id, oldRequest.project.id))
                            .allMatch(r -> r.project.version < oldRequest.project.version);

                    long delay = newRequest.repeatDate - System.currentTimeMillis();

                    if (isYoungestVersion && !scheduler.isShutdown()) {
                        scheduler.schedule(new ScheduledTask(newRequest), delay, TimeUnit.MILLISECONDS);
                    } else {
                        boolean isRegistered = register(oldRequest);
                        if (!isRegistered) LOG.info("{} isn't registered on failed requests set.", oldRequest);
                    }
                }
            } catch (InterruptedException e) {
                LOG.debug("Listener task is interrupted correctly.");
            } catch (ExecutionException e) {
                LOG.error("Exception is occurred unexpectedly. ", e);
            }
        }
    }

    private class ScheduledTask implements Runnable {
        private final Request request;

        ScheduledTask(Request request) {
            this.request = request;
        }

        @Override
        public void run() {
            boolean isRegistered = register(request);
            if (!isRegistered) LOG.info("{} isn't registered on failed requests set.", request);

            if (!listener.isShutdown() && !repeater.isShutdown()) {
                listener.execute(new ListenerTask(repeaterService));

                if (!closed)
                    repeaterService.submit(new SendingTask(request));
            }
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
