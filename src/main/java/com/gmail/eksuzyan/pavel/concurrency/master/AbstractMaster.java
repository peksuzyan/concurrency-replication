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
    private static final int LISTENER_THREAD_POOL_SIZE = 4;

    private final Map<String, Slave> slaves;

    private final ConcurrentMap<String, Project> projects = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Set<Request> failed = new HashSet<>();

    private final ExecutorService dispatcher =
            Executors.newFixedThreadPool(
                    DISPATCHER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("dispatcher"));

    private final ScheduledExecutorService repeater =
            Executors.newScheduledThreadPool(
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

    private final ScheduledExecutorService listener =
            Executors.newScheduledThreadPool(
                    LISTENER_THREAD_POOL_SIZE,
                    new NamedThreadFactory("listener"));

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

        listener.scheduleAtFixedRate(
                new ListenerTask(false), 0, 10, TimeUnit.MILLISECONDS);
        listener.scheduleAtFixedRate(
                new ListenerTask(true), 0, 10, TimeUnit.MILLISECONDS);

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

    //todo
    private void prepareProject(String projectId, String data) {
        long startTime = System.currentTimeMillis();

        Project newProject = new Project(projectId, data);

        Project oldProject = projects.putIfAbsent(projectId, newProject);

        boolean isActual = false;
        if (oldProject != null)
            isActual = projects.replace(
                    projectId, oldProject, newProject = oldProject.setDataAndIncVersion(data));

        if (isActual)
            prepareProjectToSlaves(newProject);
        else
            prepareProject(projectId, data);

        LOG.trace("prepareProject: {}ms", System.currentTimeMillis() - startTime);
    }

    private void prepareProjectToSlaves(final Project project) {
        long startTime = System.currentTimeMillis();

        Collection<Request> requests = slaves.values().parallelStream()
                .map(slave -> deliverToDispatcher(new Request(slave, project)))
                .collect(Collectors.toList());

        boolean isRegistered = registerRequests(requests);

        if (!isRegistered)
            LOG.info("Total {} requests haven't been registered.", requests.size());

        LOG.trace("prepareProjectToSlaves: {}ms", System.currentTimeMillis() - startTime);
    }

    private Request deliverToDispatcher(Request request) {
        long startTime = System.currentTimeMillis();

        if (!dispatcher.isShutdown())
            dispatcher.submit(new SendingTask(request));
        else
            LOG.debug("{} can't be delivered because dispatcher is shutdown.", request);

        LOG.trace("deliverToDispatcher: {}ms", System.currentTimeMillis() - startTime);

        return request;
    }

    private boolean registerRequests(Collection<Request> requests) {
        long startTime = System.currentTimeMillis();

        lock.writeLock().lock();
        try {
            return failed.addAll(requests);
        } finally {
            lock.writeLock().unlock();

            LOG.trace("registerRequests: {}ms", System.currentTimeMillis() - startTime);
        }
    }

    private void deliverToRepeater(Request request) {
        long startTime = System.currentTimeMillis();

        long delay = request.repeatDate - System.currentTimeMillis();

        if (!repeater.isShutdown())
            repeater.schedule(new SendingTask(request), delay, TimeUnit.MILLISECONDS);
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
        lock.readLock().lock();
        try {
            return new ArrayList<>(failed);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Stops workers and thread pools.
     */
    protected void shutdownDefault() {

        if (closed)
            throw new IllegalStateException(getName() + " closed already.");

        closed = true;

        preparator.shutdownNow();
        listener.shutdownNow();
        dispatcher.shutdownNow();
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

    private class ListenerTask implements Runnable {

        private final boolean forRepeater;
        private final CompletionService<Request> service;

        ListenerTask(boolean forRepeater) {
            this.forRepeater = forRepeater;
            this.service = forRepeater ? repeaterService : dispatcherService;
        }

        private void deliverTo(Request request) {
            if (forRepeater)
                deliverToRepeater(request);
            else
                deliverToDispatcher(request);
        }

        @Override
        public void run() {
            try {
                final Request oldRequest = service.take().get();

                lock.writeLock().lock();
                boolean isRemoved = failed.remove(oldRequest);

                if (!isRemoved)
                    LOG.info("{} isn't removed from failed requests set.", oldRequest);

                if (oldRequest.code == Request.Code.REJECTED) {

                    Request newRequest = oldRequest.incAttemptAndReturn();

                    boolean isRegistered;

                    boolean isYoungestVersion =
                            failed.stream().allMatch(
                                    r -> r.slave == oldRequest.slave
                                            && Objects.equals(r.project.id, oldRequest.project.id)
                                            && r.project.version < oldRequest.project.version);

                    if (isYoungestVersion) {
                        isRegistered = failed.add(newRequest);
                        deliverTo(newRequest);
                    } else {
                        isRegistered = failed.add(oldRequest);
                    }

                    if (!isRegistered)
                        LOG.info("{} isn't registered on failed requests set.",
                                isYoungestVersion ? newRequest : oldRequest);
                }
            } catch (InterruptedException e) {
                LOG.debug("Listener task is interrupted correctly.");
            } catch (ExecutionException e) {
                LOG.error("Exception is occurred unexpectedly. ", e);
            } finally {
                if (lock.writeLock().isHeldByCurrentThread()) lock.writeLock().unlock();
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

//                LOG.debug("[+] => {} => {}.", request.slave.getName(), request);
                return request.setCodeAndReturn(Request.Code.DELIVERED);
            } catch (Exception e) {
//                LOG.debug("[-] => {} => {}.", request.slave.getName(), request);
                return request.setCodeAndReturn(Request.Code.REJECTED);
            } finally {
                LOG.trace("sendingTask: {}ms", System.currentTimeMillis() - startTime);
            }
        }
    }
}
