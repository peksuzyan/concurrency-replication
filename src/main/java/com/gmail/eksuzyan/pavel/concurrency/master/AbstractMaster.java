package com.gmail.eksuzyan.pavel.concurrency.master;

import com.gmail.eksuzyan.pavel.concurrency.entities.Message;
import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;
import com.gmail.eksuzyan.pavel.concurrency.tasks.SendingTask;
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
    private static final String defaultName = "Master";

    /**
     * Master name.
     */
    private String name;

    private static final int INITIAL_QUEUE_CAPACITY = 100;
    private static final int THREAD_POOL_SIZE = 4;

    private final Map<String, Slave> slaves;

    private final Map<String, Project> projects = new HashMap<>();

    private final ScheduledExecutorService dispatcher =
            Executors.newScheduledThreadPool(THREAD_POOL_SIZE);

    private final Comparator<Request> requestDateComparator =
            (r1, r2) -> (int) (r1.repeatDate - r2.repeatDate);

    private final BlockingQueue<Message> entryMessages =
            new LinkedBlockingQueue<>();

    private final BlockingQueue<Request> failedRequests =
            new PriorityBlockingQueue<>(INITIAL_QUEUE_CAPACITY, requestDateComparator);

    private Thread restoreWorker;
    private Thread prepareWorker;

    /**
     * Single constructor.
     *
     * @param name   master name
     * @param slaves slaves
     */
    protected AbstractMaster(String name, Slave... slaves) {
        long id = ++masterCounter;
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", defaultName, id) : name;

        try {
            this.slaves =
                    (slaves == null || slaves.length == 0)
                            ? Collections.emptyMap()
                            : Collections.unmodifiableMap(
                            Arrays.stream(slaves)
                                    .peek(Objects::requireNonNull)
                                    .collect(Collectors.toMap(
                                            Slave::getName, slave -> slave)));
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Slaves have the same names!", e);
        } catch (NullPointerException e) {
            throw new IllegalArgumentException("Slave mustn't be null!", e);
        }

        initWorkers();

        LOG.info("{} initialized.", getName());
    }

    private void initWorkers() {

        restoreWorker = new Thread(() -> {
            Request request = null;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    long startTime = System.currentTimeMillis();

                    request = null;

                    request = failedRequests.take();

                    if (request != null) deliverToExecutor(request);

                    LOG.trace("restoreWorker: " + (System.currentTimeMillis() - startTime));
                }
            } catch (InterruptedException e) {
                LOG.info("{} BackWorker has been collapsed due to thread interruption.", getName());
            }
        });

        prepareWorker = new Thread(() -> {
            Message message = null;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    message = null;

                    message = entryMessages.take();

                    long startTime = System.currentTimeMillis();

                    if (message != null) prepareProject(message);

                    LOG.trace("[2] prepareWorker: " + (System.currentTimeMillis() - startTime));
                }
            } catch (InterruptedException e) {
                LOG.info("{} RestoreWorker has been collapsed due to thread interruption.", getName());
            }
        });

        restoreWorker.setName(getName() + "-restoreThread");
        prepareWorker.setName(getName() + "-prepareThread");

        restoreWorker.start();
        prepareWorker.start();
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
            throw new IllegalArgumentException("Project ID mustn't be null.") ;

        final Message message = new Message(projectId, data);

        try {
            entryMessages.put(message);
        } catch (InterruptedException e) {
            LOG.error("Client thread couldn't put {} into entry queue!", message);
        }

        LOG.trace("[1] postProjectDefault: " + (System.currentTimeMillis() - startTime));
    }

    private void prepareProject(Message message) {
        long startTime = System.currentTimeMillis();

        Project newProject = !projects.containsKey(message.projectId)
                ? new Project(message.projectId, message.data)
                : projects.get(message.projectId).setDataAndIncVersion(message.data);

        projects.put(message.projectId, newProject);

        prepareProjectToSlaves(newProject);

        LOG.trace("[3] prepareProject: " + (System.currentTimeMillis() - startTime));
    }

    private void prepareProjectToSlaves(final Project project) {
        long startTime = System.currentTimeMillis();

        slaves.values().forEach(slave ->
                deliverToExecutor(new Request(slave.getName(), project)));

        LOG.trace("[4] prepareProjectToSlaves: " + (System.currentTimeMillis() - startTime));
    }

    private void deliverToExecutor(Request request) {
        long startTime = System.currentTimeMillis();

        Runnable task = new SendingTask(
                slaves.get(request.slave), request, failedRequests);

        if (!dispatcher.isShutdown()) {
            dispatcher.schedule(
                    task,
                    request.repeatDate - System.currentTimeMillis(),
                    TimeUnit.MILLISECONDS);
        }
        else
            try {
                failedRequests.put(request);
            } catch (InterruptedException e) {
                LOG.error(Thread.currentThread().getName() + " has been interrupted unexpectedly!", e);
            }

        LOG.trace("[5] deliverToExecutor: " + (System.currentTimeMillis() - startTime));
    }

    /**
     * Returns stored projects.
     *
     * @return projects
     */
    protected Collection<Project> getProjectsDefault() {
        return projects.values();
    }

    /**
     * Returns failed requests.
     *
     * @return requests
     */
    protected Collection<Request> getFailedDefault() {
        return failedRequests.stream()
                .filter(r -> r.attempt > 1)
                .collect(Collectors.toList());
    }

    /**
     * Stops workers and thread pool.
     */
    protected void shutdownDefault() {

        restoreWorker.interrupt();
        prepareWorker.interrupt();

        dispatcher.shutdown();

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
        return slaves.values();
    }
}
