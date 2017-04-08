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

    private static final int INITIAL_QUEUE_CAPACITY = 10;

    private final Map<String, Slave> slaves;

    private final Map<String, Project> projects = new HashMap<>();

    private final ExecutorService dispatcher = Executors.newCachedThreadPool();

    private final Comparator<Request> requestDateComparator =
            (r1, r2) -> r1.getRepeatTime().compareTo(r2.getRepeatTime());

    private final BlockingQueue<Message> entryMessages =
            new LinkedBlockingQueue<>();

    private final BlockingQueue<Request> failedRequests =
            new PriorityBlockingQueue<>(INITIAL_QUEUE_CAPACITY, requestDateComparator);

    private Thread backWorker;
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
            this.slaves = (slaves == null || slaves.length == 0)
                    ? Collections.emptyMap()
                    : Collections.unmodifiableMap(
                    Arrays.stream(slaves)
                            .collect(Collectors.toMap(
                                    Slave::getName, slave -> slave)));
        } catch (IllegalStateException e) {
            throw new IllegalArgumentException("Slaves have the same names!", e);
        }

        initWorkers();

        LOG.info("{} initialized.", getName());
    }

    private void initWorkers() {

        backWorker = new Thread(() -> {
            Request request = null;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    request = null;

                    request = failedRequests.take();

                    if (request != null) deliverToExecutor(request);
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

                    if (message != null) prepareProject(message);
                }
            } catch (InterruptedException e) {
                LOG.info("{} PrepareWorker has been collapsed due to thread interruption.", getName());
            }
        });

        backWorker.setName(getName() + "-backThread");
        prepareWorker.setName(getName() + "-prepareThread");

        backWorker.start();
        prepareWorker.start();
    }

    /**
     * Posts project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    protected void postProjectDefault(String projectId, String data) {

        final Message message = new Message(projectId, data);
        try {
            entryMessages.put(message);
        } catch (InterruptedException e) {
            LOG.error("Client thread couldn't put {} into entry queue!", message);
        }
    }

    private void prepareProject(Message message) {

        Project newProject = !projects.containsKey(message.projectId)
                ? new Project(message.projectId, message.data)
                : projects.get(message.projectId).setDataAndIncVersion(message.data);

        projects.put(message.projectId, newProject);

        prepareProjectToSlaves(newProject);
    }

    private void prepareProjectToSlaves(final Project project) {
        slaves.values().forEach(slave ->
                deliverToExecutor(new Request(slave.getName(), project)));
    }

    private void deliverToExecutor(Request request) {
        Runnable task = new SendingTask(
                slaves.get(request.getSlave()), request, failedRequests);

        if (!dispatcher.isShutdown()) dispatcher.execute(task);
        else if (request.getAttempt() > 1)
            try {
                failedRequests.put(request);
            } catch (InterruptedException e) {
                LOG.error(Thread.currentThread().getName() + " has been interrupted unexpectedly!", e);
            }
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
        return failedRequests;
    }

    /**
     * Stops workers and thread pool.
     */
    protected void shutdownDefault() {

        backWorker.interrupt();
        prepareWorker.interrupt();

        dispatcher.shutdown();

        try {
            while (!dispatcher.awaitTermination(15, TimeUnit.SECONDS)) {
                LOG.info("{}: Dispatcher tasks haven't been yet completed.", getName());
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
