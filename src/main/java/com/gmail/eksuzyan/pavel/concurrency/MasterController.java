package com.gmail.eksuzyan.pavel.concurrency;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class MasterController {

    private final Map<Long, SlaveController> slaves;

    private final Map<String, Project> projects = new HashMap<>();
    private final Set<Request> failedRequests = new TreeSet<>();
    private final Queue<Request> requests = new LinkedBlockingQueue<>();

    private boolean isStopped = false;

    public MasterController(SlaveController... slaves) {
        this.slaves = Arrays.stream(slaves)
                .collect(Collectors.toMap(SlaveController::getId, slave -> slave));

        initWorkers();
    }

    public void initWorkers() {
        Thread worker = new Thread(new Runnable() {
            @Override
            public void run() {
                Request request;
                while (!isStopped) {
                    if ((request = requests.poll()) != null) {
                        Project project = request.getProject();
                        SlaveController slave = slaves.get(request.getSlaveId());
                        int code = slave.postProject(project.getId(), project.getVersion(), project.getData());
                        if (code != 0) {

                            failedRequests.add(request);
                        }
                    }
                }
            }
        });
    }

    public void postProject(String projectId, String data) {
        Project project = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : new Project(projectId, data, projects.get(projectId).getVersion() + 1L);
        projects.put(projectId, project);

        slaves.keySet().forEach(slaveId -> requests.offer(new Request(slaveId, project)));
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public Collection<Request> getFailed() {
        return failedRequests;
    }

    public void destroy() {
        isStopped = true;
    }

}
