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
    private final Map<Request, Date> failedRequests = new HashMap<>();

    private final Queue<Request> requestsQueue = new LinkedBlockingQueue<>();

    public MasterController(SlaveController... slaves) {
        this.slaves = Arrays.stream(slaves)
                .collect(Collectors.toMap(SlaveController::getId, slave -> slave));
    }

    public void postProject(String projectId, String data) {
        Project project = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : new Project(projectId, data, projects.get(projectId).getVersion() + 1L);
        projects.put(projectId, project);

        slaves.keySet().forEach(slaveId -> requestsQueue.offer(new Request(slaveId, project)));
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public Collection<Request> getFailed() {
        return failedRequests.keySet();
    }

}
