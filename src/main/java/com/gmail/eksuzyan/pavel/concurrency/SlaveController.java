package com.gmail.eksuzyan.pavel.concurrency;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class SlaveController {

    private static long lastId = 0L;
    private final long id;

    private final Map<String, Project> projects = new HashMap<>();

    public SlaveController() {
        this.id = ++lastId;
    }

    public void postProject(String projectId, long version, String data) {
        Project project = !projects.containsKey(projectId)
                ? new Project(projectId, data)
                : new Project(projectId, data, version);
        projects.put(projectId, project);
    }

    public Collection<Project> getProjects() {
        return projects.values();
    }

    public long getId() {
        return id;
    }
}
