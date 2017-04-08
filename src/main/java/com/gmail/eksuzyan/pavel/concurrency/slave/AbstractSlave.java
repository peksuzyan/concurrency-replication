package com.gmail.eksuzyan.pavel.concurrency.slave;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Presents a base implementation of slave interface.
 *
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public abstract class AbstractSlave implements Slave {

    /**
     * Counter serves to generate unique slave ID.
     */
    private static long slaveCounter = 0L;

    /**
     * Default slave name.
     */
    private static final String defaultName = "Slave";

    /**
     * Slave name.
     */
    private final String name;

    /**
     * Slave store serves to keep published projects.
     */
    private final ConcurrentMap<String, Project> projects = new ConcurrentHashMap<>();

    /**
     * Single constructor.
     *
     * @param name slave name
     */
    protected AbstractSlave(String name) {
        long id = ++slaveCounter;
        this.name = (name == null || name.trim().isEmpty())
                ? String.format("%s-%d", defaultName, id) : name;
    }

    /**
     * Returns slave name.
     *
     * @return slave name
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * Base implementation of postProject method.
     *
     * @param projectId project id
     * @param version   project version
     * @param data      project data
     */
    protected final void postProjectDefault(String projectId, long version, String data) {
        Project newProject = new Project(projectId, data, version);

        Project oldProject = projects.putIfAbsent(projectId, newProject);

        if (Objects.nonNull(oldProject)
                && !Objects.equals(oldProject, newProject)
                && oldProject.version < newProject.version) {
            projects.replace(projectId, oldProject, newProject);
        }
    }

    /**
     * Base implementation of getProjects method.
     *
     * @return a set of projects
     */
    protected final Collection<Project> getProjectsDefault() {
        return projects.values();
    }
}
