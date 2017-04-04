package com.gmail.eksuzyan.pavel.concurrency.master.impl;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.master.AbstractMaster;
import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;

import java.util.Collection;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class HealthyMaster extends AbstractMaster {

    public HealthyMaster(Slave... slaves) {
        this(null, slaves);
    }

    public HealthyMaster(String name, Slave... slaves) {
        super(name, slaves);
    }

    /**
     * Posts a project to inner store and related slaves.
     *
     * @param projectId project id
     * @param data      project data
     */
    @Override
    public void postProject(String projectId, String data) {
        postProjectV(projectId, data);
    }

    /**
     * Returns projects which are stored by master.
     *
     * @return a set of projects
     */
    @Override
    public Collection<Project> getProjects() {
        return getProjectsV();
    }

    /**
     * Returns requests which are failed while being posted.
     *
     * @return a set of requests
     */
    @Override
    public Collection<Request> getFailed() {
        return getFailedV();
    }

    @Override
    public void close() {
        shutdown();
    }
}
