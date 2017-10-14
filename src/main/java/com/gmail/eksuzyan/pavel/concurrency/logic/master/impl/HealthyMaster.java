package com.gmail.eksuzyan.pavel.concurrency.logic.master.impl;

import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Request;
import com.gmail.eksuzyan.pavel.concurrency.logic.master.AbstractMaster;
import com.gmail.eksuzyan.pavel.concurrency.logic.slave.Slave;

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
        postProjectDefault(projectId, data);
    }

    /**
     * Returns projects which are stored by master.
     *
     * @return a set of projects
     */
    @Override
    public Collection<Project> getProjects() {
        return getProjectsDefault();
    }

    /**
     * Returns requests which are failed while being posted.
     *
     * @return a set of requests
     */
    @Override
    public Collection<Request> getFailed() {
        return getFailedDefault();
    }

    @Override
    public void close() {
        shutdownDefault();
    }
}
