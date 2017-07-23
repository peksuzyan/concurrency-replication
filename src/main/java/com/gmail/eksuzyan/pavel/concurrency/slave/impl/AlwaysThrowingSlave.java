package com.gmail.eksuzyan.pavel.concurrency.slave.impl;

import com.gmail.eksuzyan.pavel.concurrency.entities.Project;
import com.gmail.eksuzyan.pavel.concurrency.slave.AbstractSlave;

import java.util.Collection;

/**
 * @author Pavel Eksuzian.
 *         Created: 09.07.2017.
 */
public class AlwaysThrowingSlave extends AbstractSlave {

    public AlwaysThrowingSlave() {
        this(null);
    }

    public AlwaysThrowingSlave(String name) {
        super(name);
    }

    @Override
    public void postProject(String projectId, long version, String data) throws Exception {
        throw new Exception();
    }

    @Override
    public Collection<Project> getProjects() {
        return getProjectsDefault();
    }

    @Override
    public void close() {
        shutdownDefault();
    }
}
