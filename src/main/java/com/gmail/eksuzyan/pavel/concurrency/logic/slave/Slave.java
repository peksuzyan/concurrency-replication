package com.gmail.eksuzyan.pavel.concurrency.logic.slave;

import com.gmail.eksuzyan.pavel.concurrency.logic.entities.Project;

import java.io.Closeable;
import java.util.Collection;

/**
 * Describes methods to interact with a slave instance.
 *
 * @author Pavel Eksuzian.
 *         Created: 03.04.2017.
 */
public interface Slave<T> extends Closeable {

    /**
     * Posts a new project version into store.
     *
     * @param projectId project id
     * @param version   project version
     * @param data      project data
     * @throws Exception exception
     */
    void postProject(String projectId, long version, T data) throws Exception;

    /**
     * Returns projects which are stored by slave.
     *
     * @return a set of projects
     */
    Collection<Project<T>> getProjects();

    /**
     * Returns slave name.
     *
     * @return slave name
     */
    String getName();

}
