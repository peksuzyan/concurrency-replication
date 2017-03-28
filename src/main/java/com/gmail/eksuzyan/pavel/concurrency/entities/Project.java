package com.gmail.eksuzyan.pavel.concurrency.entities;

import java.util.Objects;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Project {

    private final String id;
    private final long version;
    private final String data;

    public Project(String id, String data, long version) {
        this.id = id;
        this.version = version;
        this.data = data;
    }

    public Project(String id, String data) {
        this(id, data, 1L);
    }

    public Project setDataAndIncVersion(String data) {
        return new Project(id, data, version + 1L);
    }

    public String getId() {
        return id;
    }

    public long getVersion() {
        return version;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        return "Project{" +
                "id='" + id + '\'' +
                ", version=" + version +
                ", data='" + data + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Project)) return false;
        Project project = (Project) o;
        return version == project.version &&
                Objects.equals(id, project.id) &&
                Objects.equals(data, project.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, version, data);
    }
}
