package com.gmail.eksuzyan.pavel.concurrency;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Project {

    private final String id;
    private final long version;
    private String data;

    public Project(String id, String data, long version) {
        this.id = id;
        this.version = version;
        this.data = data;
    }

    public Project(String id, String data) {
        this(id, data, 1L);
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
}