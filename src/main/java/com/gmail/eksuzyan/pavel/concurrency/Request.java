package com.gmail.eksuzyan.pavel.concurrency;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Request {

    private final Project project;
    private int code;
    private final long slaveId;

    public Request(long slaveId, Project project) {
        this.slaveId = slaveId;
        this.project = project;
    }

    public Project getProject() {
        return project;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }
}
