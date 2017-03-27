package com.gmail.eksuzyan.pavel.concurrency.entities;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Request {

    private final Project project;
    private final long slaveId;
    private final int attempt;
    private final int code;
    private final LocalDateTime repeatDate;

    public Request(long slaveId, Project project) {
        this(slaveId, project, 1, 0);
    }

    public Request(Request request, int code) {
        this(request.getSlaveId(), request.getProject(), request.getAttempt() + 1, code);
    }

    private Request(long slaveId, Project project, int attempt, int code) {
        this.project = project;
        this.slaveId = slaveId;
        this.attempt = attempt;
        this.code = code;
        this.repeatDate = LocalDateTime.now().plus(1 << (attempt - 1), ChronoUnit.SECONDS);
    }

    public Project getProject() {
        return project;
    }

    public long getSlaveId() {
        return slaveId;
    }

    public LocalDateTime getRepeatTime() {
        return repeatDate;
    }

    public int getAttempt() {
        return attempt;
    }

    @Override
    public String toString() {
        return "Request{" +
                "project=" + project +
                ", id=" + slaveId +
                ", attempt=" + attempt +
                ", code=" + code +
                ", repeatDate=" + repeatDate +
                '}';
    }

}
