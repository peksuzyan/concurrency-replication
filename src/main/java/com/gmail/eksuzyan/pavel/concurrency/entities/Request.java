package com.gmail.eksuzyan.pavel.concurrency.entities;

import com.gmail.eksuzyan.pavel.concurrency.slave.Slave;

import java.util.Objects;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Request {

    public final Project project;
    public final Slave slave;
    private final int attempt;
    public final Code code;
    public final long repeatDate;

    public enum Code {
        UNDELIVERED,
        DELIVERED,
        REJECTED,
        OUTDATED
    }

    private static final int FIRST_ATTEMPT = 1;

    public Request(Slave slave, Project project) {
        this(slave, project, FIRST_ATTEMPT, Code.UNDELIVERED);
    }

    private Request(Slave slave, Project project, int attempt, Code code) {
        this.project = project;
        this.slave = slave;
        this.attempt = attempt;
        this.code = code;
        this.repeatDate = Long.sum(
                attempt > FIRST_ATTEMPT ? 1 << (attempt + 5) : 0L,
                System.currentTimeMillis());
    }

    public Request incAttemptAndReturn() {
        return new Request(slave, project, attempt + 1, Code.UNDELIVERED);
    }

    public Request setCodeAndReturn(Code code) {
        return new Request(slave, project, attempt, code);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Request)) return false;
        Request request = (Request) o;
        return Objects.equals(project, request.project) &&
                Objects.equals(slave, request.slave);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, slave);
    }

    @Override
    public String toString() {
        return "Request{" +
                "project=" + project +
                ", slave=" + slave.getName() +
                ", attempt=" + attempt +
                ", code=" + code +
                ", repeatDate=" + repeatDate +
                '}';
    }


}
