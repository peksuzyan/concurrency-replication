package com.gmail.eksuzyan.pavel.concurrency.entities;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

/**
 * @author Pavel Eksuzian.
 *         Created: 12.03.2017.
 */
public class Request {

    public final Project project;
    public final String slave;
    public final int attempt;
    public final int code;
    public final LocalDateTime repeatDate;

    public Request(String slave, Project project) {
        this(slave, project, 1, 0);
    }

    private Request(String slave, Project project, int attempt, int code) {
        this.project = project;
        this.slave = slave;
        this.attempt = attempt;
        this.code = code;
        this.repeatDate = LocalDateTime.now().plus(1 << (attempt - 1), ChronoUnit.SECONDS);
    }

    public Request setCodeAndIncAttempt(int code) {
        return new Request(slave, project, attempt + 1, code);
    }

    @Override
    public String toString() {
        return "Request{" +
                "project=" + project +
                ", slave='" + slave + '\'' +
                ", attempt=" + attempt +
                ", code=" + code +
                ", repeatDate=" + repeatDate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Request)) return false;
        Request request = (Request) o;
        return attempt == request.attempt &&
                code == request.code &&
                Objects.equals(project, request.project) &&
                Objects.equals(slave, request.slave) &&
                Objects.equals(repeatDate, request.repeatDate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, slave, attempt, code, repeatDate);
    }
}
