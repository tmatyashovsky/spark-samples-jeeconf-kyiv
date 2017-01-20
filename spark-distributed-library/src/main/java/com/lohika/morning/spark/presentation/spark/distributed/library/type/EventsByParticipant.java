package com.lohika.morning.spark.presentation.spark.distributed.library.type;

import java.io.Serializable;

public class EventsByParticipant implements Serializable {

    private String email;
    private Long eventsCount;

    public EventsByParticipant() {
    }

    public EventsByParticipant(String email, Long eventsCount) {
        this.email = email;
        this.eventsCount = eventsCount;
    }

    public String getEmail() {
        return email;
    }

    public Long getEventsCount() {
        return eventsCount;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setEventsCount(Long eventsCount) {
        this.eventsCount = eventsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        EventsByParticipant that = (EventsByParticipant) o;

        if (!email.equals(that.email)) return false;
        if (!eventsCount.equals(that.eventsCount)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = email.hashCode();
        result = 31 * result + eventsCount.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "EventsByParticipant{" +
                "email='" + email + '\'' +
                ", eventsCount=" + eventsCount +
                '}';
    }

}
