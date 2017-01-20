package com.lohika.morning.spark.presentation.spark.distributed.library.type;

import java.io.Serializable;

public class ParticipantEmailPosition implements Serializable, Comparable<ParticipantEmailPosition> {

    private String email;
    private String position;

    public ParticipantEmailPosition() {
    }

    public ParticipantEmailPosition(String email, String position) {
        this.email = email;
        this.position = position;
    }

    public String getEmail() {
        return email;
    }

    public String getPosition() {
        return position;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParticipantEmailPosition that = (ParticipantEmailPosition) o;

        if (email != null ? !email.equals(that.email) : that.email != null) return false;
        if (position != null ? !position.equals(that.position) : that.position != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = email != null ? email.hashCode() : 0;
        result = 31 * result + (position != null ? position.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(ParticipantEmailPosition otherParticipantEmailPosition) {
        int emailComparisonResult = this.getEmail().compareTo(otherParticipantEmailPosition.getEmail());

        if (emailComparisonResult != 0) {
            return emailComparisonResult;
        }

        return this.getPosition().compareTo(otherParticipantEmailPosition.getPosition());
    }

    @Override
    public String toString() {
        return "ParticipantEmailPosition{" +
                "email='" + email + '\'' +
                ", position='" + position + '\'' +
                '}';
    }
}
