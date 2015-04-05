package com.lohika.morning.spark.presentation.spark.distributed.library.type;

import java.io.Serializable;

public class ParticipantsByCompany implements Serializable {

    private String companyName;
    private Long participantsCount;

    public ParticipantsByCompany(String companyName, Long participantsCount) {
        this.companyName = companyName;
        this.participantsCount = participantsCount;
    }

    public String getCompanyName() {
        return companyName;
    }

    public Long getParticipantsCount() {
        return participantsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParticipantsByCompany that = (ParticipantsByCompany) o;

        if (!companyName.equals(that.companyName)) return false;
        if (!participantsCount.equals(that.participantsCount)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = companyName.hashCode();
        result = 31 * result + participantsCount.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ParticipantsByCompany{" +
                "companyName='" + companyName + '\'' +
                ", participantsCount=" + participantsCount +
                '}';
    }
}
