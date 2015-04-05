package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.filter;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;

public class FilterParticipantByPosition implements Function<Participant, Boolean> {

    private String position;

    public FilterParticipantByPosition(final String position) {
        this.position = position;
    }

    @Override
    public Boolean call(final Participant participant) throws Exception {
        return StringUtils.containsIgnoreCase(participant.getPosition(), position);
    }

}
