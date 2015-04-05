package com.lohika.morning.spark.presentation.spark.distributed.library.function.rdd.map;

import com.lohika.morning.spark.presentation.spark.distributed.library.type.Participant;
import org.apache.commons.lang.WordUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class FileContentToParticipantsFunction implements FlatMapFunction<Tuple2<String, String>, Participant> {

    private boolean includeOnlyPresentParticipants;

    public FileContentToParticipantsFunction(final boolean includeOnlyPresentParticipants) {
        this.includeOnlyPresentParticipants = includeOnlyPresentParticipants;
    }

    @Override
    public Iterable<Participant> call(Tuple2<String, String> filenameAndContent) throws Exception {
        StringTokenizer stringTokenizer = new StringTokenizer(filenameAndContent._2(), "\r");

        List<Participant> participants = new ArrayList<>();

        while (stringTokenizer.hasMoreElements()) {
            String participantRow = (String) stringTokenizer.nextElement();
            Participant participant = toParticipant(participantRow);

            if (includeOnlyPresentParticipants) {
                if (participant.getPresent()) {
                    participants.add(participant);
                }
            } else {
                participants.add(participant);
            }
        }

        return participants;
    }

    private Participant toParticipant(String participantRow) {
        String[] row = participantRow.split(",", -1);

        String firstName = "n/a";
        String lastName = "n/a";
        String companyName = "n/a";
        String position = "n/a";
        String email = "n/a";
        Boolean wasPresent = false;

        if (!StringUtils.isEmpty(row[0])) {
            firstName = WordUtils.capitalizeFully(row[0].trim());
        }

        if (!StringUtils.isEmpty(row[1])) {
            lastName = WordUtils.capitalizeFully(row[1].trim());
        }

        if (!StringUtils.isEmpty(row[2])) {
            companyName = WordUtils.capitalizeFully(row[2].trim());
        }

        if (!StringUtils.isEmpty(row[3])) {
            position = WordUtils.capitalizeFully(row[3].trim());
        }

        if (!StringUtils.isEmpty(row[4])) {
            email = row[4].trim();
        }

        if (!StringUtils.isEmpty(row[5])) {
            wasPresent = "1".equals(row[5].trim());
        }

        return new Participant(firstName, lastName, companyName, position, email, wasPresent);
    }

}

