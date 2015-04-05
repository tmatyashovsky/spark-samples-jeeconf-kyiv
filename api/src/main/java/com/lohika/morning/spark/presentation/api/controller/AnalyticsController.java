package com.lohika.morning.spark.presentation.api.controller;

import com.lohika.morning.spark.presentation.api.service.AnalyticsService;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.EventsByParticipant;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantEmailPosition;
import com.lohika.morning.spark.presentation.spark.distributed.library.type.ParticipantsByCompany;
import java.util.List;
import javax.annotation.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller that handles all requests to fetch valuable analytics from our system.
 *
 * @author taras.matyashovsky
 */
@RestController
public class AnalyticsController {

    @Resource(name = "${analyticsService}")
    private AnalyticsService analyticsService;

    @RequestMapping(value = "/participants-by-companies", method = RequestMethod.GET)
    ResponseEntity<List<ParticipantsByCompany>> getParticipantsByCompanies(
        @RequestParam(defaultValue = "false") boolean includeOnlyPresentParticipants) {
        List<ParticipantsByCompany> participantsByCompanies = analyticsService
            .getParticipantsByCompanies(includeOnlyPresentParticipants);

        if (!participantsByCompanies.isEmpty()) {
            return new ResponseEntity<>(participantsByCompanies, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/events-by-participants", method = RequestMethod.GET)
    ResponseEntity<List<EventsByParticipant>> getEventsByParticipants(
        @RequestParam(defaultValue = "false") boolean includeOnlyPresentParticipants) {
        List<EventsByParticipant> eventsByParticipants = analyticsService
            .getMostActiveParticipants(includeOnlyPresentParticipants);

        if (!eventsByParticipants.isEmpty()) {
            return new ResponseEntity<>(eventsByParticipants, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/participants", method = RequestMethod.GET)
    ResponseEntity<List<ParticipantEmailPosition>> getParticipantsByPosition(@RequestParam String position,
        @RequestParam(defaultValue = "false") boolean includeOnlyPresentParticipants) {
        List<ParticipantEmailPosition> participantsByPosition = analyticsService
            .getParticipantsByPosition(position, includeOnlyPresentParticipants);

        if (!participantsByPosition.isEmpty()) {
            return new ResponseEntity<>(participantsByPosition, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/participants-emails", method = RequestMethod.GET)
    ResponseEntity<List<String>> getUniqueParticipantsEmails(
        @RequestParam(defaultValue = "false") boolean includeOnlyPresentParticipants) {
        List<String> participantsEmails = analyticsService.getUniqueParticipantsEmails(
            includeOnlyPresentParticipants);

        if (!participantsEmails.isEmpty()) {
            return new ResponseEntity<>(participantsEmails, HttpStatus.OK);
        }

        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }

    @RequestMapping(value = "/participants-count", method = RequestMethod.GET)
    ResponseEntity<Long> getParticipantsCount(
            @RequestParam(defaultValue = "false") boolean includeOnlyPresentParticipants) {
        Long participantsCount = analyticsService.getParticipantsCount(
                includeOnlyPresentParticipants);

        return new ResponseEntity<>(participantsCount, HttpStatus.OK);
    }

}
