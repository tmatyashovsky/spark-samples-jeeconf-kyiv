package com.lohika.morning.spark.presentation.api.controller;

import com.lohika.morning.spark.presentation.api.service.ConverterService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ConverterController {

    @Autowired
    private ConverterService converterService;

    @RequestMapping(value = "/convert", method = RequestMethod.GET)
    ResponseEntity<String> convertAllEventFilesToParquetFormat() {
        converterService.convertAllEventFilesToParquetFormat();

        return new ResponseEntity<>(HttpStatus.OK);
    }

}
