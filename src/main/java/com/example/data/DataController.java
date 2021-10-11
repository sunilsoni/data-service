package com.example.data;


import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@Slf4j
@RestController
public class DataController {
    private final DataService dataService;
    private DateTimeFormatter dtf;

    public DataController(DataService dataService) {
        this.dataService = dataService;
        this.dtf = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.forID("EST5EDT"));
    }

    @RequestMapping(value = "/data-service", method = RequestMethod.GET)
    public ResponseEntity<Object> runOnDemand(@RequestParam(value = "date", required = true) String date) {
        if (!CommonUtil.isValidDate(date)) {
            return new ResponseEntity<>("Invalid date format. Should be in yyyy-MM-dd format.", HttpStatus.BAD_REQUEST);
        }
        long startTime = dtf.parseMillis(date);
        long endTime = new DateTime(dtf.parseMillis(date)).plusDays(1).minusMillis(1).getMillis();

        try {
            dataService.start(startTime, endTime);
            return new ResponseEntity<>(HttpStatus.OK);
        } catch (IOException e) {
            log.error("On-demand run might have failed.", e);
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
