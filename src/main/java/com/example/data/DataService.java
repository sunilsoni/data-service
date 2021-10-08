package com.example.data;

import com.example.data.aws.AmazonS3Service;
import com.example.data.config.SourceData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
@Component
public class DataService {

    int queueSize;
    int poolSize;
    private AmazonS3Service amazonS3Service;
    private HBaseClient HBaseClient;
    private String date;
    private String emailRecipients;
    private String localDataDir;
    private ObjectMapper objectMapper;
    private DateTimeFormatter printFormat = DateTimeFormat.fullDateTime().withZone(DateTimeZone.forID("EST5EDT"));

    public DataService(AmazonS3Service amazonS3Service, HBaseClient HBaseClient, @Value("${queue.size:4}") int queueSize, @Value("${pool.size:2}") int poolSize, @Value("${email.recipients:a@test.com}") String emailRecipients, @Value("${local.data.dir}") String localDataDir, ObjectMapper objectMapper, String date) {
        this.amazonS3Service = amazonS3Service;
        this.HBaseClient = HBaseClient;
        this.queueSize = queueSize;
        this.poolSize = poolSize;
        this.emailRecipients = emailRecipients;
        this.localDataDir = localDataDir;
        this.objectMapper = new ObjectMapper();
        this.date = date;
    }

    public void start(long startTime, long endTime) throws IOException {
        log.info(String.format("Started with startTime: %s and endTime: %s", printFormat.print(startTime), printFormat.print(endTime)));

        log.info(this.getClass().getSimpleName() + "\tStopped: " + new Date());
    }

    private class Worker<T extends BaseDto> extends Thread {

        private SourceData source;
        private Class dtoClass;
        private RepoDetails repoDetails;
        private long startTime;
        private long endTime;
        private List<String> users;
        private Map<String, Field> fields;
        private File outputFile;


        public Worker(SourceData source, long startTime, long endTime, Class<T> dtoClass, List<String> users) throws IOException {
            this.source = source;
            this.startTime = startTime;
            this.endTime = endTime;
            this.dtoClass = dtoClass;
            this.repoDetails = (RepoDetails) this.dtoClass.getAnnotation(RepoDetails.class);
            this.users = users;
            this.outputFile = new File(localDataDir
                    + File.separator + this.repoDetails.dbView() + "-" + date + "-" + this.source.getName() + ".csv");
            this.fields = DtoToCsv.getFields(dtoClass);

            Files.append(DtoToCsv.getColumnNamesCsv(fields) + "\n", outputFile, Charsets.UTF_8);
        }

        public void run() {
            log.info(String.format("Starting to process data for the source: %s, startTime: %s and dbView: %s",
                    source.getName(), new Date(startTime).toString(), repoDetails.dbView()));
            long recordCount = 0L;
            try {
                while (this.startTime < this.endTime) {
                    recordCount += process(this.startTime);
                    this.startTime += 3600000L;
                }
            } catch (Exception e) {
                log.error("Error processing " + source.getName() + "_" + dtoClass.getSimpleName(), e);
            }

            log.info(String.format("Data Processing Finished  for the source: %s and dbView: %s with %d records",
                    source.getName(), repoDetails.dbView(), recordCount));

            amazonS3Service.uploadFileToS3Bucket(outputFile.getName(), repoDetails.s3Dir(), outputFile);
        }

        private int process(Long startTime) throws IOException {

            return 0;
        }
    }
}

