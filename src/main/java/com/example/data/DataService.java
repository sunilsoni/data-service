package com.example.data;

import com.example.data.aws.AmazonS3Service;
import com.example.data.config.SourceData;
import com.example.data.model.BaseModel;
import com.example.data.model.UserActivityDetails;
import com.example.data.util.DatabaseDetails;
import com.example.data.util.ModelToCsv;
import com.example.data.util.NotifyingBlockingExecutor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
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
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class DataService {
    int queueSize;
    int poolSize;
    private final AmazonS3Service amazonS3Service;
    private final HBaseClient HBaseClient;
    private String date;
    private final String emailRecipients;
    private final String localDataDir;
    private final ObjectMapper objectMapper;
    private final DateTimeFormatter printFormat;

    public DataService(AmazonS3Service amazonS3Service, HBaseClient HBaseClient, @Value("${queue.size:4}") int queueSize, @Value("${pool.size:2}") int poolSize, @Value("${email.recipients:a@test.com}") String emailRecipients, @Value("${local.data.dir}") String localDataDir, ObjectMapper objectMapper, String date) {
        this.amazonS3Service = amazonS3Service;
        this.HBaseClient = HBaseClient;
        this.queueSize = queueSize;
        this.poolSize = poolSize;
        this.emailRecipients = emailRecipients;
        this.localDataDir = localDataDir;
        this.objectMapper = new ObjectMapper();
        this.date = date;
        printFormat = DateTimeFormat.fullDateTime().withZone(DateTimeZone.forID("EST5EDT"));
    }

    public void start(long startTime, long endTime) throws IOException {
        log.info(String.format("Started with startTime: %s and endTime: %s", printFormat.print(startTime), printFormat.print(endTime)));
        List<String> users = HBaseClient.getUsers();
        DateTimeFormatter format = DateTimeFormat.forPattern("yyyy_MM_dd");
        this.date = format.print(startTime);

        FileUtils.cleanDirectory(new File(this.localDataDir));

        ThreadPoolExecutor threadPoolExecutor = new NotifyingBlockingExecutor(poolSize, queueSize, Integer.MAX_VALUE,
                TimeUnit.HOURS, Integer.MAX_VALUE, TimeUnit.HOURS, new BlockingTimeoutCallback());

        List<SourceData> sources = objectMapper.readValue(Resources.getResource("sources.json"), typeRef);

        for (SourceData source : sources) {
            threadPoolExecutor.execute(new SingleProcessingWorker(source, startTime, endTime, UserActivityDetails.class, users));
        }

        try {
            if (threadPoolExecutor != null) {
                threadPoolExecutor.shutdown();
                threadPoolExecutor.awaitTermination(2L, TimeUnit.HOURS);
            }
        } catch (InterruptedException e) {
            log.error("threadPoolExecutor Interrupted", e);
        }
        log.info(this.getClass().getSimpleName() + "\tProcessing Stopped: " + new Date());
    }

    private class SingleProcessingWorker<T extends BaseModel> extends Thread {

        private final SourceData source;
        private final Class dtoClass;
        private final DatabaseDetails repoDetails;
        private long startTime;
        private final long endTime;
        private final List<String> users;
        private final Map<String, Field> fields;
        private final File outputFile;


        public SingleProcessingWorker(SourceData source, long startTime, long endTime, Class<T> dtoClass, List<String> users) throws IOException {
            this.source = source;
            this.startTime = startTime;
            this.endTime = endTime;
            this.dtoClass = dtoClass;
            this.repoDetails = (DatabaseDetails) this.dtoClass.getAnnotation(DatabaseDetails.class);
            this.users = users;
            this.outputFile = new File(localDataDir
                    + File.separator + this.repoDetails.dbView() + "-" + date + "-" + this.source.getName() + ".csv");
            this.fields = ModelToCsv.getFields(dtoClass);

            Files.append(ModelToCsv.getColumnNamesCsv(fields) + "\n", outputFile, Charsets.UTF_8);
        }

        public void run() {
            log.info(String.format("Starting to process data for the source: %s, startTime: %s and dbView: %s",
                    source.getName(), new Date(startTime), repoDetails.dbView()));
            long recordCount = 0L;
            try {
                while (this.startTime < this.endTime) {
                    recordCount += process(this.startTime);
                    this.startTime += 3600000L;
                }
            } catch (Exception e) {
                log.error("Error while processing " + source.getName() + "_" + dtoClass.getSimpleName(), e);
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

