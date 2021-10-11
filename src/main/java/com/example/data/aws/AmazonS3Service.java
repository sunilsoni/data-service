package com.example.data.aws;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.io.File;

@Slf4j
@Service
public class AmazonS3Service {

    @Value("${aws.s3.data.bucket}")
    private String awsS3CcDataBucket;

    private final AmazonS3 amazonS3;

    @Autowired
    public AmazonS3Service(Region awsRegion, AWSCredentialsProvider awsCredentialsProvider) {
        this.amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(awsCredentialsProvider)
                .withRegion(awsRegion.getName()).build();
    }

    @Async
    public void uploadFileToS3Bucket(String path, String s3Dir, File file) {
        log.info(String.format("Uploading file %s to S3 as %s", file.getName(), s3Dir + File.separator + path));
        PutObjectRequest putObjectRequest = new PutObjectRequest(this.awsS3CcDataBucket, s3Dir + File.separator + path, file);
        this.amazonS3.putObject(putObjectRequest);
    }
}
