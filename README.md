# Amazon S3 (Simple Storage Service)

S3 has a very simple structure – each bucket can store any number of objects which can be accessed using either a SOAP interface or an REST-style API.

We'll use the AWS SDK for Java to create, list, and delete S3 buckets. We'll also upload, list, download, copy, move, rename and delete objects within these buckets.

# Prerequisites

To use AWS SDK, we'll need a few things:

**AWS Account:** we need an Amazon Web Services account. If you still don't have any, go ahead and create an account

**AWS Security Credentials:** These are our access keys that allow us to make programmatic calls to AWS API actions. We can get these credentials in two ways, either by using AWS root account credentials from access keys section of Security Credentials page or by using IAM user credentials from IAM console

**Choosing AWS Region:** We have to select an AWS region(s) where we want to store our Amazon S3 data. Keep in mind that S3 storage prices vary by region. For more details, head over to the official documentation. For this tutorial, we'll use US East (Ohio) (region us-east-2)



**Reference:** 

1. https://www.baeldung.com/aws-s3-java











