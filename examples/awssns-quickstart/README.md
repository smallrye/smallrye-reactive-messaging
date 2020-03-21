AWS SNS Quickstart
================

This project illustrates how you can interact with AWS Simple Notification Service (SNS) using MicroProfile Reactive Messaging.

## AWS Account creation

First you need to have an account on [AWS](https://console.aws.amazon.com). Then you will create an account that have a policy (AmazonSNSFullAccess).

## AWS CLI installation

Install [AWS-CLI](https://aws.amazon.com/cli/). CLI is helpful to access AWS services and configure your AWS credentials on your machine.
after installing CLI run :
```bash
aws configure
```
Then enter your your AWS Access Key ID & Secret Key.

Note: use this only for Dev/Testing.

## AWS-SDK credential provider

SNS Connector use [DefaultAWSCredentialsProviderChain](https://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/DefaultAWSCredentialsProviderChain.html). which looks for the credentials in the following order :
* Environment Variables - AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (RECOMMENDED since they are recognized by all the AWS SDKs and CLI except for .NET), or AWS_ACCESS_KEY and AWS_SECRET_KEY (only recognized by Java SDK)
* Java System Properties - aws.accessKeyId and aws.secretKey
* Credential profiles file at the default location (~/.aws/credentials) shared by all AWS SDKs and the AWS CLI
* Credentials delivered through the Amazon EC2 container service if AWS_CONTAINER_CREDENTIALS_RELATIVE_URI" environment variable is set and security manager has permission to access the variable.
* Instance profile credentials delivered through the Amazon EC2 metadata service
* Web Identity Token credentials from the environment or container.

## Use a Test/Fake SNS

If you don't have AWS account or you don't want to send messages to your AWS Account you can use [Fake SNS](https://hub.docker.com/r/s12v/sns/) for Development.
Start it with:

```bash
docker run -d -p 9911:9911 -v "$PWD":/etc/sns s12v/sns
```


After running the docker image configure these three properties as follow :
* sns-mock-sns-topics=true , if true it will trigger fake SNS functionality.
* sns-url=http://localhost:9911 , SNS URL exposed by docker container, this will only be considered in case of fake/mock SNS.
* sns-app-host=http://docker.for.mac.host.internal

Regarding sns-app-host value. this should be accessible by fake SNS container. so you need to configure your docker outbound connections
to access your local address. in case of Mac machines the above value works fine.

## Start the application

The application can be started using:

```bash
mvn package exec:java
```

Then, looking at the output you can see messages successfully send to and retrieved from a AWS SNS topic.

## Anatomy

In addition to the commandline output, the application is composed by 2 components:

* `BeanUsingAnEmitter` - a bean sending a changing test message to AWS SNS topic every 10 second.
* `Receiver`  - on the consuming side, the `Receiver` retrieves messages from a AWS SNS topic and writes the message content to `stdout`.

The interaction with AWS SNS is managed by MicroProfile Reactive Messaging.
The configuration is located in the microprofile config properties.
