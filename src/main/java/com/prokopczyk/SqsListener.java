package com.prokopczyk;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.google.common.base.Throwables;
import org.joda.time.DateTime;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by andrzej on 15.01.2016.
 */
public class SqsListener {

    public static final String TEST_PROKOPCZYK = "testProkopczyk";
    public static final String DATE = "Date";
    private static final String ITEM_NAME = "workItem";
    public static final String HTTPS_SQS_US_WEST_2_AMAZONAWS_COM = "https://sqs.us-west-2.amazonaws.com/";
    public static final String ITEM = "Item";
    private final String queName;
    private final AmazonSQSClient sqs;
    private final ImagesManipulator imagesManipulator;
    private final AmazonSimpleDBClient simpleDBClinet;

    public SqsListener() {


        this.queName = "prokopczykSQS";
        this.sqs = new AmazonSQSClient(new DefaultAWSCredentialsProviderChain());
        this.sqs.setEndpoint(HTTPS_SQS_US_WEST_2_AMAZONAWS_COM);
        imagesManipulator = new ImagesManipulator(new DefaultAWSCredentialsProviderChain());
        this.simpleDBClinet = new AmazonSimpleDBClient(new DefaultAWSCredentialsProviderChain());
        simpleDBClinet.createDomain(new CreateDomainRequest(TEST_PROKOPCZYK));
    }


    public void listen() throws InterruptedException {
        while (true) {


            List<Message> messagesFromQueue = getMessagesFromQueue(getQueueUrl(this.queName));
            if (messagesFromQueue.size() > 0) {
                Message message = messagesFromQueue.get(0);
                deleteMessageFromQueue(getQueueUrl(this.queName), message);

                List<ReplaceableAttribute> attributes = new ArrayList<>();
                attributes.add(new ReplaceableAttribute().withName(ITEM).withValue(message.getBody()));
                attributes.add(new ReplaceableAttribute().withName(DATE).withValue(DateTime.now().toString()));
                simpleDBClinet.putAttributes(new PutAttributesRequest(TEST_PROKOPCZYK, ITEM_NAME, attributes));
                imagesManipulator.processImage(message.getBody());
                System.out.println(message.getBody());

            } else {
                Thread.sleep(2000);

            }


        }


    }


    private List<Message> getMessagesFromQueue(String queueUrl) {

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);

        List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();

        return messages;

    }


    private String getQueueUrl(String queueName) {
        GetQueueUrlRequest getQueueUrlRequest = new GetQueueUrlRequest(queueName);
        return this.sqs.getQueueUrl(getQueueUrlRequest).getQueueUrl();
    }

    private void deleteMessageFromQueue(String queueUrl, Message message) {
        String messageRecieptHandle = message.getReceiptHandle();
        //System.out.println("message deleted : " + message.getBody() + "." + message.getReceiptHandle());
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));

    }


}
