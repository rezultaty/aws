package com.prokopczyk;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.google.common.base.Throwables;
import org.imgscalr.Scalr;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by andrzej on 15.01.2016.
 */
public class ImagesManipulator {

    public static final String DIRECTORY = "andrzej.prokopczyk/";
    private final AmazonS3Client amazonS3Client;
    private final String bucketName = "lab4-weeia";


    public ImagesManipulator(DefaultAWSCredentialsProviderChain credentials) {
        this.amazonS3Client = new AmazonS3Client(credentials);
    }


    public void processImage(String imageName) {


        S3Object object = amazonS3Client.getObject(bucketName, DIRECTORY + imageName);
        try {
            byte[] bytes = IOUtils.toByteArray(object.getObjectContent());


            InputStream in = new ByteArrayInputStream(bytes);
            BufferedImage bImageFromConvert = ImageIO.read(in);

            CompletableFuture<BufferedImage>[] features = new CompletableFuture[java.lang.Runtime.getRuntime().availableProcessors()];
            for (int i = 0; i < java.lang.Runtime.getRuntime().availableProcessors(); i++) {
                features[i] = CompletableFuture.supplyAsync(() -> {
                    return generateScaledImage(bytes, bImageFromConvert);
                });
            }


            CompletableFuture<Object> objectCompletableFuture = CompletableFuture.anyOf(features);

            objectCompletableFuture.thenAccept(e -> {
                BufferedImage image = (BufferedImage) e;
                // convert BufferedImage to byte array
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    ImageIO.write(image, "jpg", baos);
                    baos.flush();
                } catch (IOException e1) {
                    Throwables.propagate(e1);
                }


                byte[] buf = baos.toByteArray();
                InputStream str = new ByteArrayInputStream(buf);
                ObjectMetadata metadata = new ObjectMetadata();
                metadata.setContentLength(buf.length);
                amazonS3Client.putObject(bucketName, DIRECTORY + LocalDateTime.now() + ".jpg", str, metadata);


            });

            objectCompletableFuture.get();

        } catch (IOException e) {
            Throwables.propagate(e);
        } catch (InterruptedException e) {
            Throwables.propagate(e);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
        }

    }

    private BufferedImage generateScaledImage(byte[] bytes, BufferedImage bImageFromConvert) {
        BufferedImage buffImg = null;
        for (int i = 0; i < 10000; i++) {
            Image img = null;
            try {
                img = ImageIO.read(new ByteArrayInputStream(bytes)).getScaledInstance(300, 300, BufferedImage.SCALE_SMOOTH);
            } catch (IOException e) {
                Throwables.propagate(e);
            }
            BufferedImage buffered = new BufferedImage(300, 300, BufferedImage.SCALE_FAST);
            buffered.getGraphics().drawImage(img, 0, 0, null);
            buffImg = Scalr.resize(bImageFromConvert, Scalr.Method.ULTRA_QUALITY, 700);
        }
        return buffImg;
    }


}
