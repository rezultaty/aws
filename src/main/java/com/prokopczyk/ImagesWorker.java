package com.prokopczyk;


import com.google.common.base.Throwables;

/**
 * Created by andrzej on 15.01.2016.
 */
public class ImagesWorker {


    public static void main(String[] args) {
        final Thread listener = new Thread(new Runnable() {
            public void run() {
                SqsListener sqsListener= new SqsListener();
                try {
                    sqsListener.listen();
                } catch (InterruptedException e) {
                    Throwables.propagate(e);
                }

            }
        });
        //listener.setDaemon(true);
        listener.start();
    }
}
