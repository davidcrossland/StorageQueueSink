package com.elastacloud.spark;

import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.queue.client.CloudQueue;
import com.microsoft.windowsazure.services.queue.client.CloudQueueClient;
import com.microsoft.windowsazure.services.queue.client.CloudQueueMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;


/**
 * Created by david on 08/04/14.
 */
public class StorageQueueWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageQueueWriter.class);
    private  CloudQueue queue = null;

    public StorageQueueWriter(String connectionString, String queueName)
    {
        try {
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
            LOGGER.trace("Creating storage account connection");
            // Create the queue client
            CloudQueueClient queueClient = storageAccount.createCloudQueueClient();
            queue = queueClient.getQueueReference(queueName);

            // Create the queue if it doesn't already exist
            queue.createIfNotExist();

        } catch (Exception e) {
            e.printStackTrace();
        }


    }
    public void sendMessage(String msg) throws Exception {
        if(queue == null)
            throw new Exception("Cannot send message, connection is null");
        try
        {
            LOGGER.trace("Sending message: " + msg);
            CloudQueueMessage message = new CloudQueueMessage(msg);
            queue.addMessage(message);
        }
        catch(Exception e)
        {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
