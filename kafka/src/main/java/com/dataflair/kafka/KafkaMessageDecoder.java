package com.dataflair.kafka;


import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.List;
import java.util.Map;

/**
 *
 * Utility class that contains logic to decode byte array into plain
 * java collections or String.
 */
@SuppressWarnings("unchecked")
public class KafkaMessageDecoder {

    /**
     * This method convert kafka message (byte[]) into list.
     *
     * @param message the message
     * @return the list
     */
    public List<Map<String, String> >decode(byte[] message) {

        try {
            // Parse byte array to Map
            ByteArrayInputStream bis = new ByteArrayInputStream(message);
            ObjectInputStream ois;
            ois = new ObjectInputStream(bis);
            List<Map<String, String>> input = (List<Map<String, String>>) ois.readObject();
            return input;
        } catch (Exception e) {
            throw new RuntimeException( "Unable to map kafka message to json/xml map.", e);
        }
    }
}