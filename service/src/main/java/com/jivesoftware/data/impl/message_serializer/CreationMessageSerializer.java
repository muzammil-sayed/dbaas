package com.jivesoftware.data.impl.message_serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.data.exceptions.EncryptionException;
import com.jivesoftware.data.impl.CreationRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

public class CreationMessageSerializer {

    private final static Logger logger = LoggerFactory.getLogger(CreationMessageSerializer.class);

    private final ObjectMapper objectMapper;

    @Inject
    public CreationMessageSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public <T> String serialize(T message) {
        String jsonMessage;
        try {
            jsonMessage = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException jpe) {
            logger.error(String.format("Object mapper failed to convert to json"));
            throw new EncryptionException(jpe.getMessage());
        }
        return jsonMessage;
    }

    public <T> T deserialize(String decryptedMessage, Class<T> deserializeType) {

        try{
            return objectMapper.readValue(decryptedMessage, deserializeType);
        } catch (Exception e) {
            logger.error(String.format("Object mapper failed in decryption of message"));
            throw new EncryptionException(e.getMessage());
        }
    }
}
