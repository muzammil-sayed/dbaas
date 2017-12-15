package com.jivesoftware.data.impl.message_serializer;

import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.exceptions.EncryptionException;
import com.jivesoftware.data.impl.CreationRequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import javax.inject.Inject;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.AlgorithmParameters;
import java.security.spec.KeySpec;
import java.util.Optional;

public class EncryptionManager {

    private final static Logger logger = LoggerFactory.getLogger(EncryptionManager.class);

    static final String INITIALIZATION_VECTOR = "iv";
    private final static int DERIVATION_ITERATION_COUNT = 65536;
    private final static int ENCRYPTION_BITS = 256;

    private String aesPassword;
    private String aesSalt;

    @Inject
    public EncryptionManager(DBaaSConfiguration dBaaSConfiguration) {

        AESValues aesValues;
        String aesFile = dBaaSConfiguration.getAesFile();

        try {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            aesValues = objectMapper.readValue(new File(aesFile),
                    AESValues.class);
            this.aesPassword = aesValues.getPassword();
            this.aesSalt = aesValues.getSalt();
        }
        catch (Exception e) {
            logger.error(String.format("Error reading instance crypto file %s", aesFile), e);
            this.aesPassword = null;
            this.aesSalt = null;
        }
    }

    public EncryptionObject encrypt(String serializedMessage) {
        if(aesPassword == null || aesSalt == null){
            throw new EncryptionException("AES password and salt uninitialized on startup. Encryption impossible");
        }
        try{
            Cipher cipher = initializeCipher(Optional.empty());

            AlgorithmParameters params = cipher.getParameters();
            byte[] iv = params.getParameterSpec(IvParameterSpec.class).getIV();
            byte[] encrypted = cipher.doFinal(serializedMessage.getBytes());

            return new EncryptionObject(iv, encrypted);
        } catch (Exception e) {
            throw new EncryptionException(String.format(e.getMessage()));
        }
    }

    public String decrypt(EncryptionObject encryptedMessage) {
        if(aesPassword == null || aesSalt == null){
            throw new EncryptionException("AES password and salt uninitialized on startup. Encryption impossible");
        }
        byte[] iv = encryptedMessage.getIV();
        byte[] encryptedBody = encryptedMessage.getEncryptedMessage();
        try{
            Cipher cipher = initializeCipher(Optional.of(iv));
            return new String(cipher.doFinal(encryptedBody), "UTF-8");
        } catch (Exception e) {
            throw new EncryptionException(String.format(e.getMessage()));
        }

    }

    @SuppressWarnings("unused")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class AESValues {

        private String password;
        private String salt;

        @JsonProperty
        public String getPassword(){
            return password;
        }

        @JsonProperty
        public String getSalt(){
            return salt;
        }
    }

    protected Cipher initializeCipher(Optional<byte[]> iv) {
        try{
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            KeySpec keySpec = new PBEKeySpec(this.aesPassword.toCharArray(),
                    this.aesSalt.getBytes(), DERIVATION_ITERATION_COUNT, ENCRYPTION_BITS);
            SecretKey secret = new SecretKeySpec((factory.generateSecret(keySpec).getEncoded()), "AES");

            Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5PADDING");
            if(iv.isPresent()){
                cipher.init(Cipher.DECRYPT_MODE, secret, new IvParameterSpec(iv.get()));
            }
            else {
                cipher.init(Cipher.ENCRYPT_MODE, secret);
            }
            return cipher;
        } catch (Exception e) {
            throw new EncryptionException(String.format(e.getMessage()));
        }
    }
}
