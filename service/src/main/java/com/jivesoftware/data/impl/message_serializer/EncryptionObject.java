package com.jivesoftware.data.impl.message_serializer;

public class EncryptionObject {

    private byte[] iv;
    private byte[] encryptedMessage;

    public EncryptionObject(byte[] iv, byte[] encryptedMessage){
        this.iv = iv;
        this.encryptedMessage = encryptedMessage;
    }

    public byte[] getIV() {
        return iv;
    }

    public byte[] getEncryptedMessage() {
        return encryptedMessage;
    }
}
