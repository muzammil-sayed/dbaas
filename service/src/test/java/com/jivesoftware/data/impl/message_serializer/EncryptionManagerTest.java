package com.jivesoftware.data.impl.message_serializer;

import com.jivesoftware.data.DBaaSConfiguration;
import com.jivesoftware.data.impl.CreationRequestMessage;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(DataProviderRunner.class)
public class EncryptionManagerTest {
    private EncryptionManager encryptionManager;

    @Mock
    private EncryptionObject encryptionObject;

    @Mock
    private CreationRequestMessage creationRequestMessage;

    @Mock
    private DBaaSConfiguration dBaaSConfiguration;

    private boolean mockInitialized = false;

    byte[] ivBytes;
    byte[] messageBytes;

    @DataProvider
    public static Object[][] data() {
        return new Object[][] {
                {"simpleString", "simpleString"},
                {"Letters and spaces", "Letters and spaces"},
                {"PUNCTUATION!>@,", "PUNCTUATION!>@,"},
                {"1337NumS", "1337NumS"},
                {"under_score", "under_score"},
                {"1234567890", "1234567890"},
                {"on-dasher", "on-dasher"},
                {"- -", "- -"},
                {"(╯°□°）╯︵ ┻━┻", "(╯°□°）╯︵ ┻━┻"},
                {"┬─┬ノ( º _ ºノ)", "┬─┬ノ( º _ ºノ)"},
                {"", ""},
                {".", "."}
        };
    }

    @Before
    public void setUp() {
        if(!mockInitialized) {
            MockitoAnnotations.initMocks(this);
            mockInitialized = true;
        }

        ivBytes = new byte[0];
        messageBytes = new byte[0];

        when(dBaaSConfiguration.getAesFile()).thenReturn("src/test/resources/aes-test.yaml");

        encryptionManager = new EncryptionManager(dBaaSConfiguration);
    }

    @Test
    @UseDataProvider("data")
    public void encryptionTest(final String original, final String control) throws Exception {
        EncryptionObject testEncrypted = encryptionManager.encrypt(original);
        assertEquals(encryptionManager.decrypt(testEncrypted), control);
    }
}
