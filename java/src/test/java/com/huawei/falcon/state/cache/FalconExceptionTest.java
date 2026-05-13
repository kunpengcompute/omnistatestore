package com.huawei.falcon.state.cache;

import static org.junit.Assert.*;

import org.junit.Test;
import org.mockito.Mockito;
import org.rocksdb.Status;

public class FalconExceptionTest {

    private static final String TEST_ERROR_MSG = "test falcon exception message";
    private static final String TEST_STATE = "test state";
    private static final String TEST_CODE_STRING = "test code string";

    @Test
    public void testFalconException_OnlyMessage() {
        FalconException exception = new FalconException(TEST_ERROR_MSG);
        assertEquals("Message should match the constructor argument", TEST_ERROR_MSG, exception.getMessage());
        assertNull("Status should be null", exception.getStatus());
    }

    @Test
    public void testFalconException_MessageAndStatus() {
        Status mockStatus = Mockito.mock(Status.class);
        Mockito.when(mockStatus.getState()).thenReturn(TEST_STATE);
        Mockito.when(mockStatus.getCodeString()).thenReturn(TEST_CODE_STRING);

        FalconException exception = new FalconException(TEST_ERROR_MSG, mockStatus);

        assertEquals("Message should match the constructor argument", TEST_ERROR_MSG, exception.getMessage());
        assertEquals("Status object should match", mockStatus, exception.getStatus());
    }

    @Test
    public void testFalconException_OnlyStatus_WithState() {
        Status mockStatus = Mockito.mock(Status.class);
        Mockito.when(mockStatus.getState()).thenReturn(TEST_STATE);
        Mockito.when(mockStatus.getCodeString()).thenReturn(TEST_CODE_STRING);

        FalconException exception = new FalconException(mockStatus);

        assertEquals("Message should use state value when state is not null", TEST_STATE, exception.getMessage());
        assertEquals("Status object should match", mockStatus, exception.getStatus());
    }

    @Test
    public void testFalconException_OnlyStatus_StateNull() {
        Status mockStatus = Mockito.mock(Status.class);
        Mockito.when(mockStatus.getState()).thenReturn(null);
        Mockito.when(mockStatus.getCodeString()).thenReturn(TEST_CODE_STRING);

        FalconException exception = new FalconException(mockStatus);

        assertEquals(
            "Message should fall back to codeString when state is null",
            TEST_CODE_STRING,
            exception.getMessage()
        );
        assertEquals("Status object should match", mockStatus, exception.getStatus());
    }

    @Test
    public void testFalconException_OnlyStatus_StateEmptyString() {
        Status mockStatus = Mockito.mock(Status.class);
        Mockito.when(mockStatus.getState()).thenReturn("");
        Mockito.when(mockStatus.getCodeString()).thenReturn(TEST_CODE_STRING);

        FalconException exception = new FalconException(mockStatus);

        assertEquals(
            "Message should use empty string state rather than falling back to codeString",
            "",
            exception.getMessage()
        );
        assertEquals("Status object should match", mockStatus, exception.getStatus());
    }

    @Test
    public void testFalconException_NullMessageAndNullStatus() {
        FalconException exception = new FalconException(null, null);

        assertNull("Message should be null", exception.getMessage());
        assertNull("Status should be null", exception.getStatus());
    }

    @Test
    public void testFalconException_EmptyMessageAndNullStatus() {
        FalconException exception = new FalconException("", null);

        assertEquals("Message should be empty string", "", exception.getMessage());
        assertNull("Status should be null", exception.getStatus());
    }
}
