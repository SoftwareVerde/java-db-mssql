package com.softwareverde.database.mssql.row;

import org.junit.Test;

import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class MssqlRowTest {

    private static final String COLUMN_TEST_VALUE = "col1";
    private static final String COLUMN_T = "col2";
    private static final String COLUMN_1 = "col3";
    private static final String COLUMN_TRUE = "col4";
    private static final String COLUMN_F = "col5";
    private static final String COLUMN_0 = "col6";
    private static final String COLUMN_FALSE = "col7";
    private static final String COLUMN_UNICODE_1234 = "col8";
    private static final String COLUMN_YES = "col9";
    private static final String COLUMN_Y = "col10";
    private static final String COLUMN_NO = "col11";
    private static final String COLUMN_N = "col12";

    private MssqlRow createTestRow() {
        MssqlRow row = new MssqlRow();
        row._columnValues.put(COLUMN_TEST_VALUE, "test value");
        row._columnValues.put(COLUMN_T, "t");
        row._columnValues.put(COLUMN_1, "1");
        row._columnValues.put(COLUMN_TRUE, "true");
        row._columnValues.put(COLUMN_F, "f");
        row._columnValues.put(COLUMN_0, "0");
        row._columnValues.put(COLUMN_FALSE, "false");
        row._columnValues.put(COLUMN_UNICODE_1234, "\u1234");
        row._columnValues.put(COLUMN_YES, "yes");
        row._columnValues.put(COLUMN_Y, "y");
        row._columnValues.put(COLUMN_NO, "no");
        row._columnValues.put(COLUMN_N, "n");
        return row;
    }

    @Test
    public void testGetString() {
        MssqlRow row = createTestRow();
        assertEquals("test value", row.getString(COLUMN_TEST_VALUE));
        assertEquals("t", row.getString(COLUMN_T));
        assertEquals("1", row.getString(COLUMN_1));
        assertEquals("true", row.getString(COLUMN_TRUE));
        assertEquals("f", row.getString(COLUMN_F));
        assertEquals("0", row.getString(COLUMN_0));
        assertEquals("false", row.getString(COLUMN_FALSE));
        assertEquals("\u1234", row.getString(COLUMN_UNICODE_1234));
        assertEquals("yes", row.getString(COLUMN_YES));
        assertEquals("y", row.getString(COLUMN_Y));
        assertEquals("no", row.getString(COLUMN_NO));
        assertEquals("n", row.getString(COLUMN_N));
    }

    @Test
    public void testGetBoolean() {
        MssqlRow row = createTestRow();
        assertEquals(false, row.getBoolean(COLUMN_TEST_VALUE));
        assertEquals(true, row.getBoolean(COLUMN_T));
        assertEquals(true, row.getBoolean(COLUMN_1));
        assertEquals(true, row.getBoolean(COLUMN_TRUE));
        assertEquals(false, row.getBoolean(COLUMN_F));
        assertEquals(false, row.getBoolean(COLUMN_0));
        assertEquals(false, row.getBoolean(COLUMN_FALSE));
        assertEquals(false, row.getBoolean(COLUMN_UNICODE_1234));
        assertEquals(true, row.getBoolean(COLUMN_YES));
        assertEquals(true, row.getBoolean(COLUMN_Y));
        assertEquals(false, row.getBoolean(COLUMN_NO));
        assertEquals(false, row.getBoolean(COLUMN_N));
    }

    @Test
    public void testGetBytes() {
        MssqlRow row = createTestRow();
        assertTrue(Arrays.equals("test value".getBytes(), row.getBytes(COLUMN_TEST_VALUE)));
        assertTrue(Arrays.equals("t".getBytes(), row.getBytes(COLUMN_T)));
        assertTrue(Arrays.equals("1".getBytes(), row.getBytes(COLUMN_1)));
        assertTrue(Arrays.equals("true".getBytes(), row.getBytes(COLUMN_TRUE)));
        assertTrue(Arrays.equals("f".getBytes(), row.getBytes(COLUMN_F)));
        assertTrue(Arrays.equals("0".getBytes(), row.getBytes(COLUMN_0)));
        assertTrue(Arrays.equals("false".getBytes(), row.getBytes(COLUMN_FALSE)));
        //                       UTF-8 bytes for \u1234
        assertTrue(Arrays.equals(new byte[]{(byte)0xE1, (byte)0x88, (byte)0xB4}, row.getBytes(COLUMN_UNICODE_1234)));
        assertTrue(Arrays.equals("yes".getBytes(), row.getBytes(COLUMN_YES)));
        assertTrue(Arrays.equals("y".getBytes(), row.getBytes(COLUMN_Y)));
        assertTrue(Arrays.equals("no".getBytes(), row.getBytes(COLUMN_NO)));
        assertTrue(Arrays.equals("n".getBytes(), row.getBytes(COLUMN_N)));
    }
}
