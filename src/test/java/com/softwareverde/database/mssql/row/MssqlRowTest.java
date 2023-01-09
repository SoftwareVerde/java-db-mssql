package com.softwareverde.database.mssql.row;

import com.softwareverde.database.jdbc.row.JdbcRow;
import com.softwareverde.database.query.parameter.TypedParameter;
import com.softwareverde.util.HexUtil;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class MssqlRowTest {
    private static final String BYTES_HEX = "00102030405060708090A0B0C0D0E0F0FF";
    private static final byte[] BYTES = HexUtil.hexStringToByteArray(BYTES_HEX);

    private static final String COLUMN_TEST_VALUE = "col1";
    private static final String COLUMN_T = "col2";
    private static final String COLUMN_1 = "col3";
    private static final String COLUMN_TRUE = "col4";
    private static final String COLUMN_F = "col5";
    private static final String COLUMN_0 = "col6";
    private static final String COLUMN_FALSE = "col7";
    private static final String COLUMN_BYTES = "col8";
    private static final String COLUMN_YES = "col9";
    private static final String COLUMN_Y = "col10";
    private static final String COLUMN_NO = "col11";
    private static final String COLUMN_N = "col12";

    class TestMssqlRow extends MssqlRow {
        public TestMssqlRow() {
            _columnValues.put(COLUMN_TEST_VALUE, new TypedParameter("test value"));
            _columnValues.put(COLUMN_T, new TypedParameter("t"));
            _columnValues.put(COLUMN_1, new TypedParameter(1));
            _columnValues.put(COLUMN_TRUE, new TypedParameter(true));
            _columnValues.put(COLUMN_F, new TypedParameter("f"));
            _columnValues.put(COLUMN_0, new TypedParameter(0));
            _columnValues.put(COLUMN_FALSE, new TypedParameter(false));
            _columnValues.put(COLUMN_BYTES, new TypedParameter(BYTES));
            _columnValues.put(COLUMN_YES, new TypedParameter("yes"));
            _columnValues.put(COLUMN_Y, new TypedParameter("y"));
            _columnValues.put(COLUMN_NO, new TypedParameter("no"));
            _columnValues.put(COLUMN_N, new TypedParameter("n"));
        }
    };

    private MssqlRow createTestRow() {
        MssqlRow row = new TestMssqlRow();
        return row;
    }

    @Test
    public void testGetString() throws UnsupportedEncodingException {
        MssqlRow row = createTestRow();
        assertEquals("test value", row.getString(COLUMN_TEST_VALUE));
        assertEquals("t", row.getString(COLUMN_T));
        assertEquals("1", row.getString(COLUMN_1));
        assertEquals("1", row.getString(COLUMN_TRUE));
        assertEquals("f", row.getString(COLUMN_F));
        assertEquals("0", row.getString(COLUMN_0));
        assertEquals("0", row.getString(COLUMN_FALSE));
        assertEquals(new String(BYTES, JdbcRow.STRING_ENCODING), row.getString(COLUMN_BYTES));
        assertEquals("yes", row.getString(COLUMN_YES));
        assertEquals("y", row.getString(COLUMN_Y));
        assertEquals("no", row.getString(COLUMN_NO));
        assertEquals("n", row.getString(COLUMN_N));
    }

    @Test
    public void testGetBoolean() {
        MssqlRow row = createTestRow();
        assertEquals(false, row.getBoolean(COLUMN_TEST_VALUE));
        assertEquals(false, row.getBoolean(COLUMN_T));
        assertEquals(true, row.getBoolean(COLUMN_1));
        assertEquals(true, row.getBoolean(COLUMN_TRUE));
        assertEquals(false, row.getBoolean(COLUMN_F));
        assertEquals(false, row.getBoolean(COLUMN_0));
        assertEquals(false, row.getBoolean(COLUMN_FALSE));
        assertEquals(false, row.getBoolean(COLUMN_BYTES));
        assertEquals(false, row.getBoolean(COLUMN_YES));
        assertEquals(false, row.getBoolean(COLUMN_Y));
        assertEquals(false, row.getBoolean(COLUMN_NO));
        assertEquals(false, row.getBoolean(COLUMN_N));
    }

    @Test
    public void testGetBytes() {
        MssqlRow row = createTestRow();
        assertTrue(Arrays.equals("test value".getBytes(), row.getBytes(COLUMN_TEST_VALUE)));
        assertTrue(Arrays.equals("t".getBytes(), row.getBytes(COLUMN_T)));
        assertTrue(Arrays.equals("1".getBytes(), row.getBytes(COLUMN_1)));
        assertTrue(Arrays.equals("1".getBytes(), row.getBytes(COLUMN_TRUE)));
        assertTrue(Arrays.equals("f".getBytes(), row.getBytes(COLUMN_F)));
        assertTrue(Arrays.equals("0".getBytes(), row.getBytes(COLUMN_0)));
        assertTrue(Arrays.equals("0".getBytes(), row.getBytes(COLUMN_FALSE)));
        //                       UTF-8 bytes for \u1234
        assertTrue(Arrays.equals(BYTES, row.getBytes(COLUMN_BYTES)));
        assertTrue(Arrays.equals("yes".getBytes(), row.getBytes(COLUMN_YES)));
        assertTrue(Arrays.equals("y".getBytes(), row.getBytes(COLUMN_Y)));
        assertTrue(Arrays.equals("no".getBytes(), row.getBytes(COLUMN_NO)));
        assertTrue(Arrays.equals("n".getBytes(), row.getBytes(COLUMN_N)));
    }
}