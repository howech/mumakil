import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.avro.Column;
import org.apache.cassandra.avro.ColumnOrSuperColumn;
import org.apache.cassandra.avro.Mutation;

public class CassandraUtils {

    public static byte[] stringToLongBytes(String value) {
        Long longValue = Long.parseLong(value);
        byte[] asBytes = new byte[ 8 ];
        asBytes[0] = (byte)(longValue >>> 56);
        asBytes[1] = (byte)(longValue >>> 48);
        asBytes[2] = (byte)(longValue >>> 40);
        asBytes[3] = (byte)(longValue >>> 32);
        asBytes[4] = (byte)(longValue >>> 24);
        asBytes[5] = (byte)(longValue >>> 16);
        asBytes[6] = (byte)(longValue >>>  8);
        asBytes[7] = (byte)(longValue >>>  0);
        return asBytes;
    }
    
    public static Mutation getMutation(String name, String value, Long timeStamp) {
        Mutation m = new Mutation();
        m.column_or_supercolumn = getCoSC(name, value, timeStamp);
        return m;
    }

    public static ColumnOrSuperColumn getCoSC(String name, String value, Long timeStamp) {
        ByteBuffer columnName  = ByteBuffer.wrap(name.getBytes());
        ByteBuffer columnValue = ByteBuffer.wrap(value.getBytes());

        Column c    = new Column();
        c.name      = columnName;
        c.value     = columnValue;
        c.timestamp = timeStamp;
        c.ttl       = 0;
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.column = c;
        return cosc;
    }

    public static Mutation getMutation(byte[] name, byte[] value, Long timeStamp) {
        Mutation m = new Mutation();
        m.column_or_supercolumn = getCoSC(name, value, timeStamp);
        return m;
    }

    public static ColumnOrSuperColumn getCoSC(byte[] name, byte[] value, Long timeStamp) {
        ByteBuffer columnName  = ByteBuffer.wrap(name);
        ByteBuffer columnValue = ByteBuffer.wrap(value);

        Column c    = new Column();
        c.name      = columnName;
        c.value     = columnValue;
        c.timestamp = timeStamp;
        c.ttl       = 0;
        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
        cosc.column = c;
        return cosc;
    }
}
