/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.db.*;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.clock.TimestampReconciler;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.service.StorageService;

public class MemtableMessenger {
    /*
     *  Create a special binary message just for Cassandra. How cute.
     */
    public static Message createMessage(String Keyspace, byte[] Key, String CFName, List<ColumnFamily> ColumnFamiles) {
        ColumnFamily baseColumnFamily;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        RowMutation rm;
        Message message;
        Column column;
    
        /* Get the first column family from list, this is just to get past validation */
        baseColumnFamily = new ColumnFamily(ColumnFamilyType.Standard,
                                            ClockType.Timestamp,
                                            DatabaseDescriptor.getComparator(Keyspace, CFName),
                                            DatabaseDescriptor.getSubComparator(Keyspace, CFName),
                                            TimestampReconciler.instance,
                                            CFMetaData.getId(Keyspace, CFName));
        
        for(ColumnFamily cf : ColumnFamiles) {
            bufOut.reset();
            ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
            byte[] data = new byte[bufOut.getLength()];
            System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());
            column = new Column(FBUtilities.toByteArray(cf.id()), data, new TimestampClock(0));
            baseColumnFamily.addColumn(column);
        }
        rm = new RowMutation(Keyspace, Key);
        rm.add(baseColumnFamily);
    
        try { /* Make message */
            message = rm.makeRowMutationMessage(StorageService.Verb.BINARY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return message;
    }

    public static Message createSuperMessage(String Keyspace, byte[] Key, String CFName, List<ColumnFamily> ColumnFamiles) {
        ColumnFamily baseColumnFamily;
        DataOutputBuffer bufOut = new DataOutputBuffer();
        RowMutation rm;
        Message message;
        Column column;
    
        /* Get the first column family from list, this is just to get past validation */
        baseColumnFamily = new ColumnFamily(ColumnFamilyType.Standard,
                                            ClockType.Timestamp,
                                            DatabaseDescriptor.getComparator(Keyspace, CFName),
                                            DatabaseDescriptor.getSubComparator(Keyspace, CFName),
                                            TimestampReconciler.instance,
                                            CFMetaData.getId(Keyspace, CFName));
        
        for(ColumnFamily cf : ColumnFamiles) {
            bufOut.reset();
            ColumnFamily.serializer().serializeWithIndexes(cf, bufOut);
            byte[] data = new byte[bufOut.getLength()];
            System.arraycopy(bufOut.getData(), 0, data, 0, bufOut.getLength());
            column = new Column(FBUtilities.toByteArray(cf.id()), data, new TimestampClock(0));
            baseColumnFamily.addColumn(column);
        }
        rm = new RowMutation(Keyspace, Key);
        rm.add(baseColumnFamily);
    
        try { /* Make message */
            message = rm.makeRowMutationMessage(StorageService.Verb.BINARY);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return message;
    }
}
