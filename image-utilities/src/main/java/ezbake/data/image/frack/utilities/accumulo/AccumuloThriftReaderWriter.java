/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

package ezbake.data.image.frack.utilities.accumulo;

import static org.slf4j.LoggerFactory.getLogger;

import static ezbake.data.common.classification.ClassificationUtils.getAuthsFromString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RowDeletingIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

import ezbake.configuration.constants.EzBakePropertyConstants;
import ezbake.thrift.ThriftUtils;

public class AccumuloThriftReaderWriter {
    private static final Logger logger = getLogger(AccumuloThriftReaderWriter.class);

    private final String tableName = "ImageStore";
    private static final String IMAGE_CHUNK_FAMILY = "Image_Chunk";
    private static final String LENGTH_QUALIFIER = "Length";
    private static final String COLUMN_QUALIFIER_PREFIX = "Piece_";

    // Use 5 MB to make sure most of images to be written in one chunk
    private static final int DEFAULT_CHUNK_SIZE = 5;
    private static final long MAX_MEMORY = 32 * 1024 * 1024;
    private static final int MAX_WRITE_THREADS = 8;
    private static final long MAX_LATENCY_MS = Long.MAX_VALUE;

    private final Properties config;
    private final Connector connector;
    private final BatchWriter writer;

    public AccumuloThriftReaderWriter(Properties config) throws AccumuloException, AccumuloSecurityException,
            TableExistsException, TableNotFoundException {
        this(createZookeeperInstance(config), config);
    }

    public AccumuloThriftReaderWriter(Instance instance, Properties config) throws AccumuloException,
            AccumuloSecurityException, TableExistsException, TableNotFoundException {
        this.config = config;

        final String userName = config.getProperty(EzBakePropertyConstants.ACCUMULO_USERNAME);

        final PasswordToken passToken =
                new PasswordToken(config.getProperty(EzBakePropertyConstants.ACCUMULO_PASSWORD));

        connector = instance.getConnector(userName, passToken);
        makesureTableExist();

        final BatchWriterConfig writerConfig =
                new BatchWriterConfig().setMaxMemory(MAX_MEMORY).setMaxWriteThreads(MAX_WRITE_THREADS)
                        .setMaxLatency(MAX_LATENCY_MS, TimeUnit.MILLISECONDS);

        writer = connector.createBatchWriter(tableName, writerConfig);
    }

    public <T extends TBase<?, ?>> T read(Class<T> clazz, byte[] rowId, String authorization) throws IOException,
            TException {
        return read(clazz, rowId, authorization, IMAGE_CHUNK_FAMILY);
    }

    public <T extends TBase<?, ?>> T read(Class<T> clazz, byte[] rowId, String authorization, String appFamily)
            throws IOException, TException {
        final byte[] bytes = read(rowId, authorization, appFamily);
        if (bytes == null) {
            return null;
        }

        return ThriftUtils.deserialize(clazz, bytes);
    }

    public void write(TBase<?, ?> t, byte[] rowId, String visibility) throws IOException, TException {
        write(t, rowId, visibility, IMAGE_CHUNK_FAMILY);
    }

    public void write(TBase<?, ?> t, byte[] rowId, String visibility, String appFamily) throws IOException,
            TException {
        write(t, rowId, visibility, DEFAULT_CHUNK_SIZE, appFamily);
    }

    public void write(TBase<?, ?> t, byte[] rowId, String visibility, int chunkInMB, String appFamily)
            throws IOException, TException {
        write(ThriftUtils.serialize(t), rowId, visibility, chunkInMB, appFamily);
    }

    public void delete(byte[] rowIDbyte) throws IOException {
        try {
            final Text rowID = new Text(rowIDbyte);
            final Mutation mutation = new Mutation(rowID);
            mutation.put(new Text(), new Text(), new ColumnVisibility(), RowDeletingIterator.DELETE_ROW_VALUE);
            writer.addMutation(mutation);
            writer.flush();
        } catch (final Exception ex) {
            ex.printStackTrace();
            logger.error("Error in deleting Accumulo row.");
        }
    }

    public void deleteRow(byte[] rowId, String authorizations) {
        try {
            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(authorizations));
            final Range range = new Range(new Text(rowId));
            scanner.setRange(range);

            Mutation deleter = null;
            for (final Entry<Key, Value> entry : scanner) {
                if (deleter == null) {
                    deleter = new Mutation(entry.getKey().getRow());
                }
                deleter.putDelete(entry.getKey().getColumnFamily(), entry.getKey().getColumnQualifier());
            }
            writer.addMutation(deleter);
            writer.flush();
        } catch (final Exception ex) {
            logger.error("Error delete row", ex);
        }
    }

    public void close() throws IOException {
        try {
            writer.close();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }

    private static ZooKeeperInstance createZookeeperInstance(Properties config) {
        final String instanceName = config.getProperty(EzBakePropertyConstants.ACCUMULO_INSTANCE_NAME);
        String zookeepers = config.getProperty(EzBakePropertyConstants.ACCUMULO_ZOOKEEPERS);
        if (zookeepers.indexOf(':') == -1) {
            zookeepers += ":2181";
        }

        return new ZooKeeperInstance(instanceName, zookeepers);
    }

    private void makesureTableExist() {
        final TableOperations operations = connector.tableOperations();
        if (operations.exists(tableName)) {
            return;
        }

        try {
            operations.create(tableName);
            final String splitBitsKey = "accumulo.splitBits";
            int splitBits = 0;
            try {
                final String splitBitsString = config.getProperty(splitBitsKey);
                if (splitBitsString != null) {
                    splitBits = Integer.parseInt(splitBitsString);
                }
            } catch (final NumberFormatException e) {
                logger.warn("Invalid number given for config key " + splitBitsKey);
            }

            if (splitBits > 0) {
                operations.addSplits(tableName, getSplits(splitBits));
            }

            operations.attachIterator(tableName, new IteratorSetting(1, RowDeletingIterator.class));
        } catch (final Exception ex) {
            logger.error("Error in create Accumulo table.");
        }
    }

    private SortedSet<Text> getSplits(int splitBits) {
        Preconditions.checkArgument(splitBits <= Byte.SIZE);

        final int splitMax = 1 << splitBits;
        final int splitOffset = Byte.SIZE - splitBits;

        final SortedSet<Text> splits = new TreeSet<>();

        final byte[] key = new byte[1];
        for (int i = 1; i < splitMax; i++) {
            key[0] = (byte) (i << splitOffset);
            splits.add(new Text(key));
        }
        return splits;
    }

    private byte[] read(byte[] rowId, String authorizations, String appFamily) throws IOException {
        logger.debug("Reading authorization = {}", authorizations);
        try {
            final Scanner scanner = connector.createScanner(tableName, getAuthsFromString(authorizations));
            scanner.fetchColumnFamily(new Text(appFamily));

            final Range range = new Range(new Text(rowId));
            scanner.setRange(range);

            ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
            int n = -1;
            int bufferSize = 0;
            for (final Entry<Key, Value> kv : scanner) {
                final Key key = kv.getKey();
                final byte[] bytes = kv.getValue().get();
                if (++n == 0) {
                    if (!key.getColumnQualifier().toString().equals(LENGTH_QUALIFIER)) {
                        throw new IOException("The column qulifier for first k/v pair must be " + LENGTH_QUALIFIER);
                    }

                    byteBuffer.put(bytes);
                    bufferSize = byteBuffer.getInt(0);
                    byteBuffer = ByteBuffer.allocate(bufferSize);
                    continue;
                }

                if (!key.getColumnQualifier().toString()
                        .equals(String.format("%s%04d", COLUMN_QUALIFIER_PREFIX, n - 1))) {
                    throw new IOException("The column qualifier is not in right order.");
                }

                byteBuffer.put(bytes);
            }

            final byte[] dataBuffer = bufferSize == 0 ? null : byteBuffer.array();
            return dataBuffer;
        } catch (final TableNotFoundException ex) {
            logger.error("Could not find Accumulo table " + tableName, ex);
        }

        return null;
    }

    private void write(byte[] bytes, byte[] rowIDbyte, String visibility, int chunkInMB, String appFamily)
            throws IOException {
        // makesureTableExist();

        try {
            final int nBytes = bytes.length;
            logger.debug("Size of thrift object is {}", nBytes);

            final int chunkSize = chunkInMB > 0 ? chunkInMB * 1024 * 1024 : nBytes;
            final ColumnVisibility vis = new ColumnVisibility(visibility);

            final Text rowID = new Text(rowIDbyte);

            // write image length
            {
                final Mutation mutation = new Mutation(rowID);
                final ByteBuffer byteBuffer = ByteBuffer.allocate(4);
                byteBuffer.putInt(nBytes);
                mutation.put(appFamily, LENGTH_QUALIFIER, vis, new Value(byteBuffer.array()));
                writer.addMutation(mutation);
            }

            final int nChunk = nBytes / chunkSize;
            for (int i = 0; i < nChunk + 1; i++) {
                int size = chunkSize;
                if (i == nChunk) {
                    size = nBytes % chunkSize;
                    if (size == 0) {
                        break;
                    }
                }
                final Mutation mutation = new Mutation(rowID);
                final String colQualifier = String.format("%s%04d", COLUMN_QUALIFIER_PREFIX, i);
                mutation.put(appFamily, colQualifier, vis, new Value(bytes, i * chunkSize, size));
                writer.addMutation(mutation);
            }
            writer.flush();
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
    }
}
