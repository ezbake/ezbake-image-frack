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

import static org.apache.commons.io.IOUtils.toByteArray;
import static org.junit.Assert.assertArrayEquals;

import java.io.InputStream;
import java.util.Properties;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ezbake.services.extractor.imagemetadata.thrift.Image;

public class MockAccumuloTest {
    private AccumuloThriftReaderWriter accumuloReaderWriter;

    @Before
    public void setup() throws Exception {
        final MockInstance instance = new MockInstance("mock");
        final Properties config = new Properties();
        config.setProperty("accumulo.username", "");
        config.setProperty("accumulo.password", "");
        accumuloReaderWriter = new AccumuloThriftReaderWriter(instance, config);
    }

    @Test
    public void testWriteRead() throws Exception {
        final String fileName = "test.png";
        byte[] bytes = null;
        try (InputStream fileIn = MockAccumuloTest.class.getResourceAsStream("/" + fileName)) {
            bytes = toByteArray(fileIn);
        }

        final Image image = new Image();
        image.setFileName(fileName);
        image.setOriginalDocumentUri("file://" + fileName);
        image.setBlob(bytes);
        accumuloReaderWriter.write(image, fileName.getBytes(), "U");

        final Image imageR = accumuloReaderWriter.read(Image.class, fileName.getBytes(), "U");
        final byte[] bytesR = imageR.getBlob();
        assertArrayEquals(bytes, bytesR);
    }

    @After
    public void tearDown() throws Exception {
        accumuloReaderWriter.close();
    }
}
