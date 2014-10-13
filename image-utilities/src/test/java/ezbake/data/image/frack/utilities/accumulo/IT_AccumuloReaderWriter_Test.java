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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.InputStream;
import java.util.Properties;

import org.elasticsearch.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Visibility;
import ezbake.services.extractor.imagemetadata.thrift.Image;

@Ignore
public class IT_AccumuloReaderWriter_Test {
    AccumuloThriftReaderWriter accumuloReaderWriter;
    AccumuloImageStore imageStore;

    @Before
    public void setup() throws Exception {
        final Properties config = new Properties();
        config.setProperty("accumulo.instance.name", "gary");
        config.setProperty("accumulo.zookeepers", "localhost");
        config.setProperty("accumulo.username", "root");
        config.setProperty("accumulo.password", "gary");
        config.setProperty("accumulo.splitBits", "2"); // use two split bits
        accumuloReaderWriter = new AccumuloThriftReaderWriter(config);
        imageStore = new AccumuloImageStore(config);
    }

    @Test
    public void testSplitWriteRead() throws Exception {
        final Image image = loadImageResource("test.png");
        final byte[] rowId = image.getFileName().getBytes();
        accumuloReaderWriter.write(image, rowId, "U");
        final Image imageR = accumuloReaderWriter.read(Image.class, rowId, "U");
        assertArrayEquals(image.getBlob(), imageR.getBlob());
    }

    @Test
    public void testDelete() throws Exception {
        final Visibility unclassVis = new Visibility().setFormalVisibility("U");
        final Authorizations noAuths = new Authorizations();
        final Authorizations unclassAuths = new Authorizations().setFormalAuthorizations(Sets.newHashSet("U"));

        final Image image = loadImageResource("test.tif");
        final String imageId = "0000000000";
        imageStore.addImage(image, imageId, unclassVis);

        final Image getAfterAdd = imageStore.getImage(imageId, unclassAuths);
        assertNotNull(getAfterAdd);
        assertArrayEquals(image.getBlob(), getAfterAdd.getBlob());

        imageStore.deleteImage(imageId, noAuths);
        final Image getAfterUnauthDelete = imageStore.getImage(imageId, unclassAuths);
        assertNotNull(getAfterUnauthDelete);
        assertArrayEquals(image.getBlob(), getAfterUnauthDelete.getBlob());

        imageStore.deleteImage(imageId, unclassAuths);
        final Image getAfterDelete = imageStore.getImage(imageId, unclassAuths);
        assertNull(getAfterDelete);
    }

    @After
    public void tearDown() throws Exception {
        accumuloReaderWriter.close();
    }

    private Image loadImageResource(String fileName) throws Exception {
        byte[] bytes = null;
        try (InputStream fileIn = AccumuloImageStoreTest.class.getResourceAsStream("/" + fileName)) {
            bytes = toByteArray(fileIn);
        }

        final Image image = new Image();
        image.setFileName(fileName);
        image.setOriginalDocumentUri("file://" + fileName);
        image.setBlob(bytes);

        return image;
    }
}
