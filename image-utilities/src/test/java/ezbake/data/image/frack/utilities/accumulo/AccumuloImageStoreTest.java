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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

import static ezbake.data.image.frack.utilities.IndexingUtils.bytesToHex;
import static ezbake.data.image.frack.utilities.IndexingUtils.getHash;

import java.io.InputStream;
import java.util.Properties;

import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Visibility;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.indexing.image.thrift.Thumbnail;
import ezbake.services.indexing.image.thrift.ThumbnailSize;

public class AccumuloImageStoreTest {
    private AccumuloImageStore imageStore;

    @Before
    public void setUp() throws Exception {
        final MockInstance instance = new MockInstance("mock");
        final Properties config = new Properties();
        config.setProperty("accumulo.username", "");
        config.setProperty("accumulo.password", "");
        imageStore = new AccumuloImageStore(instance, config);
    }

    @Test
    public void testBmp() throws Exception {
        testImage("test.bmp");
    }

    @Test
    public void testGif() throws Exception {
        testImage("test.gif");
    }

    @Test
    public void testJpg() throws Exception {
        testImage("test.jpg");
        testImage("test2.jpg");
        testImage("test3.jpg");
    }

    @Test
    public void testPng() throws Exception {
        testImage("test.png");
    }

    @Test
    public void testTiff() throws Exception {
        testImage("test.tif");
    }

    @After
    public void tearDown() throws Exception {
        imageStore.close();
    }

    private void testImage(String fileName) throws Exception {
        byte[] bytes = null;
        try (InputStream fileIn = AccumuloImageStoreTest.class.getResourceAsStream("/" + fileName)) {
            bytes = IOUtils.toByteArray(fileIn);
        }

        final Visibility unclassVis = new Visibility().setFormalVisibility("U");
        final Authorizations unclassAuths = new Authorizations().setFormalAuthorizations(Sets.newHashSet("U"));

        final Image image = new Image();
        image.setFileName(fileName);
        image.setOriginalDocumentUri("file://" + fileName);
        image.setBlob(bytes);

        final String imageId = bytesToHex(getHash(bytes, fileName));
        imageStore.addImage(image, imageId, unclassVis);
        final Image imageR = imageStore.getImage(imageId, unclassAuths);
        assertArrayEquals(image.getBlob(), imageR.getBlob());

        for (final ThumbnailSize thumbSize : ThumbnailSize.values()) {
            final Thumbnail thumbnail = imageStore.getThumbnail(imageId, unclassAuths, thumbSize);
            assertNotNull(thumbnail);
        }

        imageStore.deleteImage(imageId, unclassAuths);
    }
}
