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

package ezbake.data.image.frack.utilities.ingest;

import static org.apache.commons.lang.exception.ExceptionUtils.getStackTrace;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static ezbake.data.image.frack.utilities.ingest.EmbeddedImageExtractor.getImages;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.junit.Test;

import ezbake.services.extractor.imagemetadata.thrift.Image;

public class EmbeddedImageExtractorTest {
    private static class ImageInfo {
        public String origDocUri;
        public String name;
        public String mime;
        public int length;

        public ImageInfo(String origDocUri, String name, String mime, int length) {
            this.origDocUri = origDocUri;
            this.name = name;
            this.mime = mime;
            this.length = length;
        }

        @Override
        public String toString() {
            return String.format("name: '%s', mime: '%s', origDocUri: '%s', length: %d", name, mime, origDocUri,
                    length);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + length;
            result = prime * result + (mime == null ? 0 : mime.hashCode());
            result = prime * result + (name == null ? 0 : name.hashCode());
            result = prime * result + (origDocUri == null ? 0 : origDocUri.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }

            if (obj == null) {
                return false;
            }

            if (!(obj instanceof ImageInfo)) {
                return false;
            }

            final ImageInfo other = (ImageInfo) obj;

            if (length != other.length) {
                return false;
            }

            if (mime == null) {
                if (other.mime != null) {
                    return false;
                }
            } else if (!mime.equals(other.mime)) {
                return false;
            }

            if (name == null) {
                if (other.name != null) {
                    return false;
                }
            } else if (!name.equals(other.name)) {
                return false;
            }

            if (origDocUri == null) {
                if (other.origDocUri != null) {
                    return false;
                }
            } else if (!origDocUri.equals(other.origDocUri)) {
                return false;
            }

            return true;
        }
    }

    private static ImageInfo[] convertImageInfo(List<Image> extractedImages) {
        ImageInfo[] converted = null;
        if (extractedImages != null) {
            converted = new ImageInfo[extractedImages.size()];
            for (int idx = 0; idx < extractedImages.size(); idx++) {
                final Image image = extractedImages.get(idx);
                converted[idx] =
                        new ImageInfo(image.getOriginalDocumentUri(), image.getFileName(), image.getMimeType(),
                                image.getBlob().length);
            }
        }

        return converted;
    }

    private static void testImageExtract(String testFileName, ImageInfo[] expected) {
        try (InputStream testFileStream = EmbeddedImageExtractorTest.class.getResourceAsStream("/" + testFileName)) {
            assertNotNull("Test resource is missing: " + testFileName, testFileStream);
            try {
                assertArrayEquals(expected,
                        convertImageInfo(getImages(testFileStream, testFileName, "file://" + testFileName)));
            } catch (final Exception e) {
                fail(getStackTrace(e));
            }
        } catch (final IOException e) {
            fail(getStackTrace(e));
        }
    }

    @Test
    public void testImage() {
        final String fileName = "test.jpg";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected = {new ImageInfo(uri, fileName, "image/jpeg", 134594)};
        testImageExtract(fileName, expected);
    }

    @Test
    public void testDoc() {
        final String fileName = "extract_test.doc";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_image1.png", "image/png", 260694),
                        new ImageInfo(uri, fileName + "_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_image3.jpg", "image/jpeg", 134594)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testDocx() {
        final String fileName = "extract_test.docx";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_rId5_image1.gif", "image/gif", 229184),
                        new ImageInfo(uri, fileName + "_rId6_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_rId7_image3.jpeg", "image/jpeg", 134594)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testPpt() {
        final String fileName = "extract_test.ppt";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_image1.png", "image/png", 4059),
                        new ImageInfo(uri, fileName + "_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_image3.jpg", "image/jpeg", 134594)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testPptx() {
        final String fileName = "extract_test.pptx";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_slide1_rId2_image1.png", "image/png", 4059),
                        new ImageInfo(uri, fileName + "_slide1_rId3_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_slide2_rId2_image3.jpg", "image/jpeg", 134594)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testXls() {
        final String fileName = "extract_test.xls";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_image1.png", "image/png", 260694),
                        new ImageInfo(uri, fileName + "_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_image3.jpg", "image/jpeg", 134594)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testXlsx() {
        final String fileName = "extract_test.xlsx";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_rId1_image1.gif", "image/gif", 229184),
                        new ImageInfo(uri, fileName + "_rId2_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_rId1_image3.jpg", "image/jpeg", 134594)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testZip() {
        final String fileName = "extract_test.zip";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_test.bmp", "image/x-ms-bmp", 46182),
                        new ImageInfo(uri, fileName + "_test.gif", "image/gif", 229184),
                        new ImageInfo(uri, fileName + "_test.jpg", "image/jpeg", 134594),
                        new ImageInfo(uri, fileName + "_test.png", "image/png", 976)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testZipWithSubdirs() {
        final String fileName = "extract_test_with_subdirs.zip";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_pics/bar/test.jpg", "image/jpeg", 134594),
                        new ImageInfo(uri, fileName + "_pics/bar/test.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_pics/foo/test.bmp", "image/x-ms-bmp", 46182),
                        new ImageInfo(uri, fileName + "_pics/foo/test.gif", "image/gif", 229184)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testZipWithInnerZip() {
        final String fileName = "extract_test_inner_zip.zip";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_data/extract_test_with_subdirs.zip_pics/bar/test.jpg", "image/jpeg",
                        134594),
                        new ImageInfo(uri, fileName + "_data/extract_test_with_subdirs.zip_pics/bar/test.png",
                                "image/png", 976),
                        new ImageInfo(uri, fileName + "_data/extract_test_with_subdirs.zip_pics/foo/test.bmp",
                                "image/x-ms-bmp", 46182),
                        new ImageInfo(uri, fileName + "_data/extract_test_with_subdirs.zip_pics/foo/test.gif",
                                "image/gif", 229184),
                        new ImageInfo(uri, fileName + "_data/other_pics/baz/test2.jpg", "image/jpeg", 44606)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testZipWithInnerDocs() {
        final String fileName = "extract_test_inner_docs.zip";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_data/office/extract_test.docx_rId5_image1.gif", "image/gif", 229184),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.docx_rId6_image2.png", "image/png",
                                976),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.docx_rId7_image3.jpeg",
                                "image/jpeg", 134594),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.pptx_slide1_rId2_image1.png",
                                "image/png", 4059),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.pptx_slide1_rId3_image2.png",
                                "image/png", 976),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.pptx_slide2_rId2_image3.jpg",
                                "image/jpeg", 134594),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.xlsx_rId1_image1.gif", "image/gif",
                                229184),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.xlsx_rId2_image2.png", "image/png",
                                976),
                        new ImageInfo(uri, fileName + "_data/office/extract_test.xlsx_rId1_image3.jpg", "image/jpeg",
                                134594),
                        new ImageInfo(uri, fileName + "_data/other_pics/baz/test2.jpg", "image/jpeg", 44606)};

        testImageExtract(fileName, expected);
    }

    @Test
    public void testZipVeryNested() {
        final String fileName = "extract_test_very_nested.zip";
        final String uri = "file://" + fileName;
        final ImageInfo[] expected =
                {new ImageInfo(uri, fileName + "_data/inner.zip_inner/bar/test.jpg", "image/jpeg", 134594),
                        new ImageInfo(uri, fileName + "_data/inner.zip_inner/bar/test.png", "image/png", 976),
                        new ImageInfo(uri, fileName + "_data/inner.zip_inner/foo/test.bmp", "image/x-ms-bmp", 46182),
                        new ImageInfo(uri, fileName + "_data/inner.zip_inner/foo/test.gif", "image/gif", 229184),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.docx_rId5_image1.gif", "image/gif",
                                229184),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.docx_rId6_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.docx_rId7_image3.jpeg", "image/jpeg",
                                134594),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.pptx_slide1_rId2_image1.png",
                                "image/png", 4059),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.pptx_slide1_rId3_image2.png",
                                "image/png", 976),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.pptx_slide2_rId2_image3.jpg",
                                "image/jpeg", 134594),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.xlsx_rId1_image1.gif", "image/gif",
                                229184),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.xlsx_rId2_image2.png", "image/png", 976),
                        new ImageInfo(uri, fileName
                                + "_data/inner.zip_inner/office/extract_test.xlsx_rId1_image3.jpg", "image/jpeg",
                                134594),
                        new ImageInfo(uri, fileName + "_data/other_pics/baz/test2.jpg", "image/jpeg", 44606)};

        testImageExtract(fileName, expected);
    }
}
