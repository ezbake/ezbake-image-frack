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

import static org.apache.commons.io.IOUtils.toByteArray;
import static org.apache.tika.config.TikaConfig.getDefaultConfig;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.slf4j.Logger;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import ezbake.services.extractor.imagemetadata.thrift.Image;

public class EmbeddedImageExtractor implements EmbeddedDocumentExtractor {
    private static final String[] EXCLUDED_PREFIXES = {"__MACOSX"};
    private static final Logger logger = getLogger(EmbeddedImageExtractor.class);

    private final Parser parser;
    private final String containerFileName;
    private final String origDocUri;
    private final List<Image> extractedImages = new ArrayList<>();
    private int imageCount = 1;

    private static MediaType getMediaType(InputStream stream) throws IOException {
        return new DefaultDetector().detect(stream, new Metadata());
    }

    public EmbeddedImageExtractor(Parser parser, String containerFileName, String origDocUri) {
        this.parser = parser;
        this.containerFileName = containerFileName;
        this.origDocUri = origDocUri;
    }

    public static List<Image> getImages(InputStream stream, String fileName, String origDocUri) throws IOException,
            SAXException, TikaException {
        final List<Image> imageInfos = new ArrayList<>();

        final MediaType contentType = getMediaType(stream);
        if (contentType.getType().equals("image")) {
            final Image image = new Image();
            image.setBlob(toByteArray(stream));
            image.setFileName(fileName);
            image.setOriginalDocumentUri(origDocUri);
            image.setMimeType(contentType.toString());
            imageInfos.add(image);
        } else {
            final Parser parser = new AutoDetectParser();
            final EmbeddedImageExtractor imageExtractor = new EmbeddedImageExtractor(parser, fileName, origDocUri);
            final ParseContext context = new ParseContext();
            context.set(EmbeddedDocumentExtractor.class, imageExtractor);
            parser.parse(stream, new DefaultHandler(), new Metadata(), context);
            imageInfos.addAll(imageExtractor.getExtractedImages());
        }

        return imageInfos;
    }

    @Override
    public void parseEmbedded(InputStream stream, ContentHandler handler, Metadata metadata, boolean outputHtml)
            throws SAXException, IOException {
        final StringBuilder name = new StringBuilder(containerFileName);
        boolean updateCount = false;

        final String relId = metadata.get(TikaMetadataKeys.EMBEDDED_RELATIONSHIP_ID);
        if (relId != null) {
            name.append('_');
            name.append(relId);
        }

        String resName = metadata.get(TikaMetadataKeys.RESOURCE_NAME_KEY);
        if (resName == null) {
            resName = "image" + imageCount;
            updateCount = true;
        }

        if (!resName.equals(relId)) {
            name.append('_');
            name.append(resName);
        }

        final MediaType contentType = getMediaType(stream);
        if (contentType == null) {
            logger.error("Could not detect content type for file '{}' in '{}'", resName, containerFileName);
            return;
        }

        if (resName.indexOf('.') == -1) {
            try {
                final String extension =
                        getDefaultConfig().getMimeRepository().forName(contentType.toString()).getExtension();
                name.append(extension);
            } catch (final MimeTypeException e) {
                logger.error("Error when trying to get extension for file '{}' in '{}'", resName, containerFileName);
            }
        }

        final String imageName = name.toString();

        for (final String excludedPrefix : EXCLUDED_PREFIXES) {
            if (resName.startsWith(excludedPrefix) || relId != null && relId.startsWith(excludedPrefix)) {
                logger.debug("Skipping resource '{}' in '{}' as it matches excluded prefix '{}'", imageName,
                        containerFileName, excludedPrefix);

                return;
            }
        }

        if (contentType.getType().equals("image")) {
            final Image image = new Image();
            image.setBlob(toByteArray(stream));
            image.setFileName(imageName);
            image.setOriginalDocumentUri(origDocUri);
            if (contentType != null) {
                image.setMimeType(contentType.toString());
            }

            logger.debug("Found image with name '{}' in '{}'", imageName, containerFileName);
            extractedImages.add(image);

            if (updateCount) {
                imageCount++;
            }
        } else if (contentType.getType().equals("text")) {
            logger.debug("Ignoring text file '{}' in '{}'", resName, containerFileName);
        } else {
            logger.debug("Recursing into inner file '{}' in '{}'", resName, containerFileName);
            final ParseContext context = new ParseContext();
            final EmbeddedImageExtractor subExtractor =
                    new EmbeddedImageExtractor(parser, name.toString(), origDocUri);

            context.set(EmbeddedDocumentExtractor.class, subExtractor);
            try {
                parser.parse(stream, new DefaultHandler(), new Metadata(), context);
                extractedImages.addAll(subExtractor.getExtractedImages());
            } catch (final TikaException e) {
                logger.error("Could not parse sub-element '" + resName + "' in '" + containerFileName + "'", e);
            }
        }
    }

    @Override
    public boolean shouldParseEmbedded(Metadata metadata) {
        return true;
    }

    public boolean hasImages() {
        return !extractedImages.isEmpty();
    }

    public List<Image> getExtractedImages() {
        return extractedImages;
    }
}
