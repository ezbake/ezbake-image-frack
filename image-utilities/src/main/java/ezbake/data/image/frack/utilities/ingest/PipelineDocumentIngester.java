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

import static org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;
import static org.slf4j.LoggerFactory.getLogger;

import static ezbake.data.image.frack.utilities.IndexingUtils.bytesToHex;
import static ezbake.data.image.frack.utilities.IndexingUtils.getHash;
import static ezbake.data.image.frack.utilities.ingest.EmbeddedImageExtractor.getImages;

import static ezbake.common.openshift.OpenShiftUtil.inOpenShiftContainer;
import static ezbake.data.common.classification.ClassificationUtils.extractUserAuths;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.thrift.TException;
import org.apache.tika.exception.TikaException;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import ezbake.data.image.frack.utilities.accumulo.AccumuloThriftReaderWriter;
import ezbake.data.image.frack.utilities.accumulo.IndexingStatusReaderWriter;
import ezbake.data.image.frack.utilities.accumulo.WarehausWrapper;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.ezbroadcast.core.EzBroadcaster;
import ezbake.security.lock.smith.thrift.EzLocksmith;
import ezbake.security.lock.smith.thrift.EzLocksmithConstants;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.indexing.image.thrift.Document;
import ezbake.services.indexing.image.thrift.ImageIndexerServiceConstants;
import ezbake.services.indexing.image.thrift.IndexingStage;
import ezbake.services.indexing.image.thrift.IngestedDocumentInfo;
import ezbake.services.indexing.image.thrift.IngestedImage;
import ezbake.services.indexing.image.thrift.IngestedImageInfo;
import ezbake.thrift.ThriftClientPool;
import ezbake.thrift.ThriftUtils;

public class PipelineDocumentIngester implements DocumentIngester {
    public static final String TOPIC_NAME = "ImageIngest";

    public static final String URI_PREFIX = String.format("%s://%s/", ImageIndexerServiceConstants.SERVICE_NAME,
            TOPIC_NAME);

    private static final Logger logger = getLogger(PipelineDocumentIngester.class);

    private final Properties props;
    private final String groupId;
    private final ThriftClientPool pool;
    private final WarehausWrapper warehaus;
    private final AccumuloThriftReaderWriter accumuloThriftWriter;
    private final IndexingStatusReaderWriter statusWriter;

    public PipelineDocumentIngester(Properties props) {
        this.props = props;
        this.groupId = randomAlphanumeric(20);
        this.pool = new ThriftClientPool(props);
        this.warehaus = new WarehausWrapper(URI_PREFIX, props);

        try {
            this.accumuloThriftWriter = new AccumuloThriftReaderWriter(props);
        } catch (final Exception ex) {
            final String errMsg = "Cannot open Accumulo writer";
            logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }

        this.statusWriter = new IndexingStatusReaderWriter(accumuloThriftWriter);
    }

    @Override
    public IngestedDocumentInfo ingestDocument(EzSecurityToken token, Document document) throws TException {
        final String userAuths = extractUserAuths(token);
        final Visibility visibility = document.getVisibility();
        final String fileName = document.getFileName();
        final byte[] fileContents = document.getBlob();
        try (EzBroadcaster broadcaster = getBroadcaster(token)) {
            logger.info("Inserting {} into Warehaus", fileName);
            final String warehausUri = warehaus.put(fileContents, fileName, visibility);
            logger.info("Inserted {} into Warehaus with URI {}", fileName, warehausUri);

            // Extract images from document
            logger.info("Extracting images from {}", fileName);
            List<Image> extractedImages = null;
            extractedImages = getImages(new ByteArrayInputStream(fileContents), fileName, warehausUri);
            logger.info("Extracted {} images from {}", extractedImages.size(), fileName);

            final List<IngestedImageInfo> ingestedImageInfos = new ArrayList<>(extractedImages.size());
            for (final Image image : extractedImages) {
                logger.info("Found image {} in {}", image.getFileName(), fileName);
                final byte[] imageHash = getHash(image.getBlob(), image.getFileName());
                final String imageIndex = bytesToHex(imageHash);

                statusWriter.addCompletedStage(imageHash, visibility.getFormalVisibility(), userAuths,
                        IndexingStage.EXTRACTED_FROM_DOC);

                final IngestedImageInfo ingestedImageInfo = new IngestedImageInfo();
                ingestedImageInfo.setOrigDocumentUri(warehausUri);
                ingestedImageInfo.setVisibility(visibility);
                ingestedImageInfo.setImageId(imageIndex);
                ingestedImageInfo.setMimeType(image.getMimeType());
                ingestedImageInfo.setSize(image.getBlob().length);
                ingestedImageInfo.setFileName(image.getFileName());

                ingestedImageInfos.add(ingestedImageInfo);

                final IngestedImage ingestedImage = new IngestedImage();
                ingestedImage.setImageInfo(ingestedImageInfo);
                ingestedImage.setAuthorizations(userAuths);

                logger.info("Writing image into Accumulo with index {} and visibility {}", imageIndex,
                        visibility.getFormalVisibility());

                accumuloThriftWriter.write(image, imageHash, visibility.getFormalVisibility());

                statusWriter.addCompletedStage(imageHash, visibility.getFormalVisibility(), userAuths,
                        IndexingStage.BINARY_SAVED);

                // Broadcast image to pipeline
                broadcaster.broadcast(TOPIC_NAME, visibility, ThriftUtils.serialize(ingestedImage));

                statusWriter.addCompletedStage(imageHash, visibility.getFormalVisibility(), userAuths,
                        IndexingStage.TO_PROCESSING_WORKERS);
            }

            final IngestedDocumentInfo ingested =
                    new IngestedDocumentInfo(warehausUri, ingestedImageInfos, visibility);

            ingested.setFileName(fileName);
            broadcaster.unregisterFromTopic(TOPIC_NAME);

            return ingested;
        } catch (final IOException e) {
            logger.error("IO error", e);
            throw new TException(e);
        } catch (SAXException | TikaException e) {
            final String errMsg = "Could not extract images from document " + fileName;
            logger.error(errMsg, e);
            throw new TException(errMsg, e);
        } catch (final NoSuchAlgorithmException e) {
            final String errMsg = "Could not create image ID for image from document " + fileName;
            logger.error(errMsg, e);
            throw new TException(errMsg, e);
        }
    }

    @SuppressWarnings("resource")
    private EzBroadcaster getBroadcaster(EzSecurityToken token) {
        EzBroadcaster ezbroadcaster = null;
        if (new EzProperties(props, false).getBoolean(EzBroadcaster.PRODUCTION_MODE, false) || inOpenShiftContainer()) {
            EzLocksmith.Client locksmith = null;
            try {
                locksmith = pool.getClient(EzLocksmithConstants.SERVICE_NAME, EzLocksmith.Client.class);
                final String key = locksmith.retrievePublicKey(token, TOPIC_NAME, null);
                ezbroadcaster = EzBroadcaster.create(props, groupId, key, TOPIC_NAME, false);
            } catch (final TException e) {
                throw new RuntimeException("Could not initialize broadcaster without key from locksmith", e);
            } finally {
                pool.returnToPool(locksmith);
            }
        } else {
            ezbroadcaster = EzBroadcaster.create(props, groupId);
        }

        ezbroadcaster.registerBroadcastTopic(TOPIC_NAME);

        return ezbroadcaster;
    }
}
