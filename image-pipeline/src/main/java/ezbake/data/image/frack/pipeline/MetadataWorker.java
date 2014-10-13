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

package ezbake.data.image.frack.pipeline;

import static org.slf4j.LoggerFactory.getLogger;

import static ezbake.data.image.frack.utilities.IndexingUtils.bytesToHex;
import static ezbake.data.image.frack.utilities.IndexingUtils.hexToBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import ezbake.data.image.frack.utilities.accumulo.AccumuloThriftReaderWriter;
import ezbake.data.image.frack.utilities.accumulo.IndexingStatusReaderWriter;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.extractor.imagemetadata.thrift.ImageMetadata;
import ezbake.services.extractor.imagemetadata.thrift.ImageMetadataExtractorConstants;
import ezbake.services.extractor.imagemetadata.thrift.ImageMetadataExtractorService;
import ezbake.services.extractor.imagemetadata.thrift.InvalidImageException;
import ezbake.services.indexing.image.thrift.AreaTag;
import ezbake.services.indexing.image.thrift.ImageIndexerService;
import ezbake.services.indexing.image.thrift.ImageIndexerServiceConstants;
import ezbake.services.indexing.image.thrift.IndexedImage;
import ezbake.services.indexing.image.thrift.IndexingStage;
import ezbake.services.indexing.image.thrift.IngestedImage;
import ezbake.services.indexing.image.thrift.InsertFailed;
import ezbake.thrift.ThriftClientPool;
import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

public class MetadataWorker extends Worker<IngestedImage> {
    private static final long serialVersionUID = 4841242625011285630L;
    private static final Logger logger = getLogger(MetadataWorker.class);

    private EzSecurityToken token;
    private ThriftClientPool pool;
    private EzbakeSecurityClient securityClient;
    private AccumuloThriftReaderWriter accumuloReaderWriter;
    private IndexingStatusReaderWriter statusWriter;

    public MetadataWorker() {
        super(IngestedImage.class);
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);

        try {
            pool = new ThriftClientPool(properties);

            securityClient = new EzbakeSecurityClient(properties);
            final String securityId = new EzBakeApplicationConfigurationHelper(properties).getSecurityID();
            token = securityClient.fetchAppToken(securityId);

            accumuloReaderWriter = new AccumuloThriftReaderWriter(properties);
            statusWriter = new IndexingStatusReaderWriter(accumuloReaderWriter);
        } catch (final EzSecurityTokenException ex) {
            final String errMsg = "EzSecurity token error";
            logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException ex) {
            final String errMsg = "Accumulo error";
            logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();

        try {
            accumuloReaderWriter.close();
        } catch (final IOException ex) {
            logger.error("Could not close Accumulo reader/writer", ex);
        }

        if (pool != null) {
            pool.close();
        }

        if (securityClient != null) {
            try {
                securityClient.close();
            } catch (final IOException e) {
                logger.error("Could not close security client", e);
            }
        }
    }

    @Override
    public void process(Visibility visibility, IngestedImage ingestedImage) {
        ImageMetadataExtractorService.Client imageMetadataExtractor = null;
        ImageIndexerService.Client imageIndexer = null;
        try {
            final String imageId = ingestedImage.getImageInfo().getImageId();
            final byte[] indexBinary = hexToBytes(imageId);
            final String authorizations = ingestedImage.getAuthorizations();
            final Image image = accumuloReaderWriter.read(Image.class, indexBinary, authorizations);
            if (image == null) {
                logger.error("Accumulo read of image returned null");
                return;
            }

            logger.info("Extracting metadata from image {}", imageId);

            final String indexString = bytesToHex(indexBinary);
            final List<String> textTags = new ArrayList<>();
            final List<AreaTag> areaTags = new ArrayList<>();
            final Visibility imageVis = ingestedImage.getImageInfo().getVisibility();

            imageMetadataExtractor =
                    pool.getClient(ImageMetadataExtractorConstants.SERVICE_NAME,
                            ImageMetadataExtractorService.Client.class);

            final ImageMetadata metadata = imageMetadataExtractor.getMetadata(image);
            logger.info("Extracted metadata from image {}", imageId);

            final IndexedImage toIndex = new IndexedImage(indexString, textTags, areaTags, imageVis, metadata);

            imageIndexer =
                    pool.getClient(ImageIndexerServiceConstants.SERVICE_NAME, ImageIndexerService.Client.class);

            imageIndexer.upsertImage(toIndex, token);
            logger.info("Inserted metadata from image {} into index", imageId);

            statusWriter.addCompletedStage(indexBinary, visibility.getFormalVisibility(), authorizations,
                    IndexingStage.METADATA_EXTRACTED);
        } catch (final InvalidImageException ex) {
            logger.error("Invalid or unsupported image given", ex);
        } catch (final InsertFailed ex) {
            logger.error("Insertion of image metadata into dataset failed", ex);
        } catch (final TException ex) {
            logger.error("Thrift error", ex);
        } catch (final Exception ex) {
            logger.error("Unknown error", ex);
        } finally {
            if (imageMetadataExtractor != null) {
                pool.returnToPool(imageMetadataExtractor);
            }

            if (imageIndexer != null) {
                pool.returnToPool(imageIndexer);
            }
        }
    }
}
