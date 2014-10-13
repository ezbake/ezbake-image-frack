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

import static ezbake.data.image.frack.utilities.ImageUtils.createThumbnail;
import static ezbake.data.image.frack.utilities.IndexingUtils.hexToBytes;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;

import ezbake.data.image.frack.utilities.accumulo.AccumuloThriftReaderWriter;
import ezbake.data.image.frack.utilities.accumulo.IndexingStatusReaderWriter;

import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.extractor.imagemetadata.thrift.InvalidImageException;
import ezbake.services.indexing.image.thrift.IndexingStage;
import ezbake.services.indexing.image.thrift.IngestedImage;
import ezbake.services.indexing.image.thrift.Thumbnail;
import ezbake.services.indexing.image.thrift.ThumbnailSize;

public class ThumbnailWorker extends Worker<IngestedImage> {
    private static final long serialVersionUID = -6322323192743295953L;
    private static final Logger logger = getLogger(ThumbnailWorker.class);

    private String defaultType;
    private AccumuloThriftReaderWriter accumuloReaderWriter;
    private IndexingStatusReaderWriter statusWriter;

    public ThumbnailWorker() {
        super(IngestedImage.class);
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);
        defaultType = properties.getProperty("thumbnail.type", "jpg");

        try {
            accumuloReaderWriter = new AccumuloThriftReaderWriter(properties);
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException | TableNotFoundException ex) {
            final String errMsg = "Accumulo error";
            logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }

        statusWriter = new IndexingStatusReaderWriter(accumuloReaderWriter);
    }

    @Override
    public void cleanup() {
        super.cleanup();

        try {
            accumuloReaderWriter.close();
        } catch (final IOException ex) {
            logger.error("Could not close Accumulo reader/writer", ex);
        }
    }

    @Override
    public void process(Visibility visibility, IngestedImage ingestedImage) {
        try {
            final String imageId = ingestedImage.getImageInfo().getImageId();
            final byte[] rowId = hexToBytes(imageId);
            final String authorizations = ingestedImage.getAuthorizations();
            final Image image = accumuloReaderWriter.read(Image.class, rowId, authorizations);
            if (image == null) {
                logger.error("Accumulo read of image returned null");
                return;
            }

            final String fileName = ingestedImage.getImageInfo().getFileName();
            final String type = fileName == null ? defaultType : fileName.substring(fileName.lastIndexOf('.') + 1);

            for (final ThumbnailSize thumbSize : ThumbnailSize.values()) {
                final Thumbnail thumbnail = createThumbnail(image, thumbSize, type);
                accumuloReaderWriter.write(thumbnail, rowId, visibility.getFormalVisibility(), thumbSize.name());
                logger.info("Wrote thumbnail of size {} for image {}", thumbSize, imageId);
            }

            statusWriter.addCompletedStage(rowId, visibility.getFormalVisibility(), authorizations,
                    IndexingStage.THUMBNAILS_GENERATED);
        } catch (final IOException ex) {
            logger.error("Error completing stage", ex);
        } catch (final InvalidImageException ex) {
            logger.error("Invalid ingested image", ex);
        } catch (final TException ex) {
            logger.error("Thrift error", ex);
        }
    }
}
