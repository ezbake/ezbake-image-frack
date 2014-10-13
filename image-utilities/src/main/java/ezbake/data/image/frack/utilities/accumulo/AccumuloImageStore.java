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

import static ezbake.data.image.frack.utilities.IndexingUtils.hexToBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.data.image.frack.utilities.ImageBinaryStore;
import ezbake.data.image.frack.utilities.ImageUtils;

import com.google.common.base.Joiner;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Visibility;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.extractor.imagemetadata.thrift.InvalidImageException;
import ezbake.services.indexing.image.thrift.IndexingStage;
import ezbake.services.indexing.image.thrift.IndexingStatus;
import ezbake.services.indexing.image.thrift.InsertFailed;
import ezbake.services.indexing.image.thrift.Thumbnail;
import ezbake.services.indexing.image.thrift.ThumbnailSize;

public class AccumuloImageStore implements ImageBinaryStore {
    private static final Logger logger = LoggerFactory.getLogger(AccumuloImageStore.class);

    private final AccumuloThriftReaderWriter accumuloReaderWriter;
    private final IndexingStatusReaderWriter statusWriter;

    public AccumuloImageStore(Properties config) throws AccumuloException, AccumuloSecurityException,
            TableExistsException, TableNotFoundException {
        accumuloReaderWriter = new AccumuloThriftReaderWriter(config);
        statusWriter = new IndexingStatusReaderWriter(accumuloReaderWriter);
    }

    public AccumuloImageStore(Instance instance, Properties config) throws AccumuloException,
            AccumuloSecurityException, TableExistsException, TableNotFoundException {
        accumuloReaderWriter = new AccumuloThriftReaderWriter(instance, config);
        statusWriter = new IndexingStatusReaderWriter(accumuloReaderWriter);
    }

    @Override
    public boolean ping() {
        return true;
    }

    @Override
    public void addImage(Image image, String imageId, Visibility visibility, String... types) throws InsertFailed {
        try {
            final byte[] rowId = hexToBytes(imageId);
            accumuloReaderWriter.write(image, rowId, visibility.getFormalVisibility());

            String type = "jpg";
            if (types.length > 0) {
                type = types[0];
            }

            for (final ThumbnailSize thumbSize : ThumbnailSize.values()) {
                final Thumbnail thumbnail = ImageUtils.createThumbnail(image, thumbSize, type);
                if (thumbnail != null) {
                    accumuloReaderWriter.write(thumbnail, rowId, visibility.getFormalVisibility(), thumbSize.name());
                }
            }
        } catch (final IOException ex) {
            final String errMsg = "Could not write image " + imageId + " into Accumulo";
            logger.error(errMsg, ex);
            throw new InsertFailed(imageId, errMsg);
        } catch (final InvalidImageException ex) {
            final String errMsg = "Could not write image " + imageId + " into Accumulo";
            logger.error(errMsg, ex);
            throw new InsertFailed(imageId, errMsg);
        } catch (final TException ex) {
            final String errMsg = "Could not write image " + imageId + " into Accumulo";
            logger.error(errMsg, ex);
            throw new InsertFailed(imageId, errMsg);
        }
    }

    @Override
    public Image getImage(String imageId, Authorizations auths) {
        try {
            return accumuloReaderWriter.read(Image.class, hexToBytes(imageId),
                    formalAuthsToString(auths.getFormalAuthorizations()));
        } catch (final IOException ex) {
            logger.error("Could not read image " + imageId + " from Accumulo.", ex);
        } catch (final TException ex) {
            logger.error("Could not read image " + imageId + " from Accumulo.", ex);
        }

        return null;
    }

    @Override
    public void deleteImage(String imageId, Authorizations auths) {
        try {
            accumuloReaderWriter.delete(hexToBytes(imageId));
        } catch (final Exception ex) {
            logger.error("Could not delete image " + imageId, ex);
        }
    }

    @Override
    public Thumbnail getThumbnail(String imageId, Authorizations auths, ThumbnailSize size) {
        try {
            return accumuloReaderWriter.read(Thumbnail.class, hexToBytes(imageId),
                    formalAuthsToString(auths.getFormalAuthorizations()), size.name());
        } catch (final IOException ex) {
            logger.error("Could not read thumbnail of size " + size + " for image " + imageId + " from Accumulo", ex);
        } catch (final TException ex) {
            logger.error("Could not read thumbnail of size " + size + " for image " + imageId + " from Accumulo", ex);
        }

        return null;
    }

    @Override
    public IndexingStatus getIndexingStatus(String imageId, Authorizations auths) {
        IndexingStatus status = null;
        try {
            status =
                    statusWriter.getStatus(hexToBytes(imageId), formalAuthsToString(auths.getFormalAuthorizations()));
        } catch (final IOException ex) {
            logger.error("Could not read indexing status for image " + imageId + " from Accumulo", ex);
        } catch (final TException ex) {
            logger.error("Could not read indexing status for image " + imageId + " from Accumulo", ex);
        }

        if (status == null) {
            status = new IndexingStatus(new ArrayList<IndexingStage>(), false);
        }

        return status;
    }

    @Override
    public void close() throws IOException {
        accumuloReaderWriter.close();
    }

    private String formalAuthsToString(Set<String> formalAuths) {
        return Joiner.on(',').join(formalAuths);
    }
}
