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

package ezbake.data.image.frack.utilities;

import java.io.IOException;

import ezbake.base.thrift.Authorizations;
import ezbake.base.thrift.Visibility;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.indexing.image.thrift.IndexingStatus;
import ezbake.services.indexing.image.thrift.InsertFailed;
import ezbake.services.indexing.image.thrift.Thumbnail;
import ezbake.services.indexing.image.thrift.ThumbnailSize;

public interface ImageBinaryStore {
    boolean ping();

    void addImage(Image image, String imageId, Visibility visibility, String... types) throws InsertFailed;

    Image getImage(String imageId, Authorizations auths);

    void deleteImage(String imageId, Authorizations auths);

    Thumbnail getThumbnail(String imageId, Authorizations auths, ThumbnailSize size);

    IndexingStatus getIndexingStatus(String imageId, Authorizations auths);

    void close() throws IOException;
}
