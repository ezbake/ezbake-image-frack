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

package ezbake.data.image.frack.utilities.pojo;

import java.util.List;

import org.apache.thrift.TException;

import ezbake.services.extractor.imagemetadata.thrift.ImageMetadata;

public class ImageMetadataPojo {
    public String fileName;
    public String mimeType;
    public String originalDocumentUri;
    public List<OriginalMetadataPojo> original;
    public NormalizedImageMetadataPojo normalized;

    public static ImageMetadataPojo fromThrift(ImageMetadata thrift) {
        if (thrift == null) {
            return null;
        }

        final ImageMetadataPojo pojo = new ImageMetadataPojo();
        pojo.fileName = thrift.getFileName();
        pojo.mimeType = thrift.getMimeType();
        pojo.originalDocumentUri = thrift.getOriginalDocumentUri();
        pojo.original = OriginalMetadataPojo.fromThrift(thrift.getOriginal());
        pojo.normalized = NormalizedImageMetadataPojo.fromThrift(thrift.getNormalized());
        return pojo;
    }

    public static ImageMetadata toThrift(ImageMetadataPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final ImageMetadata thrift = new ImageMetadata();
        thrift.setFileName(pojo.fileName);
        thrift.setMimeType(pojo.mimeType);
        thrift.setOriginalDocumentUri(pojo.originalDocumentUri);
        thrift.setOriginal(OriginalMetadataPojo.toThrift(pojo.original));
        thrift.setNormalized(NormalizedImageMetadataPojo.toThrift(pojo.normalized));

        thrift.validate();

        return thrift;
    }
}
