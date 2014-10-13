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

import java.io.IOException;

import org.apache.thrift.TException;
import org.elasticsearch.common.Base64;

import ezbake.services.indexing.image.thrift.SimilaritySearchQueryImage;

// Union
public class SimilaritySearchQueryImagePojo {
    public String imageId;
    public String imageBinary; // base64-encoded

    public static SimilaritySearchQueryImagePojo fromThrift(SimilaritySearchQueryImage thrift) {
        if (thrift == null) {
            return null;
        }

        final SimilaritySearchQueryImagePojo pojo = new SimilaritySearchQueryImagePojo();

        switch (thrift.getSetField()) {
            case IMAGE_ID:
                pojo.imageId = thrift.getImageId();
                break;
            case IMAGE_BINARY:
                pojo.imageBinary = Base64.encodeBytes(thrift.getImageBinary());
                break;
            default:
                throw new IllegalArgumentException("Unknown field set in union");
        }

        return pojo;
    }

    public static SimilaritySearchQueryImage toThrift(SimilaritySearchQueryImagePojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final SimilaritySearchQueryImage thrift = new SimilaritySearchQueryImage();

        if (pojo.imageId != null) {
            thrift.setImageId(pojo.imageId);
        } else if (pojo.imageBinary != null) {
            try {
                thrift.setImageBinary(Base64.decode(pojo.imageBinary));
            } catch (final IOException e) {
                throw new IllegalArgumentException("Similarity search has invalid image binary");
            }
        } else {
            throw new IllegalArgumentException("No field set in POJO for Thrift union");
        }

        return thrift;
    }
}
