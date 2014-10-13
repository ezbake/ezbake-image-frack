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

import org.apache.thrift.TException;

import ezbake.services.indexing.image.thrift.SimilarityFeature;
import ezbake.services.indexing.image.thrift.SimilaritySearchQuery;

public class SimilaritySearchQueryPojo {
    public SimilaritySearchQueryImagePojo queryImage;
    public SimilarityFeature feature;

    public static SimilaritySearchQueryPojo fromThrift(SimilaritySearchQuery thrift) {
        if (thrift == null) {
            return null;
        }

        final SimilaritySearchQueryPojo pojo = new SimilaritySearchQueryPojo();
        pojo.queryImage = SimilaritySearchQueryImagePojo.fromThrift(thrift.getQueryImage());
        pojo.feature = thrift.getFeature();

        return pojo;
    }

    public static SimilaritySearchQuery toThrift(SimilaritySearchQueryPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final SimilaritySearchQuery thrift = new SimilaritySearchQuery();
        thrift.setQueryImage(SimilaritySearchQueryImagePojo.toThrift(pojo.queryImage));
        thrift.setFeature(pojo.feature);

        return thrift;
    }
}
