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

import ezbake.services.indexing.image.thrift.ImageSearchQuery;

// Union
public class ImageSearchQueryPojo {
    public String elasticJson;
    public SimilaritySearchQueryPojo similarityQuery;

    public static ImageSearchQueryPojo fromThrift(ImageSearchQuery thrift) {
        if (thrift == null) {
            return null;
        }

        final ImageSearchQueryPojo pojo = new ImageSearchQueryPojo();

        switch (thrift.getSetField()) {
            case ELASTIC_JSON:
                pojo.elasticJson = thrift.getElasticJson();
                break;
            case SIMILARITY_QUERY:
                pojo.similarityQuery = SimilaritySearchQueryPojo.fromThrift(thrift.getSimilarityQuery());
                break;
            default:
                throw new IllegalArgumentException("Unknown field set in union");
        }

        return pojo;
    }

    public static ImageSearchQuery toThrift(ImageSearchQueryPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final ImageSearchQuery thrift = new ImageSearchQuery();

        if (pojo.elasticJson != null) {
            thrift.setElasticJson(pojo.elasticJson);
        } else if (pojo.similarityQuery != null) {
            thrift.setSimilarityQuery(SimilaritySearchQueryPojo.toThrift(pojo.similarityQuery));
        } else {
            throw new IllegalArgumentException("No field set in POJO for Thrift union");
        }

        return thrift;
    }
}
