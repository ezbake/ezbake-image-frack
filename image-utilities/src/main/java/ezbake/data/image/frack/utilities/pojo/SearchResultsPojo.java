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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.thrift.TException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import ezbake.data.elastic.common.pojo.FacetResultPojo;
import ezbake.data.elastic.thrift.FacetResult;
import ezbake.services.indexing.image.thrift.SearchResults;

public class SearchResultsPojo {
    public List<IndexedImagePojo> images;
    public long totalResults;
    public PagingPojo paging; // optional
    public Map<String, FacetResultPojo> facets; // optional

    public static SearchResultsPojo fromThrift(SearchResults thrift) throws TException {
        if (thrift == null) {
            return null;
        }

        final SearchResultsPojo pojo = new SearchResultsPojo();
        pojo.images = IndexedImagePojo.fromThrift(thrift.getImages());
        pojo.totalResults = thrift.getTotalResults();
        pojo.paging = PagingPojo.fromThrift(thrift.getPaging());

        if (thrift.isSetFacets()) {
            pojo.facets = new HashMap<>(thrift.getFacetsSize());
            for (final Entry<String, FacetResult> thriftFacetEntry : thrift.getFacets().entrySet()) {
                pojo.facets.put(thriftFacetEntry.getKey(), FacetResultPojo.fromThrift(thriftFacetEntry.getValue()));
            }
        }

        return pojo;
    }

    public static SearchResults toThrift(SearchResultsPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final SearchResults thrift = new SearchResults();
        thrift.setImages(IndexedImagePojo.toThrift(pojo.images));
        thrift.setTotalResults(pojo.totalResults);
        thrift.setPaging(PagingPojo.toThrift(pojo.paging));

        if (pojo.facets != null) {
            final Map<String, FacetResult> thriftFacets = new HashMap<>(pojo.facets.size());
            for (final Entry<String, FacetResultPojo> pojoFacetEntry : pojo.facets.entrySet()) {
                thriftFacets.put(pojoFacetEntry.getKey(), FacetResultPojo.toThrift(pojoFacetEntry.getValue()));
            }

            thrift.setFacets(thriftFacets);
        }

        thrift.validate();

        return thrift;
    }

    public SearchResults toThrift() throws TException {
        return toThrift(this);
    }

    public static SearchResultsPojo fromJson(String json) {
        try {
            final Gson gson = new GsonBuilder().create();
            final SearchResultsPojo pojo = gson.fromJson(json, SearchResultsPojo.class);
            return pojo;
        } catch (final JsonSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String toJson() {
        final Gson gson =
                new GsonBuilder().disableHtmlEscaping().serializeSpecialFloatingPointValues().setPrettyPrinting()
                        .create();

        return gson.toJson(this, SearchResultsPojo.class);
    }
}
