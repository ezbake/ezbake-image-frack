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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import ezbake.data.elastic.common.pojo.FacetPojo;
import ezbake.data.elastic.common.pojo.FieldSortPojo;
import ezbake.services.indexing.image.thrift.ImageSearch;

public class ImageSearchPojo {
    public ImageSearchQueryPojo query;
    public PagingPojo paging; // optional
    public FieldSortPojo sort; // optional
    public List<FacetPojo> facets; // optional

    public static ImageSearchPojo fromThrift(ImageSearch thrift) {
        if (thrift == null) {
            return null;
        }

        final ImageSearchPojo pojo = new ImageSearchPojo();
        pojo.query = ImageSearchQueryPojo.fromThrift(thrift.getQuery());
        pojo.paging = PagingPojo.fromThrift(thrift.getPaging());
        pojo.sort = FieldSortPojo.fromThrift(thrift.getSort());
        pojo.facets = FacetPojo.fromThrift(thrift.getFacets());

        return pojo;
    }

    public static ImageSearch toThrift(ImageSearchPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final ImageSearch thrift = new ImageSearch();
        thrift.setQuery(ImageSearchQueryPojo.toThrift(pojo.query));
        thrift.setPaging(PagingPojo.toThrift(pojo.paging));
        thrift.setSort(FieldSortPojo.toThrift(pojo.sort));
        thrift.setFacets(FacetPojo.toThrift(pojo.facets));

        thrift.validate();

        return thrift;
    }

    public ImageSearch toThrift() throws TException {
        return toThrift(this);
    }

    public static ImageSearchPojo fromJson(String json) {
        try {
            final Gson gson = new GsonBuilder().create();
            final ImageSearchPojo pojo = gson.fromJson(json, ImageSearchPojo.class);

            return pojo;
        } catch (final JsonSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String toJson() {
        final Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        return gson.toJson(this, ImageSearchPojo.class);
    }
}
