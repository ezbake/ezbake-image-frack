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

import static ezbake.thrift.ThriftUtils.deserializeFromBase64;
import static ezbake.thrift.ThriftUtils.serializeToBase64;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;

import ezbake.base.thrift.Visibility;
import ezbake.services.indexing.image.thrift.IndexedImage;

public class IndexedImagePojo {
    public String imageId;
    public List<String> textTags;
    public List<AreaTagPojo> areaTags;
    public String visibility;
    public ImageMetadataPojo metadata;

    public static IndexedImagePojo fromThrift(IndexedImage thrift) throws TException {
        if (thrift == null) {
            return null;
        }

        final IndexedImagePojo pojo = new IndexedImagePojo();
        pojo.imageId = thrift.getImageId();
        pojo.textTags = thrift.getTextTags();
        pojo.areaTags = AreaTagPojo.fromThrift(thrift.getAreaTags());
        pojo.visibility = serializeToBase64(thrift.getVisibility());
        pojo.metadata = ImageMetadataPojo.fromThrift(thrift.getMetadata());
        return pojo;
    }

    public static List<IndexedImagePojo> fromThrift(List<IndexedImage> thrifts) throws TException {
        if (thrifts == null) {
            return null;
        }

        final List<IndexedImagePojo> pojos = new ArrayList<>(thrifts.size());
        for (final IndexedImage thrift : thrifts) {
            pojos.add(fromThrift(thrift));
        }

        return pojos;
    }

    public static IndexedImage toThrift(IndexedImagePojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final IndexedImage thrift = new IndexedImage();
        thrift.setImageId(pojo.imageId);
        thrift.setTextTags(pojo.textTags);
        thrift.setAreaTags(AreaTagPojo.toThrift(pojo.areaTags));
        thrift.setVisibility(deserializeFromBase64(Visibility.class, pojo.visibility));
        thrift.setMetadata(ImageMetadataPojo.toThrift(pojo.metadata));

        thrift.validate();

        return thrift;
    }

    public static List<IndexedImage> toThrift(List<IndexedImagePojo> pojos) throws TException {
        if (pojos == null) {
            return null;
        }

        final List<IndexedImage> thrifts = new ArrayList<>(pojos.size());
        for (final IndexedImagePojo pojo : pojos) {
            thrifts.add(toThrift(pojo));
        }

        return thrifts;
    }

    public IndexedImage toThrift() throws TException {
        return toThrift(this);
    }

    public static IndexedImagePojo fromJson(String json) {
        try {
            final Gson gson = new GsonBuilder().create();
            final IndexedImagePojo pojo = gson.fromJson(json, IndexedImagePojo.class);
            return pojo;
        } catch (final JsonSyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String toJson(boolean serializeImageId, boolean serializeVisibility) throws TException {
        final String origImageId = imageId;
        if (!serializeImageId) {
            imageId = null;
        }

        final String origVisibility = visibility;
        if (!serializeVisibility) {
            visibility = null;
        }

        final Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
        final String json = gson.toJson(this, IndexedImagePojo.class);

        imageId = origImageId;
        visibility = origVisibility;

        return json;
    }
}
