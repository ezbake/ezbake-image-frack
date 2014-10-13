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

import ezbake.services.extractor.imagemetadata.thrift.Dimensions;

public class DimensionsPojo {
    public int width;
    public int height;
    public double aspectRatio;

    public static DimensionsPojo fromThrift(Dimensions thrift) {
        if (thrift == null) {
            return null;
        }

        final DimensionsPojo pojo = new DimensionsPojo();
        pojo.width = thrift.getWidth();
        pojo.height = thrift.getHeight();
        pojo.aspectRatio = thrift.getAspectRatio();
        return pojo;
    }

    public static Dimensions toThrift(DimensionsPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final Dimensions thrift = new Dimensions(pojo.width, pojo.height, pojo.aspectRatio);

        thrift.validate();

        return thrift;
    }
}
