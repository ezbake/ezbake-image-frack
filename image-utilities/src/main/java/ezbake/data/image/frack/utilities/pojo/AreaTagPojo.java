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

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;

import ezbake.services.indexing.image.thrift.AreaTag;

public class AreaTagPojo {
    public PointPojo upperLeft;
    public PointPojo lowerRight;
    public String comment;

    public static List<AreaTagPojo> fromThrift(List<AreaTag> thrifts) {
        final List<AreaTagPojo> pojos = new ArrayList<>(thrifts.size());
        for (final AreaTag thrift : thrifts) {
            final AreaTagPojo pojo = new AreaTagPojo();
            pojo.upperLeft = PointPojo.fromThrift(thrift.getUpperLeft());
            pojo.lowerRight = PointPojo.fromThrift(thrift.getLowerRight());
            pojo.comment = thrift.getComment();
            pojos.add(pojo);
        }

        return pojos;
    }

    public static List<AreaTag> toThrift(List<AreaTagPojo> pojos) throws TException {
        final List<AreaTag> thrifts = new ArrayList<>(pojos.size());
        for (final AreaTagPojo pojo : pojos) {
            final AreaTag thrift =
                    new AreaTag(PointPojo.toThrift(pojo.upperLeft), PointPojo.toThrift(pojo.lowerRight), pojo.comment);

            thrift.validate();

            thrifts.add(thrift);
        }

        return thrifts;
    }
}
