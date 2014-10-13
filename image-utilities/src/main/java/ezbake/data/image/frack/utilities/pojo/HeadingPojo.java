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

import ezbake.services.extractor.imagemetadata.thrift.Heading;
import ezbake.services.extractor.imagemetadata.thrift.HeadingReference;

public class HeadingPojo {
    public double degrees;
    public HeadingReference reference;

    public static List<HeadingPojo> fromThrift(List<Heading> thrifts) {
        final List<HeadingPojo> pojos = new ArrayList<>(thrifts.size());
        for (final Heading thrift : thrifts) {
            final HeadingPojo pojo = new HeadingPojo();
            pojo.degrees = thrift.getDegrees();
            pojo.reference = thrift.getReference();
            pojos.add(pojo);
        }

        return pojos;
    }

    public static List<Heading> toThrift(List<HeadingPojo> pojos) throws TException {
        final List<Heading> thrifts = new ArrayList<>(pojos.size());
        for (final HeadingPojo pojo : pojos) {
            final Heading thrift = new Heading(pojo.degrees, pojo.reference);

            thrift.validate();

            thrifts.add(thrift);
        }

        return thrifts;
    }
}
