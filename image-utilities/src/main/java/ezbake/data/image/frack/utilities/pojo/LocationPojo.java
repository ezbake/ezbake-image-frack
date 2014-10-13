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

import ezbake.services.extractor.imagemetadata.thrift.Location;

public class LocationPojo {
    public static class GeoPointPojo {
        public double lat;
        public double lon;

        public GeoPointPojo(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
        }
    }

    public GeoPointPojo geo;
    public Double alt;

    public static List<LocationPojo> fromThrift(List<Location> thrifts) {
        final List<LocationPojo> pojos = new ArrayList<>(thrifts.size());
        for (final Location thrift : thrifts) {
            final LocationPojo pojo = new LocationPojo();
            pojo.geo = new GeoPointPojo(thrift.getLatitude(), thrift.getLongitude());

            if (thrift.isSetAltitude()) {
                pojo.alt = thrift.getAltitude();
            } else {
                pojo.alt = null;
            }

            pojos.add(pojo);
        }

        return pojos;
    }

    public static List<Location> toThrift(List<LocationPojo> pojos) throws TException {
        final List<Location> thrifts = new ArrayList<>(pojos.size());
        for (final LocationPojo pojo : pojos) {
            final Location thrift = new Location();
            thrift.setLatitude(pojo.geo.lat);
            thrift.setLongitude(pojo.geo.lon);

            if (pojo.alt != null) {
                thrift.setAltitude(pojo.alt);
            }

            thrift.validate();

            thrifts.add(thrift);
        }

        return thrifts;
    }
}
