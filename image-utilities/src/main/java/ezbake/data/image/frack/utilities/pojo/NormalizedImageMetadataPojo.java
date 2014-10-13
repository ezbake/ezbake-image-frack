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
import java.util.TreeSet;

import org.apache.thrift.TException;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import ezbake.base.thrift.Date;
import ezbake.base.thrift.DateTime;
import ezbake.base.thrift.Time;
import ezbake.base.thrift.TimeZone;
import ezbake.services.extractor.imagemetadata.thrift.NormalizedImageMetadata;

public class NormalizedImageMetadataPojo {
    public DimensionsPojo dimensions;
    public List<LocationPojo> locations;
    public List<HeadingPojo> headings;
    public List<String> names;
    public List<String> createdDateTimes;
    public List<String> modifiedDateTimes;
    public List<String> comments;
    public List<String> software;
    public CameraPojo camera;
    public Short orientation;
    public ResolutionPojo resolution;

    static List<String> dateTimesFromThrift(List<DateTime> thrifts) {
        final List<String> dtStrings = new ArrayList<>(thrifts.size());
        for (final DateTime thrift : thrifts) {
            final Date date = thrift.getDate();
            final short year = date.getYear();
            final short month = date.getMonth();
            final short day = date.getDay();

            String dtString = "";
            if (thrift.isSetTime()) {
                final short hour = thrift.getTime().getHour();
                final short min = thrift.getTime().getMinute();
                final short sec = thrift.getTime().getSecond();
                final short ms = thrift.getTime().getMillisecond();
                final org.joda.time.DateTime converted =
                        new org.joda.time.DateTime(year, month, day, hour, min, sec, ms, DateTimeZone.UTC);

                dtString = ISODateTimeFormat.dateTime().print(converted);
            } else {
                final LocalDate converted = new LocalDate(year, month, day);
                dtString = ISODateTimeFormat.date().print(converted);
            }

            dtStrings.add(dtString);
        }

        return new ArrayList<>(new TreeSet<>(dtStrings));
    }

    static List<DateTime> dateTimesToThrift(List<String> dtStrings) {
        final List<DateTime> thrifts = new ArrayList<>(dtStrings.size());
        final DateTimeFormatter dateOnlyParser = ISODateTimeFormat.date().withZoneUTC();
        final DateTimeFormatter dateTimeParser = ISODateTimeFormat.dateTime().withZoneUTC();
        for (final String dtString : dtStrings) {
            org.joda.time.DateTime dt = null;
            boolean timeSet = true;

            try {
                dt = dateTimeParser.parseDateTime(dtString);
            } catch (final IllegalArgumentException e) {
                timeSet = false;
            }

            if (dt == null) {
                dt = dateOnlyParser.parseDateTime(dtString);
            }

            final Date thriftDate =
                    new Date((short) dt.getMonthOfYear(), (short) dt.getDayOfMonth(), (short) dt.getYear());

            final DateTime thrift = new DateTime(thriftDate);

            if (timeSet) {
                final Time thriftTime = new Time();
                thriftTime.setHour((short) dt.getHourOfDay());
                thriftTime.setMinute((short) dt.getMinuteOfHour());
                thriftTime.setSecond((short) dt.getSecondOfMinute());
                thriftTime.setMillisecond((short) dt.getMillisOfSecond());
                thriftTime.setTz(new TimeZone((short) 0, (short) 0, true));

                thrift.setTime(thriftTime);
            }

            thrifts.add(thrift);
        }
        return thrifts;
    }

    public static NormalizedImageMetadataPojo fromThrift(NormalizedImageMetadata thrift) {
        if (thrift == null) {
            return null;
        }

        final NormalizedImageMetadataPojo pojo = new NormalizedImageMetadataPojo();
        pojo.dimensions = DimensionsPojo.fromThrift(thrift.getDimensions());
        pojo.locations = LocationPojo.fromThrift(thrift.getLocations());
        pojo.headings = HeadingPojo.fromThrift(thrift.getHeadings());
        pojo.names = thrift.getNames();
        pojo.createdDateTimes = dateTimesFromThrift(thrift.getCreatedDateTimes());
        pojo.modifiedDateTimes = dateTimesFromThrift(thrift.getModifiedDateTimes());
        pojo.comments = thrift.getComments();
        pojo.software = thrift.getSoftware();
        pojo.camera = CameraPojo.fromThrift(thrift.getCamera());

        if (thrift.isSetOrientation()) {
            pojo.orientation = thrift.getOrientation();
        } else {
            pojo.orientation = null;
        }

        pojo.resolution = ResolutionPojo.fromThrift(thrift.getResolution());
        return pojo;
    }

    public static NormalizedImageMetadata toThrift(NormalizedImageMetadataPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final NormalizedImageMetadata thrift = new NormalizedImageMetadata();
        thrift.setDimensions(DimensionsPojo.toThrift(pojo.dimensions));
        thrift.setLocations(LocationPojo.toThrift(pojo.locations));
        thrift.setHeadings(HeadingPojo.toThrift(pojo.headings));
        thrift.setNames(pojo.names);
        thrift.setCreatedDateTimes(dateTimesToThrift(pojo.createdDateTimes));
        thrift.setModifiedDateTimes(dateTimesToThrift(pojo.modifiedDateTimes));
        thrift.setComments(pojo.comments);
        thrift.setSoftware(pojo.software);
        thrift.setCamera(CameraPojo.toThrift(pojo.camera));

        if (pojo.orientation != null) {
            thrift.setOrientation(pojo.orientation);
        }

        thrift.setResolution(ResolutionPojo.toThrift(pojo.resolution));

        thrift.validate();

        return thrift;
    }
}
