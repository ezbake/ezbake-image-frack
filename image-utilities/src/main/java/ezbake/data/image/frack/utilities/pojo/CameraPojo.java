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

import ezbake.services.extractor.imagemetadata.thrift.Camera;

public class CameraPojo {
    public String make;
    public String model;
    public Double aperture;
    public Double exposureTime;
    public Double shutterSpeed;
    public Double focalLength;
    public ResolutionPojo focalPlane;

    public static CameraPojo fromThrift(Camera thrift) {
        if (thrift == null) {
            return null;
        }

        final CameraPojo pojo = new CameraPojo();
        pojo.make = thrift.getMake();
        pojo.model = thrift.getModel();
        pojo.aperture = thrift.isSetAperture() ? thrift.getAperture() : null;
        pojo.exposureTime = thrift.isSetExposureTime() ? thrift.getExposureTime() : null;
        pojo.shutterSpeed = thrift.isSetShutterSpeed() ? thrift.getShutterSpeed() : null;
        pojo.focalLength = thrift.isSetFocalLength() ? thrift.getFocalLength() : null;
        pojo.focalPlane = ResolutionPojo.fromThrift(thrift.getFocalPlane());

        return pojo;
    }

    public static Camera toThrift(CameraPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final Camera thrift = new Camera();
        thrift.setMake(pojo.make);
        thrift.setModel(pojo.model);

        if (pojo.aperture != null) {
            thrift.setAperture(pojo.aperture);
        }

        if (pojo.exposureTime != null) {
            thrift.setExposureTime(pojo.exposureTime);
        }

        if (pojo.shutterSpeed != null) {
            thrift.setShutterSpeed(pojo.shutterSpeed);
        }

        if (pojo.focalLength != null) {
            thrift.setFocalLength(pojo.focalLength);
        }

        thrift.setFocalPlane(ResolutionPojo.toThrift(pojo.focalPlane));

        thrift.validate();

        return thrift;
    }
}
