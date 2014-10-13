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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class OriginalMetadataPojo {
    public String tagType;
    public String name;
    public String value;

    public static List<OriginalMetadataPojo> fromThrift(Map<String, String> thrift) {
        final List<OriginalMetadataPojo> pojos = new ArrayList<>(thrift.size());

        for (final Entry<String, String> origEntry : thrift.entrySet()) {
            final OriginalMetadataPojo origPojo = new OriginalMetadataPojo();
            final String[] keyParts = origEntry.getKey().split(" \\| ");
            origPojo.tagType = keyParts[0];
            origPojo.name = keyParts[1];
            origPojo.value = origEntry.getValue();

            pojos.add(origPojo);
        }

        return pojos;
    }

    public static Map<String, String> toThrift(List<OriginalMetadataPojo> pojos) {
        final Map<String, String> thrift = new HashMap<>(pojos.size());
        for (final OriginalMetadataPojo pojo : pojos) {
            thrift.put(pojo.tagType + " | " + pojo.name, pojo.value);
        }

        return thrift;
    }
}
