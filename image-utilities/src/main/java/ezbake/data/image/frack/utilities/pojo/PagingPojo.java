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

import ezbake.services.indexing.image.thrift.Paging;

public class PagingPojo {
    public int pageSize;
    public int offset;

    public static PagingPojo fromThrift(Paging thrift) {
        if (thrift == null) {
            return null;
        }

        final PagingPojo pojo = new PagingPojo();
        pojo.pageSize = thrift.getPageSize();
        pojo.offset = thrift.getOffset();

        return pojo;
    }

    public static Paging toThrift(PagingPojo pojo) throws TException {
        if (pojo == null) {
            return null;
        }

        final Paging thrift = new Paging(pojo.pageSize, pojo.offset);

        thrift.validate();

        return thrift;
    }
}
