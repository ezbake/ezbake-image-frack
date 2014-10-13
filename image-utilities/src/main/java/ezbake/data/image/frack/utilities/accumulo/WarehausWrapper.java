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

package ezbake.data.image.frack.utilities.accumulo;

import static org.slf4j.LoggerFactory.getLogger;

import static ezbake.data.image.frack.utilities.IndexingUtils.bytesToHex;
import static ezbake.data.image.frack.utilities.IndexingUtils.getHash;

import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.apache.thrift.TException;
import org.slf4j.Logger;

import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.Visibility;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;
import ezbake.warehaus.Repository;
import ezbake.warehaus.WarehausService;
import ezbake.warehaus.WarehausServiceConstants;

public class WarehausWrapper {
    private static final Logger logger = getLogger(WarehausWrapper.class);

    private final String uriPrefix;
    private final String warehausSecurityId;
    private final EzbakeSecurityClient client;
    private final ThriftClientPool pool;

    public WarehausWrapper(String uriPrefix, Properties config) {
        this.uriPrefix = uriPrefix;
        pool = new ThriftClientPool(config);
        warehausSecurityId = pool.getSecurityId(WarehausServiceConstants.SERVICE_NAME);
        client = new EzbakeSecurityClient(config);
    }

    public String put(byte[] content, String fileName, Visibility visibility) throws TException,
            NoSuchAlgorithmException {
        final byte[] index = getHash(content, fileName);
        final String uri = uriPrefix + bytesToHex(index);
        final Repository repo = new Repository().setParsedData(index).setRawData(content).setUri(uri);

        final WarehausService.Client warehaus =
                pool.getClient(WarehausServiceConstants.SERVICE_NAME, WarehausService.Client.class);

        try {
            final EzSecurityToken token = client.fetchAppToken(warehausSecurityId);
            warehaus.insert(repo, visibility, token);
            logger.info("Inserted {} into Warehaus", fileName);
        } finally {
            pool.returnToPool(warehaus);
        }

        return uri;
    }
}
