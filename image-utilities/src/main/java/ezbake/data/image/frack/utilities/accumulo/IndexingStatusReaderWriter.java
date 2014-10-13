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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.thrift.TException;

import ezbake.services.indexing.image.thrift.IndexingStage;
import ezbake.services.indexing.image.thrift.IndexingStatus;

public class IndexingStatusReaderWriter {
    private static final String STATUS_FAMILY = "IndexingStatus";

    private final AccumuloThriftReaderWriter accumuloReaderWriter;

    public IndexingStatusReaderWriter(AccumuloThriftReaderWriter accumuloReaderWriter) {
        this.accumuloReaderWriter = accumuloReaderWriter;
    }

    public void addCompletedStage(byte[] rowId, String visibility, String authorizations, IndexingStage completedStage)
            throws IOException, TException {
        final IndexingStatus status = getStatus(rowId, authorizations);
        if (!status.getCompletedStages().contains(completedStage)) {
            status.addToCompletedStages(completedStage);
        }

        if (status.getCompletedStagesSize() == IndexingStage.values().length) {
            status.setCompleted(true);
        }

        accumuloReaderWriter.write(status, rowId, visibility, STATUS_FAMILY);
    }

    public IndexingStatus getStatus(byte[] rowId, String authorizations) throws IOException, TException {
        IndexingStatus status = accumuloReaderWriter.read(IndexingStatus.class, rowId, authorizations, STATUS_FAMILY);
        if (status == null) {
            status = new IndexingStatus(new ArrayList<IndexingStage>(), false);
        }

        return status;
    }
}
