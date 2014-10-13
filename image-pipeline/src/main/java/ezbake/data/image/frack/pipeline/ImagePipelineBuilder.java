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

package ezbake.data.image.frack.pipeline;

import static org.slf4j.LoggerFactory.getLogger;

import static ezbake.data.image.frack.utilities.ingest.PipelineDocumentIngester.TOPIC_NAME;

import org.slf4j.Logger;

import ezbake.frack.api.Listener;
import ezbake.frack.api.Pipeline;
import ezbake.frack.api.PipelineBuilder;
import ezbake.services.indexing.image.thrift.IngestedImage;

public class ImagePipelineBuilder implements PipelineBuilder {
    private static final Logger logger = getLogger(ImagePipelineBuilder.class);

    private static final String LISTENER_ID = "_image_listener";
    private static final String THUMBNAIL_WORKER_ID = "_thumbnail_worker";
    private static final String METADATA_WORKER_ID = "_metadata_worker";

    @Override
    public Pipeline build() {
        final Pipeline pipeline = new Pipeline();
        final String pid = pipeline.getId();
        final String lId = pid + LISTENER_ID;
        logger.info("Pipeline ID is {}", pid);

        final String thumbnailWorkerId = pid + THUMBNAIL_WORKER_ID;
        final String metadataWorkerId = pid + METADATA_WORKER_ID;

        final Listener<IngestedImage> imageListener = new Listener<>(IngestedImage.class);
        imageListener.registerListenerTopic(TOPIC_NAME);

        pipeline.addListener(lId, imageListener);

        pipeline.addWorker(thumbnailWorkerId, new ThumbnailWorker());
        pipeline.addConnection(lId, thumbnailWorkerId);

        pipeline.addWorker(metadataWorkerId, new MetadataWorker());
        pipeline.addConnection(lId, metadataWorkerId);

        return pipeline;
    }
}
