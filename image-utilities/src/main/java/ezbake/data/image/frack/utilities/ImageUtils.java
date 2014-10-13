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

package ezbake.data.image.frack.utilities;

import static java.net.URLConnection.guessContentTypeFromName;

import static org.slf4j.LoggerFactory.getLogger;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.activation.FileTypeMap;
import javax.imageio.ImageIO;

import org.imgscalr.Scalr;
import org.slf4j.Logger;

import ezbake.services.extractor.imagemetadata.thrift.Dimensions;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.extractor.imagemetadata.thrift.InvalidImageException;
import ezbake.services.indexing.image.thrift.Thumbnail;
import ezbake.services.indexing.image.thrift.ThumbnailSize;

public class ImageUtils {
    private static final Logger logger = getLogger(ImageUtils.class);

    public static BufferedImage getBufferedImage(Image image) throws IOException {
        final InputStream imageStream = new ByteArrayInputStream(image.getBlob());
        return ImageIO.read(imageStream);
    }

    public static byte[] getBytesFromBufferedImage(BufferedImage bufImage, String outputFormat) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            ImageIO.write(bufImage, outputFormat, out);
            out.flush();
            return out.toByteArray();
        }
    }

    public static Dimensions getDimensions(Image image) throws IOException {
        return getDimensions(getBufferedImage(image));
    }

    public static Dimensions getDimensions(BufferedImage bufImage) {
        Dimensions imageDims = null;
        if (bufImage != null) {
            final int width = bufImage.getWidth();
            final int height = bufImage.getHeight();
            final double aspectRatio = (double) width / height;
            imageDims = new Dimensions(width, height, aspectRatio);
        }

        return imageDims;
    }

    public static int getMaxScaledDimension(Image image, int maxSize) throws IOException {
        return getMaxScaledDimension(getBufferedImage(image), maxSize);
    }

    public static int getMaxScaledDimension(BufferedImage bufImage, int maxSize) {
        final int maxImageDim = Math.max(bufImage.getWidth(), bufImage.getHeight());
        return Math.min(maxSize, maxImageDim);
    }

    public static String getMimeType(String fileName) {
        String mime = guessContentTypeFromName(fileName);
        if (mime == null) {
            mime = FileTypeMap.getDefaultFileTypeMap().getContentType(fileName);
        }

        return mime;
    }

    public static Thumbnail createThumbnail(Image source, ThumbnailSize size, String outputFormat)
            throws IOException, InvalidImageException {
        final BufferedImage bufferedImage = getBufferedImage(source);
        if (bufferedImage == null) {
            String errMsg;
            if (source.isSetMimeType()) {
                errMsg = "Image of type " + source.getMimeType() + " is not supported for thumbnail creation";
            } else {
                errMsg = "Image type is not supported for thumbnail creation";
            }

            throw new InvalidImageException(errMsg);
        }

        final int actualMaxSize = getMaxScaledDimension(bufferedImage, getMaxThumbnailSize(size));

        final BufferedImage tbImage =
                Scalr.resize(bufferedImage, Scalr.Method.BALANCED, Scalr.Mode.AUTOMATIC, actualMaxSize);

        String mimeType = getMimeType("thumbnail." + outputFormat);
        if (mimeType == null || mimeType.equals("application/octet-stream")) {
            outputFormat = "jpg";
            mimeType = "image/jpeg";
        }

        final Thumbnail thumbnail = new Thumbnail();
        thumbnail.setThumbnailBytes(getBytesFromBufferedImage(tbImage, outputFormat));
        thumbnail.setMimeType(mimeType);
        thumbnail.setDimensions(getDimensions(tbImage));

        return thumbnail;
    }

    private static int getMaxThumbnailSize(ThumbnailSize size) {
        switch (size) {
            case SMALL:
                return 100;
            case MEDIUM:
                return 300;
            case LARGE:
                return 600;
            default:
                final String errMsg = "Unknown thumbnail size " + size + " given";
                logger.error(errMsg);
                throw new IllegalArgumentException(errMsg);
        }
    }
}
