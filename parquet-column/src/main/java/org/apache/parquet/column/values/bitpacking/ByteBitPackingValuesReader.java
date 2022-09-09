/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.column.values.bitpacking;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByteBitPackingValuesReader extends ValuesReader {
  private static final int VALUES_AT_A_TIME = 8; // because we're using unpack8Values()

  private static final Logger LOG = LoggerFactory.getLogger(ByteBitPackingValuesReader.class);

  private final int bitWidth;
  private final BytePacker packer;
  private final int[] decoded = new int[VALUES_AT_A_TIME];
  private int decodedPosition = VALUES_AT_A_TIME - 1;
  private ByteBufferInputStream in;
  private final byte[] tempEncode;

  public ByteBitPackingValuesReader(int bound, Packer packer) {
    this.bitWidth = BytesUtils.getWidthFromMaxInt(bound);
    this.packer = packer.newBytePacker(bitWidth);
    // Create and retain byte array to avoid object creation in the critical path
    this.tempEncode = new byte[this.bitWidth];
  }

  private void readMore() {
    try {
      int avail = in.available();
      if (avail < bitWidth) {
        in.read(tempEncode, 0, avail);
        // Clear the portion of the array we didn't read into
        for (int i=avail; i<bitWidth; i++) tempEncode[i] = 0;
      } else {
        in.read(tempEncode, 0, bitWidth);
      }

      // The "deprecated" unpacker is faster than using the one that takes ByteBuffer
      packer.unpack8Values(tempEncode, 0, decoded, 0);
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read packed values", e);
    }
    decodedPosition = 0;
  }

  @Override
  public int readInteger() {
    ++ decodedPosition;
    if (decodedPosition == decoded.length) {
      readMore();
    }
    return decoded[decodedPosition];
  }

  @Override
  public boolean readBoolean() {
    return readInteger() != 0;
  }

  // Heavily optimized array read method
  @Override
  public void readIntegers(int[] arr, int offset, int len) {
    int i = 0;
    int dl = decoded.length;
    int dlm1 = dl-1;

    // Use up any remainder of the decoded buffer
    if (decodedPosition < dlm1) {
      int c = dlm1 - decodedPosition;
      if (c < len) {
        // Using up all of the rest of the decided buffer and needing more
        System.arraycopy(decoded, decodedPosition + 1, arr, offset, c);
        i = c + offset;
        len -= c;
        // Don't need to do these since readMore replaces their values, so this is here for semantics
        //decodedPosition = dlm1;
        //c = 0;
      } else {
        // Need no more values than remain in the decoded buffer, so copy them and return.
        System.arraycopy(decoded, decodedPosition + 1, arr, offset, len);
        decodedPosition += len;
        return;
      }
    }

    // Read in whole buffer units until output array is filled
    while (true) {
      // If we've gotten here, the decoded buffer has been used up, so load another.
      readMore();

      if (dl < len) {
        // Using up the entire decoded buffer and needing more
        System.arraycopy(decoded, 0, arr, i, dl);
        i += dl;
        len -= dl;
        // Don't need to do these since decodeMore replaces their values, so this is here for semantics
        //decodedPosition = dlm1;
        //c = 0;
      } else {
        // Need no more values than remain in the decoded buffer, so copy them and return.
        System.arraycopy(decoded, 0, arr, i, len);
        decodedPosition = len - 1;
        return;
      }
    }
  }

  private static void boolCopy(int[] src, int src_pos, boolean[] dst, int dst_pos, int count) {
    for (int i=0; i<count; i++) {
      dst[dst_pos++] = 0 != src[src_pos++];
    }
  }

  @Override
  public void readBooleans(boolean[] arr, int offset, int len) {
    int i = 0;
    int dl = decoded.length;
    int dlm1 = dl-1;

    // Use up any remainder of the decoded buffer
    if (decodedPosition < dlm1) {
      int c = dlm1 - decodedPosition;
      if (c < len) {
        // Using up all of the rest of the decided buffer and needing more
        boolCopy(decoded, decodedPosition + 1, arr, offset, c);
        i = c + offset;
        len -= c;
        // Don't need to do these since readMore replaces their values, so this is here for semantics
        //decodedPosition = dlm1;
        //c = 0;
      } else {
        // Need no more values than remain in the decoded buffer, so copy them and return.
        boolCopy(decoded, decodedPosition + 1, arr, offset, len);
        decodedPosition += len;
        return;
      }
    }

    // Read in whole buffer units until output array is filled
    while (true) {
      // If we've gotten here, the decoded buffer has been used up, so load another.
      readMore();

      if (dl < len) {
        // Using up the entire decoded buffer and needing more
        boolCopy(decoded, 0, arr, i, dl);
        i += dl;
        len -= dl;
        // Don't need to do these since decodeMore replaces their values, so this is here for semantics
        //decodedPosition = dlm1;
        //c = 0;
      } else {
        // Need no more values than remain in the decoded buffer, so copy them and return.
        boolCopy(decoded, 0, arr, i, len);
        decodedPosition = len - 1;
        return;
      }
    }
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream stream)
      throws IOException {
    int effectiveBitLength = valueCount * bitWidth;
    int length = BytesUtils.paddedByteCountFromBits(effectiveBitLength); // ceil
    LOG.debug("reading {} bytes for {} values of size {} bits.",
        length, valueCount, bitWidth);
    // work-around for null values. this will not happen for repetition or
    // definition levels (never null), but will happen when valueCount has not
    // been adjusted for null values in the data.
    length = Math.min(length, stream.available());
    this.in = stream.sliceStream(length);
    this.decodedPosition = VALUES_AT_A_TIME - 1;
    updateNextOffset(length);
  }

  @Override
  public void skip() {
    readInteger();
  }

  @Override
  public void skip(int n) {
    int i = 0;
    int dl = decoded.length;
    int dlm1 = dl-1;

    // Use up any remainder of the decoded buffer
    if (decodedPosition < dlm1) {
      int c = dlm1 - decodedPosition;
      if (c < n) {
        // Using up all of the rest of the decided buffer and needing more
        i = c;
        n -= c;
        // Don't need to do these since decodeMore replaces their values, so this is here for semantics
        //decodedPosition = dlm1;
        //c = 0;
      } else {
        // Need no more values than remain in the decoded buffer, so copy them and return.
        decodedPosition += n;
        return;
      }
    }

    // Read in whole buffer units until output array is filled
    while (true) {
      // If we've gotten here, the decoded buffer has been used up, so load another.
      readMore();

      if (dl < n) {
        // Using up the entire decided buffer and needing more
        i += dl;
        n -= dl;
        // Don't need to do these since decodeMore replaces their values, so this is here for semantics
        //decodedPosition = dlm1;
        //c = 0;
      } else {
        // Need no more values than remain in the decoded buffer, so copy them and return.
        decodedPosition = n - 1;
        return;
      }
    }
  }

  @Override
  public int availableIntegers() {
    if (bitWidth == 0) return Integer.MAX_VALUE;
    return (VALUES_AT_A_TIME-1) - decodedPosition + in.available() * 8 / bitWidth;
  }

}
