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
package org.apache.parquet.column.values.rle;


import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Decodes values written in the grammar described in {@link RunLengthBitPackingHybridEncoder}
 */
public class RunLengthBitPackingHybridDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(RunLengthBitPackingHybridDecoder.class);

  private final int bitWidth;
  private final BytePacker packer;
  private final ByteBufferInputStream in;
  boolean packed_mode;

  private int currentCount;
  private int currentValue;
  private int[] currentBuffer;

  // We look ahead one group or run in order so that availableIntegers reports
  // a larger count.
  private int nextHeader = -1;
  private int extraCount = 0;

  public RunLengthBitPackingHybridDecoder(int bitWidth, InputStream in) {
    LOG.debug("decoding bitWidth {}", bitWidth);

    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
    this.in = (ByteBufferInputStream)in;
  }

  public int availableIntegers() {
    if (currentCount == 0) {
      // If we've used up the run or group, see if we can decode the next one
      // from the input stream.
      try {
        if (in.available() < 1) return 0;
        readNext();
      } catch (IOException x) {
        return 0;
      }
    }

    return currentCount + extraCount;
  }

  public int readInt() throws IOException {
    if (currentCount == 0) {
      readNext();
    }
    -- currentCount;
    int result;
    if (packed_mode) {
      return currentBuffer[currentBuffer.length - 1 - currentCount];
    } else {
      return currentValue;
    }
  }

  public boolean readBoolean() throws IOException {
    return readInt() != 0;
  }

  public void readBooleans(boolean[] arr, int offset, int len) throws IOException {
    int s = offset;
    int e = offset + len;
    for (int i=s; i<e; i++) {
      arr[i] = readInt() != 0;
    }
  }


  public void skip() throws IOException {
    if (currentCount == 0) {
      readNext();
    }
    -- currentCount;
  }

  public void skip(int n) throws IOException {
    while (true) {
      if (n <= currentCount) {
        // If we need to skip no more than the current group/run, then
        // decrement the count and exit.
        currentCount -= n;
        return;
      } else {
        // If we need to skip more than the current group/run, then
        // deduct the remaining count and then fetch the next group/run.
        n -= currentCount;
        //currentCount = 0;  // Semantically, this happens but is handled by readNext()
        readNext();
      }
    }
  }

  // Heavily optimized array read method.
  public void readInts(int[] arr, int offset, int len) throws IOException {
    if (len < 1) return;
    if (currentCount == 0) readNext();

    int ix = offset;
    while (true) {
      if (len <= currentCount) {
        // If we need no more values than the current run/group, then copy them and exit
        if (!packed_mode) {
          int e = ix+len;
          for (int i=ix; i<e; i++) arr[i] = currentValue;
        } else {
          System.arraycopy(currentBuffer, currentBuffer.length - currentCount, arr, ix, len);
        }
        currentCount -= len;
        return;
      } else {
        // If we need more values than the current run/group, then copy what we have and then
        // fetch more.
        if (!packed_mode) {
          int e = ix + currentCount;
          for (int i=ix; i<e; i++) arr[i] = currentValue;
          ix = e;
        } else {
          System.arraycopy(currentBuffer, currentBuffer.length - currentCount, arr, ix, currentCount);
          ix += currentCount;
        }
        len -= currentCount;
        //currentCount = 0;  // Semantically, this happens but is handled by readNext()
        readNext();
      }
    }
  }

  private void readNext() throws IOException {
    Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");

    int header;
    if (nextHeader != -1) {
      // If we pre-fetched a header, use it now.
      header = nextHeader;
      nextHeader = -1;
    } else {
      // Otherwise, fetch a header byte
      header = in.readUnsignedVarInt();
    }
    extraCount = 0;

    packed_mode = (header & 1) != 0;
    if (!packed_mode) {
      currentCount = header >>> 1;
      LOG.debug("reading {} values RLE", currentCount);
      currentValue = in.readIntLittleEndianPaddedOnBitWidth(bitWidth);
    } else {
      int numGroups = header >>> 1;
      currentCount = numGroups * 8;
      LOG.debug("reading {} values BIT PACKED", currentCount);
      currentBuffer = new int[currentCount]; // TODO: reuse a buffer
      byte[] bytes = new byte[numGroups * bitWidth];
      // At the end of the file RLE data though, there might not be that many bytes left.
      int bytesToRead = (int)Math.ceil(currentCount * bitWidth / 8.0);
      bytesToRead = Math.min(bytesToRead, in.available());
      in.readFully(bytes, 0, bytesToRead);
      for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += bitWidth) {
        // It's faster to use an array with unpack8Values than to use a ByteBuffer.
        packer.unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
      }
    }

    // Look ahead to see if we can get another header byte. We do this so that availableIntegers()
    // can report a larger value. It would be nice to read even further ahead, but this is a relatively
    // uninvasive change.
    if (in.available() > 0) {
      nextHeader = in.readUnsignedVarInt();
      if ((nextHeader & 1) == 0) {
        extraCount = nextHeader >>> 1;
      } else {
        extraCount = (nextHeader >>> 1) * 8;
      }
    }
  }
}
