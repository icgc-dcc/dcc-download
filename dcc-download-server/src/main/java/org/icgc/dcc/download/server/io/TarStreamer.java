/*
 * Copyright (c) 2016 The Ontario Institute for Cancer Research. All rights reserved.                             
 *                                                                                                               
 * This program and the accompanying materials are made available under the terms of the GNU Public License v3.0.
 * You should have received a copy of the GNU General Public License along with                                  
 * this program. If not, see <http://www.gnu.org/licenses/>.                                                     
 *                                                                                                               
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY                           
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES                          
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT                           
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,                                
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED                          
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;                               
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER                              
 * IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN                         
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.icgc.dcc.download.server.io;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;
import lombok.val;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;

public class TarStreamer extends GzipStreamer {

  /**
   * State.
   */
  private final ByteArrayOutputStream bufferStream;
  private final TarArchiveOutputStream tarOut;

  private boolean header = false;
  private boolean readBuffer = true;
  private Buffer buffer;
  private Integer headerFirstByte;

  public TarStreamer(FileSystem fileSystem, List<DataTypeFile> downloadFiles, Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, String> headers) {
    super(fileSystem, downloadFiles, fileSizes, headers);
    this.bufferStream = new ByteArrayOutputStream();
    this.tarOut = createTarOutputStream(bufferStream);
    nextTarEntry();
  }

  @Override
  public int read() throws IOException {
    if (readBuffer) {
      if (buffer.hasNext()) {
        return buffer.next();
      } else {
        readBuffer = false;
      }
    }

    if (isFirstHeaderFile()) {
      return super.read();
    }

    // headerFirstByte which was read to initiate tar entry creation. Now actually sending it to the client
    if (headerFirstByte != null) {
      int nextByte = headerFirstByte;
      headerFirstByte = null;

      return nextByte;
    }

    // Has to read the byte first. Otherwise, the upstream GzipStreamer will not change
    val nextByte = super.read();

    if (isDataToHeaderSwitch()) {
      headerFirstByte = nextByte;
      header = true;
      nextTarEntry();

      return buffer.next();
    } else if (isHeaderToDataSwitch()) {
      header = false;
    }

    return nextByte;
  }

  private boolean isHeaderToDataSwitch() {
    return !isCurrentHeaderFile() && header;
  }

  private boolean isDataToHeaderSwitch() {
    return isCurrentHeaderFile() && !header;
  }

  @SneakyThrows
  private void nextTarEntry() {
    // Create next tar entry
    val dataType = getCurrentDownloadDataType();
    val fileName = getFileName(dataType);
    val fileSize = fileSizes.get(dataType);
    addArchiveEntry(fileName, fileSize);

    // Send the tar entry to the buffer stream.
    tarOut.flush();

    // Save buffer stream content to the buffer and reset the buffer stream
    buffer = new Buffer(bufferStream.toByteArray());
    bufferStream.reset();
    readBuffer = true;
  }

  @SneakyThrows
  private void addArchiveEntry(String filename, long fileSize) {
    val entry = new TarArchiveEntry(filename);
    entry.setSize(fileSize);
    tarOut.putArchiveEntry(entry);
  }

  @SneakyThrows
  private void closeArchiveEntry() {
    tarOut.closeArchiveEntry();
  }

  // TODO: explicitly specify on the pom
  private static TarArchiveOutputStream createTarOutputStream(OutputStream out) {
    val tarOut = new TarArchiveOutputStream(new BufferedOutputStream(out));
    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOut;
  }

  private static String getFileName(DownloadDataType dataType) {
    return dataType.getCanonicalName() + ".gz";
  }

  private static class Buffer {

    private final byte[] data;
    private int position = 0;

    @SneakyThrows
    public Buffer(byte[] data) {
      this.data = data;
    }

    public boolean hasNext() {
      return data.length > position;
    }

    public int next() {
      return data[position++] & 0xFF;
    }

  }

}
