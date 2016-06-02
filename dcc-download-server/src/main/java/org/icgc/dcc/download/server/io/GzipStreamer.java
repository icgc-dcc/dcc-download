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

import static com.google.common.base.Preconditions.checkArgument;
import static org.icgc.dcc.common.core.util.Joiners.PATH;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;

@Slf4j
public class GzipStreamer extends InputStream {

  /**
   * Constants.
   */
  private static final int EOS = -1; // END_OF_STREAM

  /**
   * Dependencies.
   */
  protected final FileSystem fileSystem;
  protected final List<DataTypeFile> downloadFiles;
  protected final Map<DownloadDataType, Long> fileSizes;
  protected final Map<DownloadDataType, String> headers;

  /**
   * State.
   */
  private int currentDataFileIndex = 0;
  private int currentPartFileIndex = 0;
  private InputStream currentInputStream;
  private boolean header = false;

  public GzipStreamer(
      @NonNull FileSystem fileSystem,
      @NonNull List<DataTypeFile> downloadFiles,
      @NonNull Map<DownloadDataType, Long> fileSizes,
      @NonNull Map<DownloadDataType, String> headers) {
    checkArgument(headers.size() == 1);
    this.fileSystem = fileSystem;
    this.downloadFiles = downloadFiles;
    this.fileSizes = fileSizes;
    this.headers = headers;
    checkArguments();
    setFirstHeader();
  }

  @Override
  public void close() throws IOException {
    if (currentInputStream != null) {
      currentInputStream.close();
    }
  }

  @Override
  public int read() throws IOException {
    int nextByte = currentInputStream.read();

    if (nextByte == EOS) {
      log.debug("Reached end of current input stream.");
      if (hasMoreInput()) {
        log.debug("The reader has more input.");
        setUpNextInputStream();
        nextByte = currentInputStream.read();
      } else {
        log.debug("No more input to read. Returning END_OF_STREAM");
        return EOS;
      }
    }

    return nextByte;
  }

  private void checkArguments() {
    checkArgument(!downloadFiles.isEmpty());
    checkArgument(!downloadFiles.get(0).getPartFiles().isEmpty());
  }

  @SneakyThrows
  private void setFirstHeader() {
    val currentDownloadDataType = getCurrentDownloadDataType();
    val headerFile = new Path(headers.get(currentDownloadDataType));
    log.debug("Setting input stream to {}", headerFile);
    setInputStream(headerFile);
    header = true;
  }

  private boolean hasMoreInput() {
    // The very first header
    if (isFirstHeaderFile()) {
      return true;
    }

    return !isLastPartFile() || !isLastDataFile();
  }

  /**
   * Checks if the current input stream is the very first header file.
   */
  private boolean isFirstHeaderFile() {
    return header && currentDataFileIndex == 0 && currentPartFileIndex == 0;
  }

  private void setUpNextInputStream() throws IOException {
    log.debug("Closing current input stream...");
    currentInputStream.close();

    if (isCurrentHeaderFile()) {
      log.debug("Current input stream was header. Setting up next data type...");
      setNextDataFile();
    }

    else if (isNextDataType()) {
      val headerFile = getNextHeaderFile();
      log.debug("Setting up header as next input: {}", headerFile);
      setInputStream(headerFile);
    }

    else if (isLastPartFile()) {
      setNextDataFile();
    }

    else {
      val currentDataFile = getCurrentDataFile();
      ++currentPartFileIndex;
      val path = getPath(currentDataFile, currentPartFileIndex);
      log.debug("Setting up next part file as next input: {}", path);
      setInputStream(path);
    }
  }

  /**
   * Sets the next {@code DataTypeFile} from the {@code downloadFiles} list as the {@code currentInputStream}.
   */
  private void setNextDataFile() throws IOException {
    val nextDataFile = getNextDataFile();
    log.debug("Next data type file is {}", nextDataFile);
    ++currentDataFileIndex;
    currentPartFileIndex = 0;
    val path = getPath(nextDataFile, currentPartFileIndex);
    setInputStream(path);
  }

  private Path getNextHeaderFile() {
    val nextDownloadDataType = getNextDownloadDataType();
    return new Path(headers.get(nextDownloadDataType));
  }

  private void setInputStream(Path path) throws IOException {
    currentInputStream = fileSystem.open(path);
  }

  private boolean isCurrentHeaderFile() {
    return header;
  }

  /**
   * Checks if the next {@code DataTypeFile} starts a new {@code DownloadDataType} stream.
   */
  private boolean isNextDataType() {
    return getCurrentDownloadDataType() != getNextDownloadDataType();
  }

  private DownloadDataType getCurrentDownloadDataType() {
    return getDownloadDataType(getCurrentDataFile());
  }

  private DownloadDataType getNextDownloadDataType() {
    return getDownloadDataType(getNextDataFile());
  }

  private boolean isLastDataFile() {
    return downloadFiles.size() == currentDataFileIndex + 1;
  }

  /**
   * Checks if the {@code currentInputStream} is the last part file in the current {@code DataTypeFile}.
   */
  private boolean isLastPartFile() {
    val currentDataFile = getCurrentDataFile();
    val partFiles = currentDataFile.getPartFiles();

    return partFiles.size() == currentPartFileIndex + 1;
  }

  private DataTypeFile getCurrentDataFile() {
    return downloadFiles.get(currentDataFileIndex);
  }

  private DataTypeFile getNextDataFile() {
    // Special case after the first header file
    if (isFirstHeaderFile()) {
      return downloadFiles.get(currentDataFileIndex);
    }

    return downloadFiles.get(currentDataFileIndex + 1);
  }

  private static Path getPath(DataTypeFile dataFile, int partFileIndex) {
    val partFileName = dataFile.getPartFiles().get(partFileIndex);
    val path = PATH.join(dataFile.getPath(), partFileName);
    log.debug("Resolved path '{}' from part file index '{}' and data file {}", path, partFileIndex, dataFile);

    return new Path(path);
  }

  private static DownloadDataType getDownloadDataType(DataTypeFile file) {
    // TODO: Is this cheaper to use Guava's PATH Splitter here?
    val dataTypeName = new Path(file.getPath()).getName();

    return DownloadDataType.valueOf(dataTypeName.toUpperCase());
  }

}
