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
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;
import static org.icgc.dcc.download.server.utils.DfsPaths.getFileName;
import static org.icgc.dcc.download.server.utils.Responses.throwNotFoundException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.apache.catalina.connector.ClientAbortException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.core.DownloadException;
import org.icgc.dcc.download.server.fs.PathResolver;
import org.icgc.dcc.download.server.model.DataTypeFile;
import org.icgc.dcc.download.server.utils.DataTypeFiles;
import org.icgc.dcc.download.server.utils.HadoopUtils2;

import com.google.common.io.ByteStreams;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GzipStreamer implements FileStreamer {

  /**
   * Dependencies.
   */
  private final FileSystem fileSystem;
  private final List<DataTypeFile> downloadFiles;
  private final Map<DownloadDataType, Long> fileSizes;
  private final Map<DownloadDataType, String> headers;
  private final OutputStream output;
  private final PathResolver pathResolver;
  private final String release;
  private final Map<DownloadDataType, String> fileNames;

  /**
   * State.
   */
  private int currentDataFileIndex; // Not setting default value, because of the RedundantFieldInitializer PMD warning

  public GzipStreamer(
      @NonNull FileSystem fileSystem,
      @NonNull List<DataTypeFile> downloadFiles,
      @NonNull Map<DownloadDataType, Long> fileSizes,
      @NonNull Map<DownloadDataType, String> headers,
      @NonNull OutputStream output,
      @NonNull PathResolver pathResolver,
      @NonNull String release,
      @NonNull Map<DownloadDataType, String> fileNames) {
    this.fileSystem = fileSystem;
    this.downloadFiles = downloadFiles;
    this.fileSizes = fileSizes;
    this.headers = headers;
    this.output = output;
    this.pathResolver = pathResolver;
    this.release = release;
    this.fileNames = fileNames;
    checkArguments();
  }

  @Override
  public void stream() {
    streamEntry();
  }

  @Override
  public String getName() {
    return getNextEntryName();
  }

  @Override
  public void close() throws IOException {
    output.close();
  }

  public boolean hasNext() {
    return currentDataFileIndex < downloadFiles.size();
  }

  public String getNextEntryName() {
    val downloadDataType = getCurrentDownloadDataType();
    val nextEntryName = fileNames.containsKey(downloadDataType) ?
        fileNames.get(downloadDataType) :
        getFileName(downloadDataType, empty()) + ".tsv.gz";

    log.debug("Next entry name: {}", nextEntryName);

    return nextEntryName;
  }

  public long getNextEntryLength() {
    val headerLength = getCurrentHeaderLength();
    val currentDownloadDataType = getCurrentDownloadDataType();
    val filesSize = fileSizes.get(currentDownloadDataType);
    checkNotNull(fileSizes, "Failed to resolve file size for data type '%s'", currentDownloadDataType);

    val nextEntryLength = headerLength + filesSize;
    log.debug("Next entry length is {} bytes.", nextEntryLength);

    return nextEntryLength;
  }

  public void streamEntry() {
    try {
      val currentDownloadDataType = getCurrentDownloadDataType();
      log.debug("Streaming '{}' entry...", currentDownloadDataType.getCanonicalName());
      streamHeader();

      while (hasNext() && isSameDownloadDataType(currentDownloadDataType)) {
        streamCurrentDataType();
        currentDataFileIndex++;
      }
      log.debug("Finished Streaming '{}' entry.", currentDownloadDataType.getCanonicalName());
    } catch (ClientAbortException e) {
      log.info("Archive streaming cancelled by the client.");
    } catch (Exception e) { // NOPMD
      log.error("Got exception while streaming entry: ", e);
      throw new DownloadException("An error occurred while streaming. Please contact the support.", e);
    }
  }

  private boolean isSameDownloadDataType(DownloadDataType currentDownloadDataType) {
    return getCurrentDownloadDataType() == currentDownloadDataType;
  }

  private void streamCurrentDataType() throws IOException {
    log.debug("Streaming data file '{}'", getCurrentDataFile().getPath());
    for (val partFile : getPartFiles()) {
      val path = new Path(partFile); // NOPMD
      log.debug("Streaming path '{}'", path);

      @Cleanup
      val input = fileSystem.open(path);
      ByteStreams.copy(input, output);
    }
  }

  private List<String> getPartFiles() {
    val dataFile = getCurrentDataFile();

    return pathResolver.getPartFilePaths(release, dataFile);
  }

  private void streamHeader() throws IOException {
    val header = getCurrentHeader();
    log.debug("Streaming header '{}'", header);

    @Cleanup
    val headerInput = fileSystem.open(header);
    ByteStreams.copy(headerInput, output);
  }

  private long getCurrentHeaderLength() {
    val header = getCurrentHeader();
    val status = HadoopUtils2.getFileStatus(fileSystem, header);

    return status.getLen();
  }

  private void checkArguments() {
    if (downloadFiles.isEmpty()) {
      throwNotFoundException("Job has no files to download",
          "In GzipStreamer.checkArguments(), job has no files to download");
    }
    checkArgument(!downloadFiles.get(0).getPartFileIndices().isEmpty());
    checkArgument(!headers.isEmpty());
  }

  private Path getCurrentHeader() {
    val currentDownloadDataType = getCurrentDownloadDataType();

    return new Path(headers.get(currentDownloadDataType));
  }

  private DownloadDataType getCurrentDownloadDataType() {
    return DataTypeFiles.getDownloadDataType(getCurrentDataFile());
  }

  private DataTypeFile getCurrentDataFile() {
    return downloadFiles.get(currentDataFileIndex);
  }

}
