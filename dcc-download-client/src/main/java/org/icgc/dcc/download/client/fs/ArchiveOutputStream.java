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
package org.icgc.dcc.download.client.fs;

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.isPartFile;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.icgc.dcc.download.core.util.Archives;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;

@Slf4j
@RequiredArgsConstructor
public class ArchiveOutputStream {

  private static final Pattern PART_FILE_REGEX = Pattern.compile("part-(\\d{5}).*");

  // TODO: Add to common Extensions
  private static final String GZIP_EXTENSION = ".tsv.gz";

  @NonNull
  private final Path dynamicDownloadPath;
  @NonNull
  private final FileSystem fileSystem;

  public boolean streamArchiveInTarGz(OutputStream out, String downloadId, List<DownloadDataType> downloadedDataTypes) {
    try {
      val downloadPath = new Path(dynamicDownloadPath, downloadId);
      FileStatus[] downloadTypes = fileSystem.listStatus(downloadPath);
      if (downloadTypes == null) {
        return false;
      }

      val entitySizes = resolveDownloadTypesSize(downloadId, downloadedDataTypes, downloadPath);

      @Cleanup
      val tarOutputStream = createTarOutputStream(out);
      for (val dataType : downloadedDataTypes) {
        addArchiveEntry(tarOutputStream, dataType.getId() + GZIP_EXTENSION, entitySizes.get(dataType));
        streamArchiveInGz(tarOutputStream, downloadId, dataType);
        closeArchiveEntry(tarOutputStream);
      }

      return true;
    } catch (Exception e) {
      log.error("Fail to produce an archive for download id '{}', download data types - {}.\n{}", downloadId,
          downloadedDataTypes, e);
    }

    return false;
  }

  private static TarArchiveOutputStream createTarOutputStream(OutputStream out) {
    val tarOut = new TarArchiveOutputStream(new BufferedOutputStream(out));
    tarOut.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOut.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);

    return tarOut;
  }

  private Map<DownloadDataType, Long> resolveDownloadTypesSize(String downloadId,
      List<DownloadDataType> downloadedDataTypes, Path downloadPath) throws IOException {
    val entitySizesBuilder = ImmutableMap.<DownloadDataType, Long> builder();
    for (val dataType : downloadedDataTypes) {
      Path downloadTypePath = new Path(downloadPath, dataType.getId());
      if (fileSystem.exists(downloadTypePath)) {
        // The directory name is the data type index name
        log.info("Trying to download data for download ID {} and  Data Type '{}'", downloadId, dataType);
        val size = Archives.calculateDataTypeArchiveSize(fileSystem, downloadTypePath);
        entitySizesBuilder.put(dataType, size);
      }
    }

    return entitySizesBuilder.build();
  }

  public boolean streamArchiveInGz(@NonNull OutputStream out, @NonNull String downloadId,
      @NonNull DownloadDataType dataType) {
    try {
      val dataTypePath = new Path(dynamicDownloadPath, new Path(downloadId, dataType.getId()));
      val files = fileSystem.listFiles(dataTypePath, false);
      val paths = Lists.<Path> newArrayList();

      while (files.hasNext()) {
        val filePath = files.next().getPath();
        if (isPartFile(filePath)) {
          paths.add(filePath);
        }
      }

      concatGZipFiles(fileSystem, out, orderingByPartFile().sortedCopy(paths));

      return true;
    } catch (Exception e) {
      log.error("Fail to stream archive. DownloadID: " + downloadId, e);
    }

    return false;
  }

  @SneakyThrows
  private static void addArchiveEntry(TarArchiveOutputStream os,
      String filename, long fileSize) {
    TarArchiveEntry entry = new TarArchiveEntry(filename);
    entry.setSize(fileSize);
    os.putArchiveEntry(entry);
  }

  @SneakyThrows
  private static void closeArchiveEntry(TarArchiveOutputStream os) {
    os.closeArchiveEntry();
  }

  private static void concatGZipFiles(FileSystem fs, OutputStream out, List<Path> files) throws IOException {
    for (val file : files) {
      @Cleanup
      val in = fs.open(file);
      ByteStreams.copy(in, out);
    }
  }

  private static Ordering<Path> orderingByPartFile() {
    return new Ordering<Path>() {

      @Override
      public int compare(Path left, Path right) {
        val leftIndex = getPartFileIndex(left.getName());
        val rightIndex = getPartFileIndex(right.getName());

        return Ints.compare(leftIndex, rightIndex);
      }
    };
  }

  private static int getPartFileIndex(String fileName) {
    log.debug("Extracting part file index from '{}'", fileName);
    val matcher = PART_FILE_REGEX.matcher(fileName);
    checkState(matcher.find(), "Failed to resolve part file index from file name '%s'", fileName);
    val index = matcher.group(1);
    log.debug("Part file index: {}", index);

    return Integer.parseInt(index);
  }
}
