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
package org.icgc.dcc.download.server.service;

import static com.google.common.base.Objects.firstNonNull;
import static org.icgc.dcc.common.core.util.Joiners.PATH;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableMap;
import static org.icgc.dcc.download.server.utils.DownloadDirectories.HEADERS_DIR;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.server.model.DataTypeFile;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

@RequiredArgsConstructor
public class ArchiveDownloadService {

  @NonNull
  private final Path rootPath;
  @NonNull
  private final FileSystemService fileSystemService;
  @NonNull
  private final FileSystem fileSystem;

  public InputStream downloadArchive(@NonNull String release, @NonNull Collection<String> donors,
      @NonNull Collection<DownloadDataType> dataTypes) {
    // resolve files
    val downloadFiles = fileSystemService.getDataTypeFiles(release, donors, dataTypes);

    // resolve size for each dataType
    val fileSizes = resolveFileSizes(downloadFiles);

    // resolve headers for the dataTypes
    val headers = resolveHeaders(release, dataTypes);

    // stream back as gzip if single dataType, as tar if multiple datatypes
    // during streaming insert headers and create tar entities when required.

    // Convert OutputStream to InputStream http://blog.ostermiller.org/convert-java-outputstream-inputstream
    if (headers.size() == 1) {
      return streamGzip(downloadFiles, fileSizes, headers);
    } else {
      return streamTar(downloadFiles, fileSizes, headers);
    }
  }

  private InputStream streamTar(List<DataTypeFile> downloadFiles, Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, Path> headers) {
    // TODO Auto-generated method stub
    return null;
  }

  private InputStream streamGzip(List<DataTypeFile> downloadFiles, Map<DownloadDataType, Long> fileSizes,
      Map<DownloadDataType, Path> headers) {
    // TODO Auto-generated method stub
    return null;
  }

  private Map<DownloadDataType, Path> resolveHeaders(String release, Collection<DownloadDataType> dataTypes) {
    return dataTypes.stream()
        .collect(toImmutableMap(dataType -> dataType, dataType -> getHeaderPath(release, dataType)));

  }

  private Path getHeaderPath(String release, DownloadDataType dataType) {
    val headerPath = PATH.join(release, HEADERS_DIR, dataType.getId() + ".tsv");

    return new Path(rootPath, headerPath);
  }

  private Map<DownloadDataType, Long> resolveFileSizes(List<DataTypeFile> downloadFiles) {
    val fileSizes = Maps.<DownloadDataType, Long> newHashMap();
    for (val file : downloadFiles) {
      val type = resolveType(file);
      val size = firstNonNull(fileSizes.get(type), 0L);
      fileSizes.put(type, size + file.getTotalSize());
    }

    return ImmutableMap.copyOf(fileSizes);
  }

  private static DownloadDataType resolveType(DataTypeFile file) {
    val name = new Path(file.getPath()).getName();

    return DownloadDataType.valueOf(name.toUpperCase());
  }

}
