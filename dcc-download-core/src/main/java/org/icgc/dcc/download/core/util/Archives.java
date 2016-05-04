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
package org.icgc.dcc.download.core.util;

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.List;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.common.hadoop.fs.HadoopUtils;
import org.icgc.dcc.download.core.model.DownloadDataType;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class Archives {

  public static long resolveDownloadSize(@NonNull FileSystem fileSystem, @NonNull Path jobPath) {
    if (!HadoopUtils.exists(fileSystem, jobPath)) {
      log.warn("Failed to calculate download size for non-existing path '{}'", jobPath);

      return 0;
    }

    return getDataTypePaths(fileSystem, jobPath).stream()
        .mapToLong(dtPath -> calculateDataTypeArchiveSize(fileSystem, dtPath))
        .sum();
  }

  private static List<Path> getDataTypePaths(FileSystem fileSystem, Path jobPath) {
    return HadoopUtils.lsDir(fileSystem, jobPath).stream()
        .filter(path -> DownloadDataType.canCreateFrom(path.getName().toUpperCase()))
        .collect(toImmutableList());
  }

  @SneakyThrows
  public static long calculateDataTypeArchiveSize(FileSystem fileSystem, Path downloadTypePath) {
    val files = fileSystem.listFiles(downloadTypePath, false);

    long totalSize = 0L;
    while (files.hasNext()) {
      val file = files.next();
      if (isPartFile(file.getPath())) {
        totalSize += file.getLen();
      }
    }

    return totalSize;
  }

  // TODO: move to commons hadoop
  public static boolean isPartFile(Path path) {
    return path.getName().startsWith("part-");
  }

}
