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

import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.getFileStatus;
import static org.icgc.dcc.download.server.model.Export.DATA_CONTROLLED;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.relativize;
import static org.icgc.dcc.download.server.utils.OutputStreams.createTarOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.icgc.dcc.download.server.model.Export;

import com.google.common.io.ByteStreams;

@RequiredArgsConstructor
public class DataExportStreamer implements FileStreamer {

  /**
   * Configuration.
   */
  @NonNull
  private final Path dataPath;
  @NonNull
  private final Export export;
  @NonNull
  private final FileSystem fileSystem;
  @NonNull
  private final OutputStream output;
  @NonNull
  private final Optional<String> project;

  @Override
  public void close() throws IOException {
    output.close();
  }

  @Override
  public String getName() {
    return export.getId();
  }

  @Override
  @SneakyThrows
  public void stream() {
    val tarOutputStream = createTarOutputStream(output);
    val dataDirStatus = getFileStatus(fileSystem, dataPath).get();
    val parentPath = dataPath.toString();

    streamDir(tarOutputStream, dataDirStatus, parentPath);
    tarOutputStream.finish();
    tarOutputStream.flush();
  }

  private void streamDir(TarArchiveOutputStream tarOutputStream, FileStatus dir, String parentPath) {
    for (val status : getFiles(dir.getPath())) {
      if (status.isDirectory()) {
        streamDir(tarOutputStream, status, parentPath);
      } else {
        streamFile(tarOutputStream, status, parentPath);
      }
    }
  }

  @SneakyThrows
  private FileStatus[] getFiles(Path path) {
    return isProjectPath(path) ? getProjectFiles(path) : fileSystem.listStatus(path);
  }

  @SneakyThrows
  private FileStatus[] getProjectFiles(Path path) {
    val projectName = project.get();

    return fileSystem.listStatus(path, p -> p.getName().equals(projectName));
  }

  @SneakyThrows
  private void streamFile(TarArchiveOutputStream tarOutputStream, FileStatus status, String parentPath) {
    val filePath = status.getPath();
    val fileName = relativize(parentPath, filePath);
    if (!isAuthorized(fileName)) {
      return;
    }

    val tarEntry = new TarArchiveEntry(fileName);
    tarEntry.setSize(status.getLen());

    tarOutputStream.putArchiveEntry(tarEntry);

    @Cleanup
    val fileInput = fileSystem.open(filePath);
    ByteStreams.copy(fileInput, tarOutputStream);

    tarOutputStream.closeArchiveEntry();
  }

  private boolean isAuthorized(String fileName) {
    return DATA_CONTROLLED == export || !isControlled(fileName);
  }

  private static boolean isControlled(String fileName) {
    return fileName.contains("controlled");
  }

  private boolean isProjectPath(Path path) {
    return project.isPresent() && path.getName().equals("data");
  }

}
