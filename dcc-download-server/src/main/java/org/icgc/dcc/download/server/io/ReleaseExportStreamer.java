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

import static com.google.common.base.Preconditions.checkState;
import static java.util.regex.Pattern.compile;
import static org.icgc.dcc.common.hadoop.fs.HadoopUtils.lsFile;
import static org.icgc.dcc.download.server.utils.HadoopUtils2.getFileStatus;
import static org.icgc.dcc.download.server.utils.OutputStreams.createTarOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.regex.Pattern;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.io.ByteStreams;

@Slf4j
@RequiredArgsConstructor
public class ReleaseExportStreamer implements FileStreamer {

  private static final Pattern RELEASE_FILES_PATTERN = compile(".*\\.tar\\.gz");

  /**
   * Configuration.
   */
  @NonNull
  private final String archiveName;
  @NonNull
  private final Path exportPath;

  /**
   * Dependencies.
   */
  @NonNull
  private final FileSystem fileSystem;

  /**
   * State.
   */
  @NonNull
  private final OutputStream output;

  @Override
  public void close() throws IOException {
    output.close();
  }

  @Override
  @SneakyThrows
  public void stream() {
    log.info("Started streaming {}...", archiveName);
    val tarOutputStream = createTarOutputStream(output);
    val files = lsFile(fileSystem, exportPath, RELEASE_FILES_PATTERN);
    checkState(!files.isEmpty(), "Export release directory is empty.");

    for (val file : files) {
      val fileStatus = getFileStatus(fileSystem, file);
      val fileName = file.getName();
      log.info("Streaming {}...", fileName);

      val tarEntry = new TarArchiveEntry(fileName);
      tarEntry.setSize(fileStatus.getLen());
      tarOutputStream.putArchiveEntry(tarEntry);

      @Cleanup
      val fileInput = fileSystem.open(file);
      ByteStreams.copy(fileInput, tarOutputStream);

      tarOutputStream.closeArchiveEntry();
      log.info("Finished streaming {}.", fileName);
    }

    tarOutputStream.finish();
    tarOutputStream.flush();
    log.info("Finished streaming {}.", archiveName);
  }

  @Override
  public String getName() {
    return archiveName;
  }

}
