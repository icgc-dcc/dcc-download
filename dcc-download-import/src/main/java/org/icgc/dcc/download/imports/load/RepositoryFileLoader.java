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
package org.icgc.dcc.download.imports.load;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.icgc.dcc.download.imports.core.ArchiveFileType.REPOSITORY;
import static org.icgc.dcc.download.imports.util.TarEntryNames.getIndexName;

import java.io.File;
import java.io.FileInputStream;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryContext;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;
import org.icgc.dcc.download.imports.util.TarArchiveStreams;

import com.google.common.base.Stopwatch;

@Slf4j
@RequiredArgsConstructor
public class RepositoryFileLoader implements FileLoader {

  @NonNull
  private final TarArchiveEntryCallbackFactory callbackFactory;
  @NonNull
  private final TarArchiveDocumentReaderFactory readerFactory;

  @Override
  @SneakyThrows
  public void loadFile(@NonNull File file) {
    val callbackContext = TarArchiveEntryContext.builder()
        .fileType(REPOSITORY)
        .applySettings(true)
        .indexName(resolveIndexName(file))
        .build();
    @Cleanup
    val callback = callbackFactory.createCallback(callbackContext);

    log.debug("Creating tar document reader for file {}", file);
    val reader = readerFactory.createReader(new FileInputStream(file));
    val watch = Stopwatch.createStarted();
    reader.read(callback);
    log.info("Finished indexing file {} in {} seconds.", file, watch.elapsed(SECONDS));
  }

  @SneakyThrows
  private String resolveIndexName(File file) {
    @Cleanup
    val archiveStream = TarArchiveStreams.getTarGzInputStream(file);
    val entry = archiveStream.getNextTarEntry();
    checkNotNull(entry, "Failed to resolve index name from file '%s'", file);
    val entryName = entry.getName();

    return getIndexName(entryName);
  }

}
