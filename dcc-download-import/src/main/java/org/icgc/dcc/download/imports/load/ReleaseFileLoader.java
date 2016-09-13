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

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.icgc.dcc.download.imports.core.ArchiveFileType.RELEASE;
import static org.icgc.dcc.download.imports.util.TarArchiveStreams.getTarInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackContext;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;
import org.icgc.dcc.release.core.document.DocumentType;

import com.google.common.base.Stopwatch;

@Slf4j
@RequiredArgsConstructor
public class ReleaseFileLoader implements FileLoader {

  private static final Pattern TYPE_TAR_NAME_PATTERN = Pattern.compile("^(.*)_(.*)\\.tar\\.gz$");

  @Nullable
  private final String project;
  @NonNull
  private final TarArchiveEntryCallbackFactory callbackFactory;
  @NonNull
  private final TarArchiveDocumentReaderFactory readerFactory;

  @Override
  @SneakyThrows
  public void loadFile(@NonNull File file) {
    @Cleanup
    val tarInput = getTarInputStream(file);

    TarArchiveEntry tarEntry;
    boolean applySettings = true;
    while ((tarEntry = tarInput.getNextTarEntry()) != null) { // NOPMD
      processTypeTarEntry(tarInput, tarEntry, applySettings);
      applySettings = false;
    }
  }

  private void processTypeTarEntry(InputStream inputStream, TarArchiveEntry tarEntry, boolean applySettings)
      throws IOException {
    val entryName = tarEntry.getName();

    log.debug("Creating tar document reader for tar entry {}", entryName);
    val reader = readerFactory.createReader(inputStream);
    val documentType = resolveDocumentType(entryName);
    val indexName = resolveIndexName(entryName);

    log.info("Indexing file '{}' into index '{}'", entryName, indexName);
    // Don't use @Cleanup as the callback will be closed after the log message "Finished indexing file" has been
    // written.
    val callbackContext = TarArchiveEntryCallbackContext.builder()
        .indexName(indexName)
        .fileType(RELEASE)
        .applySettings(applySettings)
        .documentType(documentType)
        .project(project)
        .build();
    val callback = callbackFactory.createCallback(callbackContext);

    val typeWatches = Stopwatch.createStarted();
    try {
      reader.read(callback);
    } finally {
      callback.close();
    }

    // Apply index settings only once, but each tar entry contains own settings copy
    log.info("Finished indexing file {} in {} seconds.", entryName, typeWatches.elapsed(SECONDS));
  }

  private static String resolveIndexName(String entryName) {
    log.debug("Resolving indexName from tar name {}", entryName);
    val matcher = TYPE_TAR_NAME_PATTERN.matcher(entryName);
    checkState(matcher.matches(), "Failed to resolve index name from tar name %s", entryName);

    return matcher.group(1);
  }

  private static DocumentType resolveDocumentType(String entryName) {
    log.debug("Resolving document type from tar name {}", entryName);
    val matcher = TYPE_TAR_NAME_PATTERN.matcher(entryName);
    checkState(matcher.matches(), "Failed to resolve document type from tar name %s", entryName);

    val documentTypeName = matcher.group(2);
    log.debug("Getting document from document type name '{}'", documentTypeName);

    return DocumentType.byName(documentTypeName);
  }

}
