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
package org.icgc.dcc.download.imports.command.index;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.io.File;
import java.io.FileInputStream;
import java.util.regex.Pattern;

import lombok.Cleanup;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.icgc.dcc.download.imports.command.ClientCommand;
import org.icgc.dcc.download.imports.core.DownloadImportException;
import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;
import org.icgc.dcc.release.core.document.DocumentType;

@Slf4j
@RequiredArgsConstructor
public class IndexClientCommand implements ClientCommand {

  private static final Pattern TYPE_TAR_NAME_PATTERN = Pattern.compile("^(.*)_(.*)\\.tar\\.gz$");

  @NonNull
  private final File inputFile;
  @NonNull
  private final TarArchiveEntryCallbackFactory callbackFactory;
  @NonNull
  private final TarArchiveDocumentReaderFactory readerFactory;

  @Override
  @SneakyThrows
  public void execute() {
    log.info("Creating tar reader for file {}", inputFile);
    @Cleanup
    val tarInput = getTarInputStream(inputFile);

    TarArchiveEntry tarEntry = null;
    while ((tarEntry = tarInput.getNextTarEntry()) != null) {
      val entryName = tarEntry.getName();
      val entrySize = tarEntry.getSize();

      log.debug("Creating tar document reader for tar entry {}", entryName);
      val reader = readerFactory.createReader(tarInput, entrySize);
      val documentType = resolveDocumentType(entryName);
      val indexName = resolveIndexName(entryName);

      log.info("Indexing file '{}' into index '{}'", entryName, indexName);
      reader.read(documentType, callbackFactory.createCallback(indexName));
      log.info("Finished indexing file {}", entryName);
    }

    log.info("Finished processing {}", inputFile);
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

  @SneakyThrows
  private static TarArchiveInputStream getTarInputStream(File inputFile) {
    checkFileReadability(inputFile);

    return new TarArchiveInputStream(new FileInputStream(inputFile));
  }

  private static void checkFileReadability(File inputFile) {
    if (!inputFile.exists()) {
      throw new DownloadImportException(format("Input file '%s' doesn't exist.", inputFile));
    }

    if (!inputFile.canRead()) {
      throw new DownloadImportException(format("Input file '%s' is not readable.", inputFile));
    }
  }

}
