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

import static org.icgc.dcc.download.imports.core.ArchiveFileType.getFileNames;

import java.io.File;

import javax.annotation.Nullable;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.imports.core.ArchiveFileType;
import org.icgc.dcc.download.imports.core.DownloadImportException;
import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;

@Slf4j
@RequiredArgsConstructor
public class FileLoaderFactory {

  @Nullable
  private final String project;
  @NonNull
  private final TarArchiveEntryCallbackFactory callbackFactory;
  @NonNull
  private final TarArchiveDocumentReaderFactory readerFactory;

  public FileLoader getFileLoader(@NonNull File file) {
    val fileType = resolveArchiveFileType(file);
    log.debug("Resolved file type {} from file '{}'", fileType, file.getAbsolutePath());

    switch (fileType) {
    case RELEASE:
      return new ReleaseFileLoader(project, callbackFactory, readerFactory);
    case REPOSITORY:
      return new RepositoryFileLoader(callbackFactory, readerFactory);
    default:
      // Won't get here
      throw new IllegalArgumentException();
    }
  }

  private static ArchiveFileType resolveArchiveFileType(File file) {
    try {
      return ArchiveFileType.from(file.getName());
    } catch (IllegalArgumentException e) {
      throw new DownloadImportException("Unsupported file '%s'. Valid files: %s", file, getFileNames());
    }
  }

}
