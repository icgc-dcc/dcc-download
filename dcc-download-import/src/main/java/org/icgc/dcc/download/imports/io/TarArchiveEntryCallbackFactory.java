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
package org.icgc.dcc.download.imports.io;

import static java.lang.String.format;
import static org.icgc.dcc.release.job.index.factory.TransportClientFactory.newTransportClient;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

import org.icgc.dcc.dcc.common.es.DocumentWriterConfiguration;
import org.icgc.dcc.dcc.common.es.DocumentWriterFactory;
import org.icgc.dcc.download.imports.service.IndexService;

@RequiredArgsConstructor
public class TarArchiveEntryCallbackFactory {

  /**
   * Dependencies.
   */
  @NonNull
  private final String esUri;
  private IndexService indexService;

  public static TarArchiveEntryCallbackFactory create(@NonNull String esUri) {
    return new TarArchiveEntryCallbackFactory(esUri);
  }

  public TarArchiveEntryCallback createCallback(@NonNull TarArchiveEntryCallbackContext context) {
    val fileType = context.getFileType();
    switch (fileType) {
    case RELEASE:
      return createReleaseCallback(context);
    case REPOSITORY:
      return createRepositoryCallback(context);
    default:
      throw new IllegalArgumentException(format("Failed to resolve %s from file type %s",
          TarArchiveEntryCallback.class.getSimpleName(), fileType));
    }
  }

  private TarArchiveEntryCallback createRepositoryCallback(TarArchiveEntryCallbackContext context) {
    val indexName = context.getIndexName();
    initializeIndexService(indexName);

    val configuration = new DocumentWriterConfiguration();
    configuration
        .esUrl(esUri)
        .indexName(indexName);

    val esDocumentWriter = DocumentWriterFactory.createDocumentWriter(configuration);

    return new BaseTarArchiveEntryCallback(context.isApplySettings(), esDocumentWriter, indexService);
  }

  private TarArchiveEntryCallback createReleaseCallback(TarArchiveEntryCallbackContext context) {
    val indexName = context.getIndexName();
    initializeIndexService(indexName);

    val configuration = new DocumentWriterConfiguration();
    configuration
        .esUrl(esUri)
        .indexName(indexName);

    val esDocumentWriter = DocumentWriterFactory.createDocumentWriter(configuration);

    return new ReleaseTarArchiveEntryCallback(
        context.isApplySettings(),
        esDocumentWriter,
        indexService,
        context.getProject(),
        context.getDocumentType());
  }

  private void initializeIndexService(final java.lang.String indexName) {
    if (indexService == null) {
      indexService = new IndexService(indexName, newTransportClient(esUri, true));
    }
  }

}
