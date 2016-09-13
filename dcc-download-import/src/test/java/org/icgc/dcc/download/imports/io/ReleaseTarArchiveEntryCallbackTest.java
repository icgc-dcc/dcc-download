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

import static org.icgc.dcc.common.test.json.JsonNodes.$;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import lombok.val;

import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.model.Document;
import org.icgc.dcc.download.imports.core.DefaultDocumentType;
import org.icgc.dcc.download.imports.service.IndexService;
import org.icgc.dcc.release.core.document.DocumentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RunWith(MockitoJUnitRunner.class)
public class ReleaseTarArchiveEntryCallbackTest {

  private static final ObjectNode DO1_SOURCE = (ObjectNode) $("{_donor_id:'DO1',_project_id:'TST1-CA'}");

  @Mock
  DocumentWriter documentWriter;
  @Mock
  IndexService indexService;

  ReleaseTarArchiveEntryCallback callback;

  @Test
  public void testOnDocument_noProject() throws Exception {
    callback = createCallback(null);

    val document = createEsDocument();
    callback.onDocument(document);

    verify(documentWriter).write(document);
  }

  @Test
  public void testOnDocument_withProject() throws Exception {
    callback = createCallback("TST1-CA");

    val document = createEsDocument();
    callback.onDocument(document);

    verify(documentWriter).write(document);
  }

  @Test
  public void testOnDocument_otherProject() throws Exception {
    callback = createCallback("fake");

    val document = createEsDocument();
    callback.onDocument(document);

    verify(documentWriter, times(0)).write(document);
  }

  private ReleaseTarArchiveEntryCallback createCallback(String project) {
    return new ReleaseTarArchiveEntryCallback(
        false,
        documentWriter,
        indexService,
        project,
        DocumentType.DONOR_TYPE);
  }

  private static Document createEsDocument() {
    return new Document("DO1", DO1_SOURCE, new DefaultDocumentType(DocumentType.DONOR_TYPE.getName()));
  }

}
