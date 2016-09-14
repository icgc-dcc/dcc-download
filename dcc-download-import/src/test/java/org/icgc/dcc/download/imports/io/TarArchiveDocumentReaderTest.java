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
import static org.icgc.dcc.download.imports.util.Tests.REPOSITORY_INPUT_FILE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;

import java.io.FileInputStream;
import java.util.zip.GZIPInputStream;

import lombok.Cleanup;
import lombok.val;

import org.icgc.dcc.dcc.common.es.model.Document;
import org.icgc.dcc.download.imports.core.DefaultDocumentType;
import org.icgc.dcc.release.core.document.DocumentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RunWith(MockitoJUnitRunner.class)
public class TarArchiveDocumentReaderTest {

  private static final DocumentType TYPE = DocumentType.GENE_CENTRIC_TYPE;
  private static final String RELEASE_INPUT_FILE = "src/test/resources/fixtures/input/icgc21-0-0_gene-centric.tar.gz";
  private static final ObjectNode G1_NODE =
      (ObjectNode) $("{_gene_id:'G1',donor:[{project:{_project_id:'TST1-CA'}},{project:{_project_id:'TST2-CA'}}]}");
  private static final ObjectNode G2_NODE =
      (ObjectNode) $("{_gene_id:'G2',donor:[{project:{_project_id:'TST3-CA'}},{project:{_project_id:'TST4-CA'}}]}");
  private static final ObjectNode FI94_NODE = (ObjectNode) $("{id:'FI94'}");

  @Mock
  TarArchiveEntryCallback callback;

  TarArchiveDocumentReader reader;

  @Test
  public void testRead_release() throws Exception {
    @Cleanup
    val input = new GZIPInputStream(new FileInputStream(RELEASE_INPUT_FILE));
    reader = new TarArchiveDocumentReader(input);
    reader.read(callback);

    val docType = new DefaultDocumentType(TYPE.getName());
    verify(callback).onDocument(new Document("G1", G1_NODE, docType));
    verify(callback).onDocument(new Document("G2", G2_NODE, docType));
  }

  @Test
  public void testRead_repository() throws Exception {
    @Cleanup
    val input = new GZIPInputStream(new FileInputStream(REPOSITORY_INPUT_FILE));
    reader = new TarArchiveDocumentReader(input);
    reader.read(callback);

    verify(callback).onSettings(any());
    verify(callback).onMapping(eq("file-text"), any());
    verify(callback).onMapping(eq("file-centric"), any());

    val docType = new DefaultDocumentType("file-centric");
    verify(callback).onDocument(new Document("FI94", FI94_NODE, docType));
  }

}
