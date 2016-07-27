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

import java.io.FileInputStream;
import java.util.Optional;
import java.util.zip.GZIPInputStream;

import lombok.val;

import org.icgc.dcc.release.core.document.Document;
import org.icgc.dcc.release.core.document.DocumentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fasterxml.jackson.databind.node.ObjectNode;

@RunWith(MockitoJUnitRunner.class)
public class TarArchiveDocumentReaderTest {

  private static final DocumentType TYPE = DocumentType.DONOR_TYPE;
  private static final String INPUT_FILE = "src/test/resources/fixtures/input/icgc21-0-0_donor.tar.gz";
  private static final ObjectNode DO1_NODE = (ObjectNode) $("{_donor_id:'DO1',_project_id:'TST1-CA'}");
  private static final ObjectNode DO2_NODE = (ObjectNode) $("{_donor_id:'DO2',_project_id:'TST2-CA'}");

  @Mock
  TarArchiveEntryCallback callback;

  TarArchiveDocumentReader reader;

  @Test
  public void testRead_all() throws Exception {
    val input = new GZIPInputStream(new FileInputStream(INPUT_FILE));
    reader = new TarArchiveDocumentReader(input, Optional.empty());
    reader.read(TYPE, callback);
    verify(callback).onDocument(new Document(TYPE, "DO1", DO1_NODE));
    verify(callback).onDocument(new Document(TYPE, "DO2", DO2_NODE));
  }

  @Test
  public void testRead_filter() throws Exception {
    val input = new GZIPInputStream(new FileInputStream(INPUT_FILE));
    reader = new TarArchiveDocumentReader(input, Optional.of("TST1-CA"));
    reader.read(TYPE, callback);
    verify(callback).onDocument(new Document(TYPE, "DO1", DO1_NODE));
    verify(callback, times(0)).onDocument(new Document(TYPE, "DO2", DO2_NODE));
  }

}
