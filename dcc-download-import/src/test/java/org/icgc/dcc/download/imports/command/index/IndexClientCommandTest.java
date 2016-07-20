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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.icgc.dcc.download.imports.io.TarArchiveDocumentReader;
import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallback;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;
import org.icgc.dcc.release.core.document.DocumentType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IndexClientCommandTest {

  private static final File DATA_FILE = new File("src/test/resources/fixtures/input/data.tar");

  @Mock
  TarArchiveEntryCallbackFactory callbackFactory;
  @Mock
  TarArchiveEntryCallback callback;
  @Mock
  TarArchiveDocumentReaderFactory readerFactory;
  @Mock
  TarArchiveDocumentReader donorReader;
  @Mock
  TarArchiveDocumentReader mutationCentricReader;

  IndexClientCommand indexCommand;

  @Test
  public void testExecute() throws Exception {
    when(readerFactory.createReader(any(TarArchiveInputStream.class), eq(288L))).thenReturn(donorReader);
    when(readerFactory.createReader(any(TarArchiveInputStream.class), eq(300L))).thenReturn(mutationCentricReader);
    when(callbackFactory.createCallback("icgc21-0-0")).thenReturn(callback);
    indexCommand = new IndexClientCommand(DATA_FILE, callbackFactory, readerFactory);
    indexCommand.execute();
    verify(donorReader, times(1)).read(DocumentType.DONOR_TYPE, callback);
    verify(mutationCentricReader, times(1)).read(DocumentType.MUTATION_CENTRIC_TYPE, callback);
  }

}
