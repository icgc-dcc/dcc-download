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

import static org.icgc.dcc.download.imports.util.Tests.RELEASE_FILE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.icgc.dcc.download.imports.io.TarArchiveDocumentReaderFactory;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallback;
import org.icgc.dcc.download.imports.io.TarArchiveEntryContext;
import org.icgc.dcc.download.imports.io.TarArchiveEntryCallbackFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReleaseFileLoaderTest {

  private static final String PROJECT = "TEST-CA";
  @Mock
  TarArchiveEntryCallbackFactory callbackFactory;
  TarArchiveDocumentReaderFactory readerFactory = TarArchiveDocumentReaderFactory.create();
  @Mock
  TarArchiveEntryCallback callback;

  ReleaseFileLoader loader;

  @Before
  public void setUp() {
    loader = new ReleaseFileLoader(PROJECT, callbackFactory, readerFactory);
    when(callbackFactory.createCallback(any(TarArchiveEntryContext.class))).thenReturn(callback);
  }

  @Test
  public void testLoadFile() throws Exception {
    loader.loadFile(RELEASE_FILE);

    verify(callback, times(2)).onSettings(any());
    verify(callback).onMapping(eq("donor"), any());
    verify(callback).onMapping(eq("mutation-centric"), any());
    verify(callback, times(2)).onDocument(any());
    verify(callback, times(2)).close();
  }

}
