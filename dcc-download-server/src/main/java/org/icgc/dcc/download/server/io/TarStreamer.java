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
package org.icgc.dcc.download.server.io;

import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import java.io.IOException;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

@Slf4j
@RequiredArgsConstructor
public class TarStreamer implements FileStreamer {

  /**
   * Dependencies.
   */
  @NonNull
  private final TarArchiveOutputStream tarOut;
  @NonNull
  private final GzipStreamer gzipStreamer;

  @Override
  public String getName() {
    return format("icgc-dataset-%s.tar", currentTimeMillis());
  }

  @Override
  @SneakyThrows
  public void stream() {
    log.debug("Starting tar streaming...");
    while (gzipStreamer.hasNext()) {
      val entryName = gzipStreamer.getNextEntryName();
      val entryLength = gzipStreamer.getNextEntryLength();
      addArchiveEntry(entryName, entryLength);
      gzipStreamer.streamEntry();
      closeCurrentTarEntry();
    }
    tarOut.finish();
    log.debug("Finished tar streaming.");
  }

  @Override
  public void close() throws IOException {
    tarOut.close();
  }

  @SneakyThrows
  private void closeCurrentTarEntry() {
    tarOut.closeArchiveEntry();
    log.debug("Closed tar entry.");
  }

  @SneakyThrows
  private void addArchiveEntry(String filename, long fileSize) {
    val entry = new TarArchiveEntry(filename);
    entry.setSize(fileSize);
    tarOut.putArchiveEntry(entry);
    log.debug("Created new archive entry '{}' of {} bytes.", filename, fileSize);
  }

}
