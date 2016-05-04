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
package org.icgc.dcc.download.client.util;

import static lombok.AccessLevel.PRIVATE;

import java.io.IOException;
import java.io.InputStream;

import lombok.NoArgsConstructor;

@NoArgsConstructor(access = PRIVATE)
public final class Streams {

  private static byte[] SKIP_BYTE_BUFFER;
  private static final int SKIP_BUFFER_SIZE = 2048;

  /**
   * Copied from org.apache.commons.io.IOUtils to remove the dependency on Apache Commons.
   */
  public static long skip(InputStream input, long toSkip) throws IOException {
    if (toSkip < 0) {
      throw new IllegalArgumentException("Skip count must be non-negative, actual: " + toSkip);
    }

    /*
     * N.B. no need to synchronize this because: - we don't care if the buffer is created multiple times (the data is
     * ignored) - we always use the same size buffer, so if it it is recreated it will still be OK (if the buffer size
     * were variable, we would need to synch. to ensure some other thread did not create a smaller one)
     */
    if (SKIP_BYTE_BUFFER == null) {
      SKIP_BYTE_BUFFER = new byte[SKIP_BUFFER_SIZE];
    }
    long remain = toSkip;
    while (remain > 0) {
      long n = input.read(SKIP_BYTE_BUFFER, 0, (int) Math.min(remain, SKIP_BUFFER_SIZE));
      if (n < 0) { // EOF
        break;
      }
      remain -= n;
    }
    return toSkip - remain;
  }

}
