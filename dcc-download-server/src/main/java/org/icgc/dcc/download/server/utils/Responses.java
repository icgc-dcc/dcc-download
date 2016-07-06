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
package org.icgc.dcc.download.server.utils;

import static lombok.AccessLevel.PRIVATE;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.icgc.dcc.download.server.endpoint.BadRequestException;
import org.icgc.dcc.download.server.endpoint.ForbiddenException;
import org.icgc.dcc.download.server.endpoint.NotFoundException;

@Slf4j
@NoArgsConstructor(access = PRIVATE)
public final class Responses {

  public static void throwForbiddenException() {
    val message = "Invalid token. Access denied.";
    log.warn(message);
    throw new ForbiddenException(message);
  }

  public static void throwJobNotFoundException(String jobId) {
    val message = "Failed to find job with ID " + jobId;
    log.warn(message);
    throw new NotFoundException(message);
  }

  public static void throwBadRequestException(String message) {
    log.warn(message);
    throw new BadRequestException(message);
  }

  public static void throwPathNotFoundException(String warnMessage) {
    log.warn(warnMessage);
    throw new NotFoundException("Malformed path");
  }

}
