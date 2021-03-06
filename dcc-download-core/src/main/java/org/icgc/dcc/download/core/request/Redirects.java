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
package org.icgc.dcc.download.core.request;

import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.download.core.util.Endpoints.DOWNLOADS_PATH;
import static org.icgc.dcc.download.core.util.Endpoints.STATIC_DOWNLOADS_PATH;

import java.net.URI;

import lombok.NoArgsConstructor;
import lombok.NonNull;

import org.icgc.dcc.common.core.model.DownloadDataType;

@NoArgsConstructor(access = PRIVATE)
public final class Redirects {

  public static URI getStaticFileRedirect(@NonNull String serverUrl, @NonNull String token) {
    return URI.create(serverUrl + STATIC_DOWNLOADS_PATH + "?token=" + token);
  }

  public static URI getDynamicFileRedirect(@NonNull String serverUrl, @NonNull String token) {
    return URI.create(serverUrl + DOWNLOADS_PATH + "?token=" + token);
  }

  public static URI getDynamicFileRedirect(@NonNull String serverUrl, @NonNull String token,
      @NonNull DownloadDataType type) {
    return URI.create(format("%s%s?token=%s&type=%s", serverUrl, DOWNLOADS_PATH, token, type.getId()));
  }

}
