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
package org.icgc.dcc.download.client.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.icgc.dcc.common.core.model.DownloadDataType;
import org.icgc.dcc.download.client.DownloadClient;
import org.icgc.dcc.download.core.model.DownloadFile;
import org.icgc.dcc.download.core.model.JobUiInfo;
import org.icgc.dcc.download.core.response.JobResponse;

public class NoOpDownloadClient implements DownloadClient {

  @Override
  public boolean isServiceAvailable() {
    return false;
  }

  @Override
  public Map<DownloadDataType, Long> getSizes(Set<String> donorIds) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String submitJob(Set<String> donorIds, Set<DownloadDataType> dataTypes, JobUiInfo jobInfo) {
    throw new UnsupportedOperationException();
  }

  @Override
  public JobResponse getJob(String jobId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<DownloadFile> listFiles(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<DownloadFile> listFiles(String path, boolean recursive) {
    throw new UnsupportedOperationException();
  }

}
