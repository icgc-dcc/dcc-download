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
package org.icgc.dcc.download.server.model;

import java.util.Collection;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableList;

public class MetadataResponse {

  private final Collection<ExportFile> files;

  public MetadataResponse(ExportFile... exportFiles) {
    this.files = ImmutableList.copyOf(exportFiles);
  }

  private MetadataResponse(Collection<ExportFile> files) {
    this.files = files;
  }

  @JsonValue
  public Collection<ExportFile> getFiles() {
    return files;
  }

  public static MetadataResponseBuilder builder() {
    return new MetadataResponseBuilder();
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class MetadataResponseBuilder {

    private final ImmutableList.Builder<ExportFile> builder = ImmutableList.builder();

    public MetadataResponseBuilder add(ExportFile exportFile) {
      builder.add(exportFile);

      return this;
    }

    public MetadataResponse build() {
      return new MetadataResponse(builder.build());
    }

  }

}
