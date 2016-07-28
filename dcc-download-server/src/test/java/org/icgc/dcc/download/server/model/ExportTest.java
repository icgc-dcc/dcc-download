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

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.server.model.Export.DATA_CONTROLLED;
import static org.icgc.dcc.download.server.model.Export.DATA_OPEN;
import static org.icgc.dcc.download.server.model.Export.RELEASE;
import static org.icgc.dcc.download.server.model.Export.REPOSITORY;

import org.junit.Test;

public class ExportTest {

  @Test
  public void testFromId() throws Exception {
    assertThat(Export.fromId("release.tar")).isEqualTo(RELEASE);
    assertThat(Export.fromId("data.open.tar")).isEqualTo(DATA_OPEN);
    assertThat(Export.fromId("data.controlled.tar")).isEqualTo(DATA_CONTROLLED);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFromId_invalid() throws Exception {
    Export.fromId("invalid_export");
  }

  @Test
  public void testGetIdInt() throws Exception {
    assertThat(RELEASE.getId()).isEqualTo("release.tar");
  }

  @Test
  public void testGetType() throws Exception {
    assertThat(RELEASE.getType()).isEqualTo("release");
    assertThat(DATA_OPEN.getType()).isEqualTo("data");
  }

  @Test
  public void testGetId() throws Exception {
    assertThat(DATA_OPEN.getId()).isEqualTo("data.open.tar");
  }

  @Test
  public void testIsControlled() throws Exception {
    assertThat(DATA_OPEN.isControlled()).isFalse();
    assertThat(DATA_CONTROLLED.isControlled()).isTrue();
    assertThat(RELEASE.isControlled()).isFalse();
    assertThat(REPOSITORY.isControlled()).isFalse();
  }

}
