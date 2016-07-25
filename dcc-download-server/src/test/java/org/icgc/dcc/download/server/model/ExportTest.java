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
import static org.icgc.dcc.download.server.model.Export.DATA;
import static org.icgc.dcc.download.server.model.Export.RELEASE_CONTROLLED;
import static org.icgc.dcc.download.server.model.Export.RELEASE_OPEN;

import org.junit.Test;

public class ExportTest {

  @Test
  public void testFromId() throws Exception {
    assertThat(Export.fromId("release21.open.tar")).isEqualTo(RELEASE_OPEN);
    assertThat(Export.fromId("release21.controlled.tar")).isEqualTo(RELEASE_CONTROLLED);
  }

  @Test
  public void testGetIdInt() throws Exception {
    assertThat(RELEASE_OPEN.getId(21)).isEqualTo("release21.open.tar");
    assertThat(RELEASE_CONTROLLED.getId(21)).isEqualTo("release21.controlled.tar");
  }

  @Test
  public void testGetType() throws Exception {
    assertThat(RELEASE_OPEN.getType()).isEqualTo("release");
    assertThat(RELEASE_CONTROLLED.getType()).isEqualTo("release");
    assertThat(DATA.getType()).isEqualTo("data");
  }

  @Test
  public void testGetId() throws Exception {
    assertThat(DATA.getId()).isEqualTo("data.tar");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetId_unsupported() throws Exception {
    RELEASE_OPEN.getId();
  }

}
