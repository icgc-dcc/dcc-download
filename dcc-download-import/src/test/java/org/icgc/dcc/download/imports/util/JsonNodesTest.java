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
package org.icgc.dcc.download.imports.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.common.test.json.JsonNodes.$;
import static org.icgc.dcc.download.imports.util.JsonNodes.getPathValue;

import java.util.Collection;
import java.util.stream.Collectors;

import lombok.val;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonNodesTest {

  @Test
  public void testGetPathValue_empty() throws Exception {
    val input = $("{}");
    val result = getPathValue(input, "path");
    assertThat(result).isEmpty();
  }

  @Test
  public void testGetPathValue_one_level() throws Exception {
    val input = $("{path:'a'}");
    val result = getPathValue(input, "path");
    assertThat(toString(result)).containsOnly("a");
  }

  @Test
  public void testGetPathValue_two_levels() throws Exception {
    val input = $("{nested:{path:'a'}}");
    val result = getPathValue(input, "nested.path");
    assertThat(toString(result)).containsOnly("a");
  }

  @Test
  public void testGetPathValue_one_level_array() throws Exception {
    val input = $("{path:['a','b']}");
    val result = getPathValue(input, "path");
    assertThat(toString(result)).containsOnly("a", "b");
  }

  @Test
  public void testGetPathValue_two_levels_array() throws Exception {
    val input = $("{nested:{path:['a','b']}}");
    val result = getPathValue(input, "nested.path");
    assertThat(toString(result)).containsOnly("a", "b");
  }

  @Test
  public void testGetPathValue_array_in_middle() throws Exception {
    val input = $("{one:{ two:[{three:'a'}, {}, {three:'a'}, {three:'b'}]} }");
    val result = getPathValue(input, "one.two.three");
    assertThat(toString(result)).containsOnly("a", "a", "b");
  }

  private static Collection<String> toString(Collection<JsonNode> jsonNodes) {
    return jsonNodes.stream()
        .map(node -> node.textValue())
        .collect(Collectors.toList());
  }

}
