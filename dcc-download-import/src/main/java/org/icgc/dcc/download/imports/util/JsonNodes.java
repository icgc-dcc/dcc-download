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

import static lombok.AccessLevel.PRIVATE;
import static org.icgc.dcc.common.core.util.Splitters.DOT;

import java.util.Collection;
import java.util.List;

import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.val;

import org.icgc.dcc.common.core.util.Joiners;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;

// TODO: Move to commons
@NoArgsConstructor(access = PRIVATE)
public final class JsonNodes {

  public static Collection<JsonNode> getPathValue(@NonNull JsonNode node, @NonNull String path) {
    val fields = DOT.splitToList(path);
    val values = ImmutableList.<JsonNode> builder();
    JsonNode childNode = null;
    for (val field : fields) {
      childNode = childNode == null ? node.path(field) : childNode.path(field);
      val last = isLast(field, fields);
      if (childNode.isMissingNode() || childNode.isNull()) {
        break;
      } else if (last && childNode.isArray()) {
        values.addAll(toList(childNode));
      } else if (last) {
        values.add(childNode);
      } else if (childNode.isArray()) {
        values.addAll(getArrayValues(childNode, getUnprocessedFields(field, fields)));
      }
    }

    return values.build();
  }

  private static List<JsonNode> toList(JsonNode elements) {
    val values = ImmutableList.<JsonNode> builder();
    for (val element : elements) {
      values.add(element);
    }

    return values.build();
  }

  private static Iterable<JsonNode> getArrayValues(JsonNode elements, List<String> fields) {
    val path = Joiners.DOT.join(fields);
    val values = ImmutableList.<JsonNode> builder();
    for (val element : elements) {
      values.addAll(getPathValue(element, path));
    }

    return values.build();
  }

  private static List<String> getUnprocessedFields(String field, List<String> fields) {
    val index = fields.indexOf(field);

    // Exclude the field already been processed
    return fields.subList(index + 1, fields.size());
  }

  private static boolean isLast(String element, List<String> elements) {
    val index = elements.indexOf(element);

    return index == elements.size() - 1;
  }

}
