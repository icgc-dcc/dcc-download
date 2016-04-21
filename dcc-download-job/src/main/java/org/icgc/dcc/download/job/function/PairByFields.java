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
package org.icgc.dcc.download.job.function;

import static org.icgc.dcc.download.job.utils.Rows.getObjectValue;
import static org.icgc.dcc.download.job.utils.Rows.getValue;

import java.util.Collection;
import java.util.Map;

import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.icgc.dcc.download.job.utils.Rows;

import scala.Tuple2;

import com.google.common.collect.ImmutableMap;

/**
 * Creates a tuple of the following structure: {@code Tuple2<Tuple2<Map<String,String>, Map<String,Object>>, Row>}.<br>
 * The key part of the first tuple is <b>{@code Tuple2<Map<String,String>, Map<String,Object>>}</b>, where <b>
 * {@code Map<String,String>}</b> contains resolved field-value pairs and <b>{@code Map<String,Object>}</b> contains
 * unresolved field-value pairs.<br>
 * The above structure is used to allow an arbitrary unrolling of the nested Rows
 */
@Slf4j
@RequiredArgsConstructor
public final class PairByFields implements PairFunction<Row, Tuple2<Map<String, String>, Map<String, Object>>, Row> {

  private final Collection<String> keyFields;

  @Override
  public Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row> call(Row row) {
    val resolvedValues = ImmutableMap.<String, String> builder();
    val unresolvedValues = ImmutableMap.<String, Object> builder();

    for (val field : keyFields) {
      if (Rows.canBeResolved(row, field)) {
        resolvedValues.put(field, getValue(row, field));
      } else {
        log.debug("Unresolved value: Key - {}, Value - {}", field, row);
        unresolvedValues.put(field, getObjectValue(row, field));
      }
    }

    val key = new Tuple2<Map<String, String>, Map<String, Object>>(resolvedValues.build(), unresolvedValues.build());
    return new Tuple2<Tuple2<Map<String, String>, Map<String, Object>>, Row>(key, row);
  }

}