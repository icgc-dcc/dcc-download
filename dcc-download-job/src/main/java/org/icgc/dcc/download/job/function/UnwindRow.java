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

import static com.google.common.base.Preconditions.checkState;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;

import java.util.Collections;
import java.util.List;

import lombok.NonNull;
import lombok.val;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.icgc.dcc.download.job.utils.Rows;

public final class UnwindRow implements Function<Row, Iterable<Row>> {

  private final List<String> unwindPath;

  public UnwindRow(@NonNull List<String> unwindPath) {
    checkState(unwindPath.isEmpty() == false, "Unwind path is empty");
    this.unwindPath = unwindPath;
  }

  @Override
  public Iterable<Row> call(@NonNull Row row) throws Exception {
    return unwindRow(row, unwindPath);
  }

  private static List<Row> unwindRow(Row source, List<String> path) {
    val field = path.get(0);
    val values = Rows.getValueAsList(source, field);
    if (isLast(path)) {
      return values;
    }

    val nestedElementsPath = getNestedElementsPath(path);

    return values.stream()
        .flatMap(row -> unwindRow(row, nestedElementsPath).stream())
        .collect(toImmutableList());
  }

  private static List<String> getNestedElementsPath(List<String> path) {
    if (path.size() <= 1) {
      return Collections.emptyList();
    }

    return path.subList(1, path.size());
  }

  private static boolean isLast(List<?> list) {
    return list.size() <= 1;
  }

}