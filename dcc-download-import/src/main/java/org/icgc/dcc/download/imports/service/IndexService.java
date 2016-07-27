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
package org.icgc.dcc.download.imports.service;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static lombok.AccessLevel.PRIVATE;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.icgc.dcc.download.imports.core.DownloadImportException;

import com.fasterxml.jackson.databind.node.ObjectNode;

@Slf4j
@RequiredArgsConstructor
public class IndexService {

  /**
   * Configuration.
   */
  @NonNull
  private final String indexName;

  /**
   * Dependencies.
   */
  @NonNull
  private final Client client;

  @Getter(lazy = true, value = PRIVATE)
  private final IndicesAdminClient indexClient = client.admin().indices(); // NOPMD

  public void applySettings(@NonNull ObjectNode settings) {
    val client = getIndexClient();
    ensureIndexNotExists(client);

    log.info("Creating index '{}'...", indexName);
    checkState(client
        .prepareCreate(indexName)
        .setSettings(settings.toString())
        .execute()
        .actionGet()
        .isAcknowledged(),
        "Index '%s' creation was not acknowledged!", indexName);
    alias();
  }

  public void applyMapping(@NonNull String typeName, @NonNull ObjectNode mapping) {
    val client = getIndexClient();
    log.info("Creating index '{}' mapping for type '{}'...", indexName, typeName);
    checkState(client.preparePutMapping(indexName)
        .setType(typeName)
        .setSource(mapping.toString())
        .execute()
        .actionGet()
        .isAcknowledged(),
        "Index '%s' type mapping in index '%s' was not acknowledged for release '%s'!",
        typeName, indexName);
  }

  private void ensureIndexNotExists(IndicesAdminClient client) {
    log.info("Checking index '{}' for existence...", indexName);
    val exists = client.prepareExists(indexName)
        .execute()
        .actionGet()
        .isExists();
    if (exists) {
      throw new DownloadImportException("Index '%s' already exists.", indexName);
    }
  }

  private void alias() {
    val alias = getAlias();
    val request = getIndexClient().prepareAliases();
    request.addAlias(indexName, alias);

    checkState(request
        .execute()
        .actionGet()
        .isAcknowledged(),
        "Assigning index alias '%s' to index '%s' was not acknowledged!",
        alias, indexName);
  }

  private String getAlias() {
    if (indexName.contains("icgc-repository")) {
      return "icgc-repository";
    }

    if (indexName.contains("icgc")) {
      return "icgc-release";
    }

    throw new IllegalArgumentException(format("Failed to resolve alias from index name '%s'", indexName));
  }

}
