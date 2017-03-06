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
package org.icgc.dcc.download.imports.io;

import static java.lang.String.format;
import static org.icgc.dcc.common.core.util.stream.Collectors.toImmutableList;
import static org.icgc.dcc.download.imports.util.JsonNodes.getPathValue;

import java.util.Collection;
import java.util.Optional;

import javax.annotation.Nullable;

import org.icgc.dcc.dcc.common.es.core.DocumentWriter;
import org.icgc.dcc.dcc.common.es.model.IndexDocument;
import org.icgc.dcc.download.imports.service.IndexService;
import org.icgc.dcc.release.core.document.DocumentType;

import com.fasterxml.jackson.databind.node.ObjectNode;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.val;

public class ReleaseTarArchiveEntryCallback extends BaseTarArchiveEntryCallback {

  private final String project;
  private final DocumentType documentType;

  public ReleaseTarArchiveEntryCallback(
      boolean applySettings,
      @NonNull DocumentWriter documentWriter,
      @NonNull IndexService indexService,
      @Nullable String project,
      @NonNull DocumentType documentType) {
    super(applySettings, documentWriter, indexService);
    this.project = project;
    this.documentType = documentType;
  }

  @Override
  @SneakyThrows
  public void onDocument(IndexDocument document) {
    val source = document.getSource();
    if (!isSkipIndexing(source)) {
      super.onDocument(document);
    }
  }

  private boolean isSkipIndexing(ObjectNode source) {
    if (project != null && hasProject(documentType)) {
      val documentProjectPath = resolveDocumentProjectPath(documentType);
      if (!documentProjectPath.isPresent()) {
        return false;
      }

      val documentProject = getDocumentProjects(source, documentProjectPath.get());

      return !documentProject.contains(project);
    }

    return false;
  }

  private static boolean hasProject(DocumentType type) {
    return resolveDocumentProjectPath(type).isPresent();
  }

  /**
   * Returns collection because some documents may contain multiple projects.<br>
   * For example, {@code gene-centric} has multiple donors that contains projects.
   */
  private static Collection<String> getDocumentProjects(ObjectNode source, String documentProjectPath) {
    return getPathValue(source, documentProjectPath).stream()
        .map(node -> node.textValue())
        .collect(toImmutableList());
  }

  private static Optional<String> resolveDocumentProjectPath(DocumentType type) {
    switch (type) {
    case DIAGRAM_TYPE:
    case DRUG_TEXT_TYPE:
    case DRUG_CENTRIC_TYPE:
    case RELEASE_TYPE:
    case GENE_SET_TYPE:
    case GENE_SET_TEXT_TYPE:
    case GENE_TYPE:
    case GENE_TEXT_TYPE:
    case MUTATION_TEXT_TYPE:
      return Optional.empty();
    case PROJECT_TYPE:
      return Optional.of("_project_id");
    case PROJECT_TEXT_TYPE:
      return Optional.of("id");
    case DONOR_TYPE:
      return Optional.of("_project_id");
    case DONOR_TEXT_TYPE:
      return Optional.of("projectId");
    case DONOR_CENTRIC_TYPE:
      return Optional.of("_project_id");
    case GENE_CENTRIC_TYPE:
      return Optional.of("donor.project._project_id");
    case OBSERVATION_CENTRIC_TYPE:
      return Optional.of("project._project_id");
    case MUTATION_CENTRIC_TYPE:
      return Optional.of("ssm_occurrence.project._project_id");
    default:
      throw new IllegalArgumentException(format("Failed to resolve project path for type '%s'", type));
    }
  }

}
