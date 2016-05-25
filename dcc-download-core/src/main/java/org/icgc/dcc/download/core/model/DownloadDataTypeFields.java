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
package org.icgc.dcc.download.core.model;

import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public final class DownloadDataTypeFields {

  public static final Map<String, String> SSM_CONTROLLED_FIELDS = ImmutableMap.<String, String> builder()
      .put("_mutation_id", "icgc_mutation_id")
      .put("_donor_id", "icgc_donor_id")
      .put("_project_id", "project_code")
      .put("_specimen_id", "icgc_specimen_id")
      .put("_sample_id", "icgc_sample_id")
      .put("_matched_sample_id", "matched_icgc_sample_id")
      .put("analyzed_sample_id", "submitted_sample_id")
      .put("matched_sample_id", "submitted_matched_sample_id")
      .put("chromosome", "chromosome")
      .put("chromosome_start", "chromosome_start")
      .put("chromosome_end", "chromosome_end")
      .put("chromosome_strand", "chromosome_strand")
      .put("assembly_version", "assembly_version")
      .put("mutation_type", "mutation_type")
      .put("reference_genome_allele", "reference_genome_allele")
      .put("control_genotype", "control_genotype")
      .put("tumour_genotype", "tumour_genotype")
      .put("expressed_allele", "expressed_allele")
      .put("mutated_from_allele", "mutated_from_allele")
      .put("mutated_to_allele", "mutated_to_allele")
      .put("quality_score", "quality_score")
      .put("probability", "probability")
      .put("total_read_count", "total_read_count")
      .put("mutant_allele_read_count", "mutant_allele_read_count")
      .put("verification_status", "verification_status")
      .put("verification_platform", "verification_platform")
      .put("biological_validation_status", "biological_validation_status")
      .put("biological_validation_platform", "biological_validation_platform")
      .put("consequence_type", "consequence_type")
      .put("aa_mutation", "aa_mutation")
      .put("cds_mutation", "cds_mutation")
      .put("gene_affected", "gene_affected")
      .put("transcript_affected", "transcript_affected")
      .put("gene_build_version", "gene_build_version")
      .put("platform", "platform")
      .put("experimental_protocol", "experimental_protocol")
      .put("sequencing_strategy", "sequencing_strategy")
      .put("base_calling_algorithm", "base_calling_algorithm")
      .put("alignment_algorithm", "alignment_algorithm")
      .put("variation_calling_algorithm", "variation_calling_algorithm")
      .put("other_analysis_algorithm", "other_analysis_algorithm")
      .put("seq_coverage", "seq_coverage")
      .put("raw_data_repository", "raw_data_repository")
      .put("raw_data_accession", "raw_data_accession")
      .put("initial_data_release_date", "initial_data_release_date")
      .build();

  public static final List<String> SSM_FIRST_LEVEL_FIELDS = ImmutableList.<String> builder()
      .add("_mutation_id")
      .add("_donor_id")
      .add("_project_id")
      .add("chromosome")
      .add("chromosome_start")
      .add("chromosome_end")
      .add("chromosome_strand")
      .add("assembly_version")
      .add("mutation_type")
      .add("reference_genome_allele")
      .add("mutated_from_allele")
      .add("mutated_to_allele")
      .add("consequence") // Used for later unrolling
      .build();

  public static final List<String> SSM_SECOND_LEVEL_FIELDS = ImmutableList.<String> builder()
      .add("_specimen_id")
      .add("_sample_id")
      .add("_matched_sample_id")
      .add("analyzed_sample_id")
      .add("matched_sample_id")
      .add("control_genotype")
      .add("tumour_genotype")
      .add("expressed_allele")
      .add("quality_score")
      .add("probability")
      .add("total_read_count")
      .add("mutant_allele_read_count")
      .add("verification_status")
      .add("verification_platform")
      .add("biological_validation_status")
      .add("biological_validation_platform")
      .add("platform")
      .add("experimental_protocol")
      .add("sequencing_strategy")
      .add("base_calling_algorithm")
      .add("alignment_algorithm")
      .add("variation_calling_algorithm")
      .add("other_analysis_algorithm")
      .add("seq_coverage")
      .add("raw_data_repository")
      .add("raw_data_accession")
      .add("initial_data_release_date")
      .build();

  public static final ImmutableSet<String> SSM_CONTROLLED_REMOVE_FIELDS = ImmutableSet.of("control_genotype",
      "tumour_genotype", "expressed_allele");

}
