package org.icgc.dcc.download.job.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.icgc.dcc.download.job.util.TestRows.createExposureSchema;
import static org.icgc.dcc.download.job.util.TestRows.createRow;
import lombok.val;

import org.icgc.dcc.common.core.util.Joiners;
import org.icgc.dcc.download.core.model.DownloadDataType;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ConvertRowTest {

  ConvertRow function;

  @Test
  public void testConvert() throws Exception {
    val exposureRow =
        createRow(createExposureSchema(), "alco_hist", "alco_hist_int", 1, "exp_notes", "exp_type", null, 2);
    val resolvedValues = ImmutableMap.of(
        "_donor_id", "DO1",
        "_project_id", "DCC-TEST",
        "donor_id", "DID123");

    function = new ConvertRow(DownloadDataType.DONOR_EXPOSURE);
    val actualValue = function.convert(exposureRow, resolvedValues);
    val expectedValue = Joiners.TAB.join("DO1", "DCC-TEST", "DID123", "exp_type", 1, "", 2, "alco_hist",
        "alco_hist_int");
    assertThat(actualValue).isEqualTo(expectedValue);
  }

}
