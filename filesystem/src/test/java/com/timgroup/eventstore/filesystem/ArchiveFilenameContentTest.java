package com.timgroup.eventstore.filesystem;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class ArchiveFilenameContentTest {
    @Test
    public void parses_archive_filename() throws Exception {
        assertThat(ArchiveFilenameContent.parseFilename("00000000.instrument.818e8eef-aeec-5062-b550-50c17c8d84fe.0.QuoteIdAssigned.data"),
                equalTo(new ArchiveFilenameContent(0, "instrument", "818e8eef-aeec-5062-b550-50c17c8d84fe", 0L, "QuoteIdAssigned", "data", "00000000.instrument.818e8eef-aeec-5062-b550-50c17c8d84fe.0.QuoteIdAssigned")));
    }
}
