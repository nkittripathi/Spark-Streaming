package com.thinkbiganalytics.nifi.v2.spark;

/*-
 * #%L
 * kylo-nifi-spark-processors
 * %%
 * Copyright (C) 2017 ThinkBig Analytics
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import com.thinkbiganalytics.nifi.core.api.metadata.KyloNiFiFlowProvider;
import com.thinkbiganalytics.nifi.core.api.metadata.MetadataProvider;
import com.thinkbiganalytics.nifi.core.api.metadata.MetadataProviderService;
import com.thinkbiganalytics.nifi.core.api.metadata.MetadataRecorder;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecuteSparkJobTest {

    /**
     * Identifier for the metadata provider service
     */
    private static final String METADATA_SERVICE_IDENTIFIER = "MockMetadataProviderService";

    /**
     * Test runner
     */
    private final TestRunner runner = TestRunners.newTestRunner(ExecuteSparkJob.class);

    /**
     * Initialize instance variables.
     */
    @Before
    public void setUp() {
        // Setup test runner
        runner.setProperty(ExecuteSparkJob.APPLICATION_JAR, "file:///home/app.jar");
        runner.setProperty(ExecuteSparkJob.MAIN_CLASS, "com.example.App");
        runner.setProperty(ExecuteSparkJob.MAIN_ARGS, "run");
        runner.setProperty(ExecuteSparkJob.SPARK_APPLICATION_NAME, "MyApp");
    }

    /**
     * Verify validators pass with default values from this test class.
     */
   
   

    /**
     * Verify validators for required properties.
     */
   
    

    /**
     * A mock implementation of {@link MetadataProviderService} for testing.
     */
    private static class MockMetadataProviderService extends AbstractControllerService implements MetadataProviderService {

        @Override
        public MetadataProvider getProvider() {
            return null;
        }

        @Override
        public MetadataRecorder getRecorder() {
            return null;
        }

        @Override
        public KyloNiFiFlowProvider getKyloNiFiFlowProvider() {
            return null;
        }
    }
}
