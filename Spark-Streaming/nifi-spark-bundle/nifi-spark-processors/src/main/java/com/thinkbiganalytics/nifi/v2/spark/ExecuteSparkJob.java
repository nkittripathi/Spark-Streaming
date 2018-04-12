package com.thinkbiganalytics.nifi.v2.spark;

/*-
 * #%L
 * thinkbig-nifi-spark-processors
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.thinkbiganalytics.metadata.rest.model.data.Datasource;
import com.thinkbiganalytics.metadata.rest.model.data.JdbcDatasource;
import com.thinkbiganalytics.nifi.core.api.metadata.MetadataProvider;
import com.thinkbiganalytics.nifi.core.api.metadata.MetadataProviderService;
import com.thinkbiganalytics.nifi.processor.AbstractNiFiProcessor;
import com.thinkbiganalytics.nifi.security.ApplySecurityPolicy;
import com.thinkbiganalytics.nifi.security.KerberosProperties;
import com.thinkbiganalytics.nifi.security.SecurityUtil;
import com.thinkbiganalytics.nifi.security.SpringSecurityContextLoader;
import com.thinkbiganalytics.nifi.util.InputStreamReaderRunnable;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationReport;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.security.PrivilegedExceptionAction;


import javax.annotation.Nonnull;

@EventDriven
@SupportsBatching
@Stateful(description = "", scopes = {Scope.CLUSTER})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"spark", "thinkbig"})
@CapabilityDescription("Execute a Spark job.")
public class ExecuteSparkJob extends AbstractNiFiProcessor {

    public static final String SPARK_NETWORK_TIMEOUT_CONFIG_NAME = "spark.network.timeout";
    public static final String SPARK_YARN_KEYTAB = "spark.yarn.keytab";
    public static final String SPARK_YARN_PRINCIPAL = "spark.yarn.principal";
    public static final String SPARK_YARN_QUEUE = "spark.yarn.queue";
    public static final String SPARK_CONFIG_NAME = "--conf";
    public static final String SPARK_EXTRA_FILES_CONFIG_NAME = "--files";
    public static final String SPARK_NUM_EXECUTORS = "spark.executor.instances";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Successful result.")
        .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("Spark execution failed. Incoming FlowFile will be penalized and routed to this relationship")
        .build();
    public static final PropertyDescriptor APPLICATION_JAR = new PropertyDescriptor.Builder()
        .name("ApplicationJAR")
        .description("Path to the JAR file containing the Spark job application")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor EXTRA_JARS = new PropertyDescriptor.Builder()
        .name("Extra JARs")
        .description("A file or a list of files separated by comma which should be added to the class path")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor YARN_QUEUE = new PropertyDescriptor.Builder()
        .name("Yarn Queue")
        .description("Optional Yarn Queue")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MAIN_CLASS = new PropertyDescriptor.Builder()
        .name("MainClass")
        .description("Qualified classname of the Spark job application class")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor MAIN_ARGS = new PropertyDescriptor.Builder()
        .name("MainArgs")
        .description("Comma separated arguments to be passed into the main as args")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor SPARK_HOME = new PropertyDescriptor.Builder()
        .name("SparkHome")
        .description("Path to the Spark Client directory")
        .required(true)
        .defaultValue("/usr/hdp/current/spark-client/")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor SPARK_MASTER = new PropertyDescriptor.Builder()
        .name("SparkMaster")
        .description("The Spark master")
        .required(true)
        .defaultValue("local")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor SPARK_YARN_DEPLOY_MODE = new PropertyDescriptor.Builder()
            .name("Spark YARN Deploy Mode")
            .description("The deploy mode for YARN master (client, cluster). Only applicable for yarn mode. "
                         + "NOTE: Please ensure that you have not set this in your application.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor DRIVER_MEMORY = new PropertyDescriptor.Builder()
        .name("Driver Memory")
        .description("How much RAM to allocate to the driver")
        .required(true)
        .defaultValue("512m")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor EXECUTOR_MEMORY = new PropertyDescriptor.Builder()
        .name("Executor Memory")
        .description("How much RAM to allocate to the executor")
        .required(true)
        .defaultValue("512m")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor NUMBER_EXECUTORS = new PropertyDescriptor.Builder()
        .name("Number of Executors")
        .description("The number of exectors to be used")
        .required(true)
        .defaultValue("1")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor EXECUTOR_CORES = new PropertyDescriptor.Builder()
        .name("Executor Cores")
        .description("The number of executor cores to be used")
        .required(true)
        .defaultValue("1")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor SPARK_APPLICATION_NAME = new PropertyDescriptor.Builder()
        .name("Spark Application Name")
        .description("The name of the spark application")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor NETWORK_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Network Timeout")
        .description(
            "Default timeout for all network interactions. This config will be used in place of spark.core.connection.ack.wait.timeout, spark.akka.timeout, spark.storage.blockManagerSlaveTimeoutMs, spark.shuffle.io.connectionTimeout, spark.rpc.askTimeout or spark.rpc.lookupTimeout if they are not configured.")
        .required(true)
        .defaultValue("120s")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
        .name("Hadoop Configuration Resources")
        .description("A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop "
                     + "will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration.")
        .required(false)
        .addValidator(createMultipleFilesExistValidator())
        .build();
    public static final PropertyDescriptor SPARK_CONFS = new PropertyDescriptor.Builder()
        .name("Spark Configurations")
        .description("Pipe separated arguments to be passed into the Spark as configurations i.e <CONF1>=<VALUE1>|<CONF2>=<VALUE2>..")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor EXTRA_SPARK_FILES = new PropertyDescriptor.Builder()
        .name("Extra Files")
        .description("Comma separated file paths to be passed to the Spark Executors")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor PROCESS_TIMEOUT = new PropertyDescriptor.Builder()
        .name("Spark Process Timeout")
        .description("Time to wait for successful completion of Spark process. Routes to failure if Spark process runs for longer than expected here")
        .required(true)
        .defaultValue("1 hr")
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor DATASOURCES = new PropertyDescriptor.Builder()
        .name("Data Sources")
        .description("A comma-separated list of data source ids to include in the environment for Spark.")
        .required(false)
        .addValidator(createUuidListValidator())
        .expressionLanguageSupported(true)
        .build();
    public static final PropertyDescriptor METADATA_SERVICE = new PropertyDescriptor.Builder()
        .name("Metadata Service")
        .description("Kylo metadata service")
        .required(false)
        .identifiesControllerService(MetadataProviderService.class)
        .build();
    public static final PropertyDescriptor CATEGORY_NAME = new PropertyDescriptor.Builder().name("Category Name")
            .required(true).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).expressionLanguageSupported(true).build();

    public static final PropertyDescriptor FEED_NAME = new PropertyDescriptor.Builder().name("Feed Name").required(false)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).expressionLanguageSupported(true).build();

    /**
     * Matches a comma-separated list of UUIDs
     */
    private static final Pattern UUID_REGEX = Pattern.compile("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
                                                              + "(,[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})*$");

    private final Set<Relationship> relationships;

    /**
     * Kerberos service keytab
     */
    private PropertyDescriptor kerberosKeyTab;

    /**
     * Kerberos service principal
     */
    private PropertyDescriptor kerberosPrincipal;

    /**
     * List of properties
     */
    private List<PropertyDescriptor> propDescriptors;

    public ExecuteSparkJob() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);
    }

    /*
     * Validates that one or more files exist, as specified in a single property.
     */
    public static final Validator createMultipleFilesExistValidator() {
        return new Validator() {
            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final String[] files = input.split(",");
                for (String filename : files) {
                    try {
                        final File file = new File(filename.trim());
                        if (!file.exists()) {
                            final String message = "file " + filename + " does not exist";
                            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                        } else if (!file.isFile()) {
                            final String message = filename + " is not a file";
                            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                        } else if (!file.canRead()) {
                            final String message = "could not read " + filename;
                            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                        }
                    } catch (SecurityException e) {
                        final String message = "Unable to access " + filename + " due to " + e.getMessage();
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

        };
    }

    /**
     * Creates a new {@link Validator} that checks for a comma-separated list of UUIDs.
     *
     * @return the validator
     */
    @Nonnull
    private static Validator createUuidListValidator() {
        return (subject, input, context) -> {
            final String value = context.getProperty(DATASOURCES).evaluateAttributeExpressions().getValue();
            if (value == null || value.isEmpty() || UUID_REGEX.matcher(value).matches()) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).explanation("List of UUIDs").build();
            } else {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("not a list of UUIDs").build();
            }
        };
    }

    @Override
    protected void init(@Nonnull final ProcessorInitializationContext context) {
        super.init(context);

        // Create Kerberos properties
        final SpringSecurityContextLoader securityContextLoader = SpringSecurityContextLoader.create(context);
        final KerberosProperties kerberosProperties = securityContextLoader.getKerberosProperties();
        kerberosKeyTab = kerberosProperties.createKerberosKeytabProperty();
        kerberosPrincipal = kerberosProperties.createKerberosPrincipalProperty();

        // Create list of properties
        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(APPLICATION_JAR);
        pds.add(EXTRA_JARS);
        pds.add(MAIN_CLASS);
        pds.add(MAIN_ARGS);
        pds.add(SPARK_MASTER);
        pds.add(SPARK_HOME); 
        pds.add(SPARK_YARN_DEPLOY_MODE);
        pds.add(PROCESS_TIMEOUT);
        pds.add(DRIVER_MEMORY);
        pds.add(EXECUTOR_MEMORY);
        pds.add(NUMBER_EXECUTORS);
        pds.add(SPARK_APPLICATION_NAME);
        pds.add(EXECUTOR_CORES);
        pds.add(NETWORK_TIMEOUT);
        pds.add(HADOOP_CONFIGURATION_RESOURCES);
        pds.add(CATEGORY_NAME);
        pds.add(FEED_NAME);
        pds.add(kerberosPrincipal);
        pds.add(kerberosKeyTab);
        pds.add(YARN_QUEUE);
        pds.add(SPARK_CONFS);
        pds.add(EXTRA_SPARK_FILES);
        pds.add(DATASOURCES);
        pds.add(METADATA_SERVICE);
        propDescriptors = Collections.unmodifiableList(pds);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }
    
   // private static YarnClient client = null;
    //private static UserGroupInformation userGroupInformation;
    private Map<String, String> currentState;
    private boolean isKerberized = false;
    private static final String APPLICATION_ID = "yarn.application.id.";
    private static final String RUNNING_FEEDS = "running.feeds.";
    private static final Log LOG = LogFactory.getLog(ExecuteSparkJob.class);
    String extraJars; 
    
    private void getCurrentState(ProcessContext context) throws YarnException, IOException, InterruptedException {
        currentState = new HashMap<>(context.getStateManager().getState(Scope.CLUSTER).toMap());

        String yarnSite = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue();

        DelegateSparkKafkaUtilities.initialiseYarn(isKerberized, yarnSite, context.getProperty(kerberosPrincipal).getValue(), context.getProperty(kerberosKeyTab).getValue());
    }
    
    private void stopSparkJob(final ProcessContext context, FlowFile flowFile) throws IOException, YarnException, InterruptedException {
        int numberOfRunningFeeds = getNumberOfFeeds(context, flowFile);
        
        System.out.println("##### In stopSparkJob numberOfRunningFeeds" + numberOfRunningFeeds);

        // if there are no running feeds, you don't need to stop anything. Just return.
        if (numberOfRunningFeeds == 0) {
            return;
        } else if (numberOfRunningFeeds == 1) {
            // if this is the last feed running for the CATEGORY_NAME/SECURITY_NAME, cancel the Spark job.
        	System.out.println("##### In stopSparkJob before killSparkJob" + numberOfRunningFeeds);
            killSparkJob(context, getApplicationID(context, flowFile));
        	System.out.println("##### In stopSparkJob after killSparkJob" + numberOfRunningFeeds);
        }

        // decrement the number of running feeds.
    	System.out.println("##### In stopSparkJob before removeFeed" + numberOfRunningFeeds);
        removeFeed(context, flowFile);
        System.out.println("##### In stopSparkJob after removeFeed" + numberOfRunningFeeds);
    }
    
    private int getNumberOfFeeds(ProcessContext context, FlowFile flowFile) throws IOException {
        int runningFeeds;
       // System.out.println("##### In getNumberOfFeeds  runningFeeds" + Integer.parseInt(currentState.get(RUNNING_FEEDS + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME))));
       // System.out.println("##### In getNumberOfFeeds current state contain " + currentState.containsKey(RUNNING_FEEDS + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME) ) );
        System.out.println("##### In getNumberOfFeeds " + RUNNING_FEEDS);
        System.out.println("##### In getNumberOfFeeds " + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME));
        if (!currentState.containsKey(RUNNING_FEEDS + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME) )) {
            runningFeeds = 0;
            System.out.println("##### In getNumberOfFeeds runningFeeds" + runningFeeds);
        } else {
            runningFeeds = Integer.parseInt(currentState.get(RUNNING_FEEDS + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME)));
            System.out.println("##### In getNumberOfFeeds in else runningFeeds");
        }
        System.out.println("##### In getNumberOfFeeds ");
        return runningFeeds;
    }
    
    private String getApplicationID(ProcessContext context, FlowFile flowFile) throws IOException {
   
        return currentState.getOrDefault(makeApplicationId(context, flowFile), null);
    }
    
    private void killSparkJob(final ProcessContext context, String applicationID) throws YarnException, IOException, InterruptedException {
        YarnClient yarnClient = DelegateSparkKafkaUtilities.getYarnClient();

        List<ApplicationReport> applications;

        if (isKerberized) {
            applications = DelegateSparkKafkaUtilities.getUserGroupInformation().doAs(new PrivilegedExceptionAction<List<ApplicationReport>>() {
                @Override
                public List<ApplicationReport> run() throws YarnException, IOException {
                    return yarnClient.getApplications();
                }
            });
        } else {
            applications = yarnClient.getApplications();
        }
        
        for (ApplicationReport application : applications) {
        	System.out.println("##### application.getApplicationId().toString() " + application.getApplicationId().toString());
            if (application.getApplicationId().toString().equals(applicationID)) {
                yarnClient.killApplication(application.getApplicationId());
                break;
            }
        }
    }
    
    private void removeFeed(ProcessContext context, FlowFile flowFile) throws IOException {
        int numberOfFeeds = getNumberOfFeeds(context, flowFile);
        System.out.println("##### removeFeed " + numberOfFeeds);
        currentState.put(makeRunningFeeds(context, flowFile), Integer.toString(numberOfFeeds - 1));
    }
    
    private String makeApplicationId(ProcessContext context, FlowFile flowFile) throws IOException {
        StringBuilder applicationIDBuilder = new StringBuilder();

        applicationIDBuilder.append(APPLICATION_ID).append(DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME));

        if (context.getProperty(FEED_NAME).isSet()) {
            applicationIDBuilder.append(".").append(DelegateSparkKafkaUtilities.getProperty(context, flowFile, FEED_NAME));
        }
    		System.out.println("##### applicationIDBuilder" + applicationIDBuilder.toString());
    		
    		System.out.println("##### makeApplicationId " + applicationIDBuilder.toString());
        return applicationIDBuilder.toString();
    }
    
    private String makeRunningFeeds(ProcessContext context, FlowFile flowFile) throws IOException {
        StringBuilder applicationIDBuilder = new StringBuilder();

        applicationIDBuilder.append(RUNNING_FEEDS).append(DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME));

        if (context.getProperty(FEED_NAME).isSet()) {
            applicationIDBuilder.append(".").append(DelegateSparkKafkaUtilities.getProperty(context, flowFile, FEED_NAME));
        }
        System.out.println("##### In makeRunningFeeds applicationIDBuilder" + applicationIDBuilder.toString());
        return applicationIDBuilder.toString();
    }
    
    private boolean getSparkJobStatus(ProcessContext context, FlowFile flowFile) throws YarnException, IOException, InterruptedException {
        String applicationID = getApplicationID(context, flowFile);

        LOG.info("Job applicationID is " + applicationID);

        if (applicationID == null) {
            return false;
        }

        YarnClient yarnClient = DelegateSparkKafkaUtilities.getYarnClient();

        List<ApplicationReport> applications;

        if (isKerberized) {
            applications = DelegateSparkKafkaUtilities.getUserGroupInformation().doAs(new PrivilegedExceptionAction<List<ApplicationReport>>() {
                @Override
                public List<ApplicationReport> run() throws YarnException, IOException {
                    return yarnClient.getApplications();
                }
            });
        } else {
            applications = yarnClient.getApplications();
        }

        for (ApplicationReport application : applications) {
        	System.out.println("##### In getSparkJobStatus ApplicationReport" + application.toString());
         	System.out.println("##### In getSparkJobStatus applicationID" + applicationID);
            if (application.getApplicationId().toString().equals(applicationID)) {
                return application.getYarnApplicationState() != YarnApplicationState.FAILED
                        && application.getYarnApplicationState() != YarnApplicationState.KILLED
                        && application.getYarnApplicationState() != YarnApplicationState.FINISHED;
            }
        }

        return false;
    }
    
    private void executeSparkJob(ProcessContext context, FlowFile flowFile) throws IOException, InterruptedException {
        SparkLauncher launcher = DelegateSparkKafkaUtilities.getNewSparkLauncher();
       /* StringBuilder driverOptions = new StringBuilder();
        StringBuilder executorOptions = new StringBuilder();
        StringBuilder yarnOptions = new StringBuilder();
        StringBuilder fileOptions = new StringBuilder(); */

		/*
         * Set additional JAR files for inclusion in the Spark job's distributed cache.
		 */
        if (context.getProperty(extraJars).isSet()) {
            File folder = new File(context.getProperty(EXTRA_JARS).evaluateAttributeExpressions(flowFile).getValue());
            File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".jar") || name.toLowerCase().endsWith(".JAR"));

            if (files != null) {
                for (File file : files) {
                    launcher.addJar(file.getCanonicalPath());
                }
            }
        }
        
        LOG.info("About to launch Spark job.");

        SparkAppHandle handle;

        handle = launcher.startApplication();

        // If the application fails to start (the startApplication method returns null), throw an IOException.
        if (handle == null) {
            throw new IOException("Unable to launch Spark application. No handle returned!");
        }

		/*
		 * The SparkLauncher class utilises an asynchronous architecture. We register a listener.
		 * When the status of the submitted Spark job changes (e.g., it is assigned an application ID), call
		 * the setApplicationID method to save it for future reference.
		 */
        handle.addListener(new Listener() {
            @Override
            public void infoChanged(SparkAppHandle handle) {
                if (handle.getAppId() != null) {
                    LOG.info("Spark job now has an applicationID: " + handle.getAppId());

                    try {
                        setApplicationID(context, handle.getAppId(), flowFile);

                        setState(context);
                    } catch (IOException e) {
                        LOG.error(e);
                    }
                }
            }

            @Override
            public void stateChanged(SparkAppHandle handle) {
            }

        });
    }
    
    private void setApplicationID(ProcessContext context, String applicationID, FlowFile flowFile) throws IOException {
        currentState.put(makeApplicationId(context, flowFile), applicationID);
    }
    
    private void setState(ProcessContext context) throws IOException {
        context.getStateManager().setState(currentState, Scope.CLUSTER);
    }
    
    private void addFeed(ProcessContext context, FlowFile flowFile) throws IOException {
        int numberOfFeeds = getNumberOfFeeds(context, flowFile);
        System.out.println("##### numberOfFeeds " + numberOfFeeds);

        LOG.info(DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME)  + " currently has " + numberOfFeeds + " running.");
        currentState.put(makeRunningFeeds(context, flowFile), Integer.toString(numberOfFeeds + 1));
    }
    
    @OnStopped
    public void stopAllSparkJobs(final ProcessContext context) throws IOException, YarnException, InterruptedException {
        getCurrentState(context);

        LOG.info("Stopping all Spark jobs.");
        System.out.println("##### stopAllSparkJobs ");
        System.out.println("##### stopAllSparkJobs ");

        for (String key : currentState.keySet()) {
        	System.out.println("##### key " + key);
            if (key.startsWith(APPLICATION_ID)) {
            		System.out.println("##### key.startsWith " + key.startsWith(APPLICATION_ID));
            		System.out.println("##### currentState.get " + currentState.get(key));
                killSparkJob(context, currentState.get(key));
            } else if (key.startsWith(RUNNING_FEEDS)) {
                currentState.put(key, Integer.toString(0));
            }
        }

        // save the state afterwards.
        setState(context);
    }

    FlowFile flowFile;
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLog();
        flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        String PROVENANCE_JOB_STATUS_KEY = "Job Status";
        String PROVENANCE_SPARK_EXIT_CODE_KEY = "Spark Exit Code";

        try {

            

              /* Configuration parameters for spark launcher */
            String appJar = context.getProperty(APPLICATION_JAR).evaluateAttributeExpressions(flowFile).getValue().trim();
            String extraJars = context.getProperty(EXTRA_JARS).evaluateAttributeExpressions(flowFile).getValue();
            String yarnQueue = context.getProperty(YARN_QUEUE).evaluateAttributeExpressions(flowFile).getValue();
            String mainClass = context.getProperty(MAIN_CLASS).evaluateAttributeExpressions(flowFile).getValue().trim();
            String sparkMaster = context.getProperty(SPARK_MASTER).evaluateAttributeExpressions(flowFile).getValue().trim();
            String sparkYarnDeployMode = context.getProperty(SPARK_YARN_DEPLOY_MODE).evaluateAttributeExpressions(flowFile).getValue();
            String appArgs = context.getProperty(MAIN_ARGS).evaluateAttributeExpressions(flowFile).getValue().trim();
            String driverMemory = context.getProperty(DRIVER_MEMORY).evaluateAttributeExpressions(flowFile).getValue();
            String executorMemory = context.getProperty(EXECUTOR_MEMORY).evaluateAttributeExpressions(flowFile).getValue();
            String numberOfExecutors = context.getProperty(NUMBER_EXECUTORS).evaluateAttributeExpressions(flowFile).getValue();
            String sparkApplicationName = context.getProperty(SPARK_APPLICATION_NAME).evaluateAttributeExpressions(flowFile).getValue();
            String executorCores = context.getProperty(EXECUTOR_CORES).evaluateAttributeExpressions(flowFile).getValue();
            String networkTimeout = context.getProperty(NETWORK_TIMEOUT).evaluateAttributeExpressions(flowFile).getValue();
            String principal = context.getProperty(kerberosPrincipal).getValue();
            String keyTab = context.getProperty(kerberosKeyTab).getValue();
            String hadoopConfigurationResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue();
            String sparkConfs = context.getProperty(SPARK_CONFS).evaluateAttributeExpressions(flowFile).getValue();
            String extraFiles = context.getProperty(EXTRA_SPARK_FILES).evaluateAttributeExpressions(flowFile).getValue();
            Integer sparkProcessTimeout = context.getProperty(PROCESS_TIMEOUT).evaluateAttributeExpressions(flowFile).asTimePeriod(TimeUnit.SECONDS).intValue();
            String datasourceIds = context.getProperty(DATASOURCES).evaluateAttributeExpressions(flowFile).getValue();
            MetadataProviderService metadataService = context.getProperty(METADATA_SERVICE).asControllerService(MetadataProviderService.class);

            String[] confs = null;
            if (!StringUtils.isEmpty(sparkConfs)) {
                confs = sparkConfs.split("\\|");
            }

            String[] args = null;
            if (!StringUtils.isEmpty(appArgs)) {
                args = appArgs.split(",");
            }

            final List<String> extraJarPaths = new ArrayList<>();
            if (!StringUtils.isEmpty(extraJars)) {
                extraJarPaths.addAll(Arrays.asList(extraJars.split(",")));
            } else {
                getLog().info("No extra jars to be added to class path");
            }

            // If all 3 fields are filled out then assume kerberos is enabled, and user should be authenticated
            boolean authenticateUser = false;
            if (!StringUtils.isEmpty(principal) && !StringUtils.isEmpty(keyTab) && !StringUtils.isEmpty(hadoopConfigurationResources)) {
                authenticateUser = true;
            }
            System.out.println("##### authenticateUser " +authenticateUser);

            if (authenticateUser) {
                ApplySecurityPolicy applySecurityObject = new ApplySecurityPolicy();
                Configuration configuration;
                try {
                    getLog().info("Getting Hadoop configuration from " + hadoopConfigurationResources);
                    configuration = ApplySecurityPolicy.getConfigurationFromResources(hadoopConfigurationResources);

                    if (SecurityUtil.isSecurityEnabled(configuration)) {
                        getLog().info("Security is enabled");
                        isKerberized=true;
                        

                        if (principal.equals("") && keyTab.equals("")) {
                            getLog().error("Kerberos Principal and Kerberos KeyTab information missing in Kerboeros enabled cluster. {} ", new Object[]{flowFile});
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }

                        try {
                        	 boolean authenticationStatus = applySecurityObject.validateUserWithKerberos(logger, hadoopConfigurationResources, principal, keyTab);
                        	 if (authenticationStatus) {
                            getLog().info("User authentication initiated");

                        }
                            
                             else {
                                getLog().error("User authentication failed.  {} ", new Object[]{flowFile});
                                session.transfer(flowFile, REL_FAILURE);
                                return;
                            }

                        } catch (Exception unknownException) {
                            getLog().error("Unknown exception occurred while validating user : {}.  {} ", new Object[]{unknownException.getMessage(), flowFile});
                            session.transfer(flowFile, REL_FAILURE);
                            return;
                        }

                    }
                } catch (IOException e1) {
                    getLog().error("Unknown exception occurred while authenticating user : {} and flow file: {}", new Object[]{e1.getMessage(), flowFile});
                    session.transfer(flowFile, REL_FAILURE);
                    return;
                }
            }

            String sparkHome = context.getProperty(SPARK_HOME).evaluateAttributeExpressions(flowFile).getValue();

            // Build environment
            final Map<String, String> env = new HashMap<>();

            if (StringUtils.isNotBlank(datasourceIds)) {
                final StringBuilder datasources = new StringBuilder(10240);
                final ObjectMapper objectMapper = new ObjectMapper();
                final MetadataProvider provider = metadataService.getProvider();

                for (final String id : datasourceIds.split(",")) {
                    datasources.append((datasources.length() == 0) ? '[' : ',');

                    final Optional<Datasource> datasource = provider.getDatasource(id);
                    if (datasource.isPresent()) {
                        if (datasource.get() instanceof JdbcDatasource && StringUtils.isNotBlank(((JdbcDatasource) datasource.get()).getDatabaseDriverLocation())) {
                            final String[] databaseDriverLocations = ((JdbcDatasource) datasource.get()).getDatabaseDriverLocation().split(",");
                            extraJarPaths.addAll(Arrays.asList(databaseDriverLocations));
                        }
                        datasources.append(objectMapper.writeValueAsString(datasource.get()));
                    } else {
                        logger.error("Required datasource {} is missing for Spark job: {}", new Object[]{id, flowFile});
                        flowFile = session.putAttribute(flowFile, PROVENANCE_JOB_STATUS_KEY, "Invalid data source: " + id);
                        session.transfer(flowFile, REL_FAILURE);
                        return;
                    }
                }

                datasources.append(']');
                env.put("DATASOURCES", datasources.toString());
            }

             /* Launch the spark job as a child process */
            
            
            /* New Launcher */
            
            try {
                // get the FlowFile's signal attribute.
                String attribute = flowFile.getAttribute("ATTRIBUTE_SIGNAL");

                // get the current state from the NiFi state manager, and make sure everything is initialised.
                getCurrentState(context);
                
                System.out.println("##### ATTRIBUTE_SIGNAL " +flowFile.getAttribute("ATTRIBUTE_SIGNAL"));
                // if the FlowFile is a "Stop FlowFile," stop the spark job (decrement the number of running feeds).
                if (attribute.equals("stop")) {
                    stopSparkJob(context, flowFile);
                    
                } else {
                    boolean running = getSparkJobStatus(context, flowFile);
                    
                    System.out.println("##### running " +running);

                    LOG.info("Received a start or maintain message.");

                    // if the Spark streaming job is not running, start it.
                    if (!running) {
                        LOG.info("Spark job for " + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME)) ;

                       // executeSparkJob(context, flowFile);
                        System.out.println("##### Before setting arguments to launcher");
                        SparkLauncher launcher = new SparkLauncher(env)
                            .setAppResource(appJar)
                            .setMainClass(mainClass)
                            .setMaster(sparkMaster)
                            .setConf(SparkLauncher.DRIVER_MEMORY, driverMemory)
                            .setConf(SPARK_NUM_EXECUTORS, numberOfExecutors)
                            .setConf(SparkLauncher.EXECUTOR_MEMORY, executorMemory)
                            .setConf(SparkLauncher.EXECUTOR_CORES, executorCores)
                            .setConf(SPARK_NETWORK_TIMEOUT_CONFIG_NAME, networkTimeout)
                            .setSparkHome(sparkHome)
                            .setAppName(sparkApplicationName);
                        
                        

                        if (authenticateUser) {
                            launcher.setConf(SPARK_YARN_KEYTAB, keyTab);
                            launcher.setConf(SPARK_YARN_PRINCIPAL, principal);
                        }
                        if (args != null) {
                            launcher.addAppArgs(args);
                        }

                        if (confs != null) {
                            for (String conf : confs) {
                                getLog().info("Adding sparkconf '" + conf + "'");
                                launcher.addSparkArg(SPARK_CONFIG_NAME, conf);
                            }
                        }

                        if (!extraJarPaths.isEmpty()) {
                            for (String path : extraJarPaths) {
                                getLog().info("Adding to class path '" + path + "'");
                                launcher.addJar(path);
                            }
                        }
                        if (StringUtils.isNotEmpty(yarnQueue)) {
                            launcher.setConf(SPARK_YARN_QUEUE, yarnQueue);
                        }
                        if (StringUtils.isNotEmpty(extraFiles)) {
                            launcher.addSparkArg(SPARK_EXTRA_FILES_CONFIG_NAME, extraFiles);
                        }

                        SparkAppHandle handle;
                        
                        System.out.println("##### Before launching the job");
                        handle = launcher.startApplication();
                        if (handle == null) {
                        		getLog().error("Spark process timed out after {} seconds using flow file: {}  ", new Object[]{sparkProcessTimeout, flowFile});
                        		flowFile = session.putAttribute(flowFile, PROVENANCE_SPARK_EXIT_CODE_KEY, "Failed");
                            session.transfer(flowFile, REL_FAILURE);
                            throw new IOException("Unable to launch Spark application. No handle returned!");
                        }
                        
                        System.out.println("##### After launching the job");

                		/*
                		 * The SparkLauncher class utilises an asynchronous architecture. We register a listener.
                		 * When the status of the submitted Spark job changes (e.g., it is assigned an application ID), call
                		 * the setApplicationID method to save it for future reference.
                		 */
                        handle.addListener(new Listener() {
                            @Override
                            public void infoChanged(SparkAppHandle handle) {
                                if (handle.getAppId() != null) {
                                	
                                    LOG.info("Spark job now has an applicationID: " + handle.getAppId());
                                    
                                    
                                    try {
                                    		
                                    	System.out.println("##### Before setting up setApplicationID");
                                        setApplicationID(context, handle.getAppId(), flowFile);
                                       // session.transfer(flowFile, REL_SUCCESS);
                                        System.out.println("##### After setting up setApplicationID");
                                        //setState(context);
                                         
                                     //   LOG.info("Spark job for " + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME)  + " is currently running.");
                                    }  catch (IOException e) {
                                        LOG.error(e);
                                    }
                                }
                            }

                            @Override
                            public void stateChanged(SparkAppHandle handle) {
                            	
                            	 System.out.println("##### In stateChanged");
                            //	handle.stop();
                            }

                        });
                    }
                       

                        /* Read/clear the process input stream */
                  /*      InputStreamReaderRunnable inputStreamReaderRunnable = new InputStreamReaderRunnable(LogLevel.INFO, logger, spark.getInputStream());
                        Thread inputThread = new Thread(inputStreamReaderRunnable, "stream input");
                        inputThread.start();
                        

                         /* Read/clear the process error stream 
                        InputStreamReaderRunnable errorStreamReaderRunnable = new InputStreamReaderRunnable(LogLevel.INFO, logger, spark.getErrorStream());
                        Thread errorThread = new Thread(errorStreamReaderRunnable, "stream error");
                        errorThread.start(); */

                        logger.info("Waiting for Spark job to complete");

                         /* Wait for job completion */
                    /*    boolean completed = spark.waitFor(sparkProcessTimeout, TimeUnit.SECONDS);
                        if (!completed) {
                            spark.destroyForcibly();
                            
                            return;
                        }

                        int exitCode = spark.exitValue();

                        flowFile = session.putAttribute(flowFile, PROVENANCE_SPARK_EXIT_CODE_KEY, exitCode + "");
                        if (exitCode != 0) {
                            logger.error("ExecuteSparkJob for {} and flowfile: {} completed with failed status {} ", new Object[]{context.getName(), flowFile, exitCode});
                            flowFile = session.putAttribute(flowFile, PROVENANCE_JOB_STATUS_KEY, "Failed");
                            session.transfer(flowFile, REL_FAILURE);
                        } else {
                            logger.info("ExecuteSparkJob for {} and flowfile: {} completed with success status {} ", new Object[]{context.getName(), flowFile, exitCode});
                            flowFile = session.putAttribute(flowFile, PROVENANCE_JOB_STATUS_KEY, "Success");
                            session.transfer(flowFile, REL_SUCCESS);
                        }
                    } else {
                        LOG.info("Spark job for " + DelegateSparkKafkaUtilities.getProperty(context, flowFile, CATEGORY_NAME)  + " is currently running.");
                    } */

                    // if it is a start FlowFile, we must increment the number of running feeds.
                    if (attribute.equals("start")) {
                    	System.out.println("##### Before setting up addFeed");
                        addFeed(context, flowFile);
                        System.out.println("##### After setting up addFeed");
                    }
                }

                // save the new system state.
                System.out.println("##### Before setting up setState");
                setState(context);
                System.out.println("##### After setting up setState");

                // send the FlowFile to SUCCESS, as no exceptions occurred.
                session.transfer(flowFile, REL_SUCCESS);
            } catch (IOException e) {
                LOG.error("IO error monitoring or starting Spark streaming job.", e);

                session.transfer(flowFile, REL_FAILURE);
            } catch (YarnException e) {
                LOG.error("Yarn error monitoring or starting Spark streaming job.", e);

                session.transfer(flowFile, REL_FAILURE);
            } catch (InterruptedException e) {
                LOG.error("Interrupted exception while authenticating with Kerberos.", e);
            }
            
            

        } catch (final Exception e) {
            logger.error("Unable to execute Spark job {},{}", new Object[]{flowFile, e.getMessage()}, e);
            flowFile = session.putAttribute(flowFile, PROVENANCE_JOB_STATUS_KEY, "Failed With Exception");
            flowFile = session.putAttribute(flowFile, "Spark Exception:", e.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(@Nonnull final ValidationContext validationContext) {
        final Set<ValidationResult> results = new HashSet<>();

        if (validationContext.getProperty(DATASOURCES).isSet() && !validationContext.getProperty(METADATA_SERVICE).isSet()) {
            results.add(new ValidationResult.Builder()
                            .subject(METADATA_SERVICE.getName())
                            .input(validationContext.getProperty(METADATA_SERVICE).getValue())
                            .valid(false)
                            .explanation("Metadata Service is required when Data Sources is not empty")
                            .build());
        }

        return results;
    }
    
}
