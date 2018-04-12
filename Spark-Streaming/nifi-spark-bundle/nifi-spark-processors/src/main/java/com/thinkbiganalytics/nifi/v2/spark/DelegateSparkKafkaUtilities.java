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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.ConfigurationProvider;
import org.apache.hadoop.yarn.conf.ConfigurationProviderFactory;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.processor.ProcessContext;
import org.apache.spark.launcher.SparkLauncher;
import com.thinkbiganalytics.nifi.security.ApplySecurityPolicy;
import org.apache.nifi.flowfile.FlowFile;
import com.thinkbiganalytics.nifi.security.SecurityUtil;

import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedAction;
import java.util.HashMap;

/**
 * Static utility methods to aid in the modular design of the DelegateSparkKafka
 * processor. This class provides Yarn, Kerberos, and SparkLauncher integration
 * for interaction with the cluster.
 *
 * @author Donald Campbell
 */
public class DelegateSparkKafkaUtilities {

    private static YarnClient client = null;

    private static final Log LOG = LogFactory.getLog(DelegateSparkKafkaUtilities.class);

    private static YarnConfiguration conf = new YarnConfiguration();

    private static UserGroupInformation userGroupInformation;

    private static boolean isKerberized;

    /**
     * This method configures the connection to Yarn (for both submission of Spark jobs and interrogation of running jobs).
     *
     * @param kerberized     A boolean indicating whether the cluster has Kerberos integration.
     * @param configLocation Location of core-site.xml and yarn-site.xml configuration files.
     * @param principal      Kerberos principal (can be NULL if kerberized is false).
     * @param keytab         Kerberos keytab name (can be NULL if kerberized is false).
     * @throws YarnException
     * @throws IOException
     */
    public static void initialiseYarn(boolean kerberized, String configLocation, String principal, String keytab) throws YarnException, IOException, InterruptedException {
        if (client == null) {
            isKerberized = kerberized;
            System.out.println("##### isKerberized " +isKerberized);
            LOG.info("Initialising yarn.");

            ConfigurationProvider configurationProvider = ConfigurationProviderFactory.getConfigurationProvider(conf);

            InputStream yarnSiteXMLInputStream = configurationProvider.getConfigurationInputStream(conf, "/etc/hadoop/conf/yarn-site.xml");
            InputStream coreSiteXMLInputStream = configurationProvider.getConfigurationInputStream(conf, "/etc/hadoop/conf/core-site.xml");
           // Configuration configuration = ApplySecurityPolicy.getConfigurationFromResources(configLocation);
            System.out.println("##### yarnSiteXMLInputStream " +yarnSiteXMLInputStream);
            if (yarnSiteXMLInputStream != null) {
                conf.addResource(yarnSiteXMLInputStream);
            }
            System.out.println("##### coreSiteXMLInputStream " +coreSiteXMLInputStream);
            if (coreSiteXMLInputStream != null) {
                conf.addResource(coreSiteXMLInputStream);
            }

            if (isKerberized) {
                UserGroupInformation.setConfiguration(conf);

                LOG.info("Kinitting with " + principal + "," + keytab);

                userGroupInformation = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytab);

                if (userGroupInformation == null) {
                    LOG.error("Could not successfully kinit.");
                } else {
                    LOG.info("Successfully kinitted.");
                    LOG.info("Logged in user: " + userGroupInformation.getUserName() + ", " + userGroupInformation.getRealUser());
                    LOG.info(userGroupInformation.getTokens());
                }

                client = userGroupInformation.doAs(new PrivilegedExceptionAction<YarnClient>() {
                    @Override
                    public YarnClient run() throws YarnException, IOException {
                        client = YarnClient.createYarnClient();
                        client.init(conf);
                        client.start();

                        return client;
                    }
                });
            } else {
                client = YarnClient.createYarnClient();
                client.init(conf);
                client.start();
            }
        }
    }

    public static UserGroupInformation getUserGroupInformation() {
        return userGroupInformation;
    }

    /**
     * This method creates a new instance of a YarnClient object, with optional kerberization (with principal and keytab).
     *
     * @return A new instance of a YarnClient object.
     * @throws IOException
     * @throws YarnException
     */
    public static YarnClient getYarnClient() throws YarnException, IOException {
        return client;
    }

    /**
     * This method attempts to look up a processor property from the current operating context.
     * @param context the operating context to look in.
     * @param propertyDescriptor the property definition
     * @return a possibly empty or null string with the property value.
     */
    public static String getProperty(final ProcessContext context, FlowFile flowFile, final PropertyDescriptor propertyDescriptor) throws IOException {

        PropertyValue value = context.getProperty(propertyDescriptor);
        if (propertyDescriptor.isExpressionLanguageSupported()) {
            if (flowFile == null) {
                throw new IOException("Passed a NULL flowFile to getProperty for a property that supports expression language.");
            }

            return value.evaluateAttributeExpressions(flowFile).getValue();
        } else {
            return value.getValue();
        }

//        return propertyDescriptor.isExpressionLanguageSupported()
//                ? context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue()
//                : context.getProperty(propertyDescriptor).getValue();

    }

    /**
     * This method attempts to look up a processor property from the current operating context.
     * @param context the operating context to look in.
     * @param propertyDescriptor the property definition
     * @return a possibly empty or null string with the property value.
     */
    public static String getProperty(final ProcessContext context, final PropertyDescriptor propertyDescriptor) throws IOException {
        return getProperty(context, null, propertyDescriptor);
    }

    /**
     * This method creates a new instance of a SparkLauncher object.
     *
     * @return A new instance of a SparkLauncher object.
     */
    public static SparkLauncher getNewSparkLauncher() throws InterruptedException {
        return new SparkLauncher(new HashMap<>());
    }
}
