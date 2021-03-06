/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bank.nifi.processors.streaming;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Set;

@Tags({"Kotak","Teradata","Spark"})
@CapabilityDescription("This processor is used to generate start/stop signal in attribute ATTRIBUTE_SIGNAL on the event of start/stop of processor")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="ATTRIBUTE_SIGNAL", description="This is to send signal to Spark Streaming Processor")})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
public class SignalGenerator extends AbstractProcessor {

	public static final PropertyDescriptor FEED_NAME = new PropertyDescriptor
			.Builder().name("Feed Name")
			.displayName("Feed Name")
			.description("The Kylo feed name")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor CATEGORY_FIELD = new PropertyDescriptor
			.Builder().name("Category")
			.displayName("Category Name")
			.description("The Kylo category name")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor BOOTSTRAP_SERVERS = new PropertyDescriptor
			.Builder().name("Bootstrap Servers")
			.displayName("Bootstrap Servers")
			.description("The Kylo category name")
			.defaultValue("localhost:9092")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor ZOOKEEPER_CONNECTION = new PropertyDescriptor
			.Builder().name("Zookeeper Address")
			.displayName("Zookeeper Address")
			.description("Zookeeper Address")
			.defaultValue("localhost:2181")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor TOPIC_LIST= new PropertyDescriptor
			.Builder().name("Topics")
			.displayName("Topics")
			.description("Comma seperated list of Topics")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor GROUP_ID = new PropertyDescriptor
			.Builder().name("Group Id")
			.displayName("Group Id")
			.description("Group Id")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor KEY_DESERIALIZER = new PropertyDescriptor
			.Builder().name("Key Deserializer")
			.displayName("Key Deserializer")
			.description("GKey Deserializer")
			.required(false)
			.defaultValue("org.apache.kafka.common.serialization.StringDeserializer")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();


	public static final PropertyDescriptor VALUE_DESERIALIZER = new PropertyDescriptor
			.Builder().name("Value Deserializer")
			.displayName("Value Deserializer")
			.description("Value Deserializer")
			.defaultValue("lorg.apache.kafka.common.serialization.StringDeserializer")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor ENABLE_AUTO_COMMIT = new PropertyDescriptor
			.Builder().name("enable.auto.commit")
			.displayName("enable.auto.commit")
			.description("enable.auto.commit")
			.defaultValue("true")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor AUTO_OFFSET_RESET = new PropertyDescriptor
			.Builder().name("auto.offset.reset")
			.displayName("auto.offset.reset")
			.description("auto.offset.reset")
			.defaultValue("true")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor BASE_URL= new PropertyDescriptor
			.Builder().name("Kylo Base URL")
			.displayName("Kylo Base URL")
			.description("Provide Kylo base URL for rest call")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor KYLO_USER= new PropertyDescriptor
			.Builder().name("Kylo Username")
			.displayName("Kylo Username")
			.description("Provide kylo valid username ")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor KYLO_PASSWORD= new PropertyDescriptor
			.Builder().name("Kylo Password")
			.displayName("Kylo Password")
			.description("Provide Kylo valid password")
			.required(true)
			.defaultValue("")
	        .sensitive(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final Relationship success = new Relationship.Builder()
			.name("success")
			.description("success relationship")
			.build();

	public static final Relationship failure = new Relationship.Builder()
			.name("failure")
			.description("failed relationship")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(FEED_NAME);
		descriptors.add(CATEGORY_FIELD);
		descriptors.add(BOOTSTRAP_SERVERS);
		descriptors.add(ZOOKEEPER_CONNECTION);
		descriptors.add(TOPIC_LIST);
		descriptors.add(AUTO_OFFSET_RESET);
		descriptors.add(GROUP_ID);
		descriptors.add(KEY_DESERIALIZER);
		descriptors.add(VALUE_DESERIALIZER);
		descriptors.add(ENABLE_AUTO_COMMIT);
		descriptors.add(BASE_URL);
		descriptors.add(KYLO_USER);
		descriptors.add(KYLO_PASSWORD);				

		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(success);
		relationships.add(failure);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	ProcessSession session;
	ComponentLog logger;
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		logger = getLogger();
		this.session =session;
		if (flowFile == null) {
			flowFile = session.create();
		}

		flowFile = session.putAttribute(flowFile, "ATTRIBUTE_SIGNAL", "start");
		flowFile = session.putAttribute(flowFile, "feedname", context.getProperty(FEED_NAME).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "category", context.getProperty(CATEGORY_FIELD).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "bootstrapServer", context.getProperty(BOOTSTRAP_SERVERS).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "zookeeperConnection", context.getProperty(ZOOKEEPER_CONNECTION).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "topicList", context.getProperty(TOPIC_LIST).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "groupID", context.getProperty(GROUP_ID).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "keyDeserializer", context.getProperty(KEY_DESERIALIZER).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "autoOffsetReset", context.getProperty(AUTO_OFFSET_RESET).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "valueDeserializer", context.getProperty(VALUE_DESERIALIZER).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "enableAutoCommit", context.getProperty(ENABLE_AUTO_COMMIT).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "baseurl", context.getProperty(BASE_URL).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "kyloUser", context.getProperty(KYLO_USER).evaluateAttributeExpressions(flowFile).getValue());
		flowFile = session.putAttribute(flowFile, "kyloPassword", context.getProperty(KYLO_PASSWORD).evaluateAttributeExpressions(flowFile).getValue());
		logger.info("sending start signal attribute");		
		session.transfer(flowFile, success);
		session.commit();

	}
	@OnStopped
	public void sendStopSignal() {
		FlowFile flowFile = session.get();
		flowFile = session.create();
		flowFile = session.putAttribute(flowFile, "ATTRIBUTE_SIGNAL", "stop");
		logger.info("sending stop signal attribute");	
		session.transfer(flowFile, success);	
		session.commit();
	}

}
