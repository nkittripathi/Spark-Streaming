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
package com.bank.nifi.processors.unica;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Proxy;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConnectToUnica extends AbstractProcessor {

	public static final PropertyDescriptor WCA_AUTH_URL = new PropertyDescriptor
			.Builder().name("WCA_Auth_URL")
			.displayName("WCA Auth URL")
			.description("Provide URL")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor EMAIL_URL = new PropertyDescriptor
			.Builder().name("EMAIL_URL")
			.displayName("EMAIL URL")
			.description("Provide EMAIL URL")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor SMS_URL = new PropertyDescriptor
			.Builder().name("SMS_URL")
			.displayName("SMS URL")
			.description("Provide SMS URL")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor
			.Builder().name("CLIENT_ID")
			.displayName("Client ID")
			.description("Provide Client ID")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor CLIENT_SECRET_ID = new PropertyDescriptor
			.Builder().name("CLIENT_SECRET_ID")
			.displayName("Client Secret ID")
			.description("Provide Client SECRET ID")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor CLIENT_REFRESH_TOKEN= new PropertyDescriptor
			.Builder().name("CLIENT_REFRESH_TOKEN")
			.displayName("Client Refresh Token")
			.description("Provide Client Refresh Token")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor
			.Builder().name("PROXY_HOST")
			.displayName("Proxy Host")
			.description("Provide Client Refresh Token")
			.required(false)
			.defaultValue("10.10.2.88")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor
			.Builder().name("PROXY_PORT")
			.displayName("Proxy Port")
			.description("Provide Proxy Port")
			.required(false)
			.defaultValue("8080")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final Relationship success = new Relationship.Builder()
			.name("Sucess")
			.description("success relationship")
			.build();
	public static final Relationship failure = new Relationship.Builder()
			.name("Sucess")
			.description("success relationship")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(WCA_AUTH_URL);
		descriptors.add(EMAIL_URL);
		descriptors.add(SMS_URL);
		descriptors.add(CLIENT_ID);
		descriptors.add(CLIENT_SECRET_ID);
		descriptors.add(CLIENT_REFRESH_TOKEN);
		descriptors.add(PROXY_HOST);
		descriptors.add(PROXY_PORT);
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

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	URL url;
	String sendEncoding;
	String wcaAuthURL;
	String emailURL;
	String smsURL;
	String clientID;
	String clientSecretID;
	String clientRefreshToken;
	String response;
	String proxyPort;
	String proxyHost;
	String token;
	Proxy proxy;
	FlowFile flowFile;
	boolean isProxyset;
	boolean accessTokenSet = true;
	ComponentLog logger;

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		logger = getLogger();
		logger.info("###### 1 in onTrigger");
		flowFile = session.get();

		if (flowFile == null) {
			return;
		}

		wcaAuthURL = context.getProperty(WCA_AUTH_URL).evaluateAttributeExpressions(flowFile).getValue().trim();
		emailURL = context.getProperty(EMAIL_URL).evaluateAttributeExpressions(flowFile).getValue().trim();
		smsURL = context.getProperty(SMS_URL).evaluateAttributeExpressions(flowFile).getValue().trim();
		clientID = context.getProperty(CLIENT_ID).evaluateAttributeExpressions(flowFile).getValue().trim();
		clientSecretID = context.getProperty(CLIENT_SECRET_ID).evaluateAttributeExpressions(flowFile).getValue().trim();
		clientRefreshToken = context.getProperty(CLIENT_REFRESH_TOKEN).evaluateAttributeExpressions(flowFile).getValue().trim();
		proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions(flowFile).getValue().trim();
		proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions(flowFile).getValue().trim();
		AccessTokenRetriever accessTokenRetriever = new AccessTokenRetriever(wcaAuthURL);
		
		HttpsURLConnection httpsURLConn = null;
		HttpURLConnection httpURLConn = null;
		String emailRequest = "<Envelope><Body><SendMailing>\r\n" +
				"<MailingId>1058734</MailingId>\r\n" +
				"<RecipientEmail>prasad.dhoble@kotak.com</RecipientEmail>\r\n" +
				"</SendMailing>\r\n" +
				"</Body></Envelope>";


		String smsRequest = "{\n" + 
				"  \"content\": \"Hey Test SMS from Silverpop\",\n" + 
				"  \"contacts\": [\n" + 
				"    {\n" + 
				"      \"contactId\": \"266867\",\n" + 
				"      \"contactLookup\": [\n" + 
				"        {\n" + 
				"          \"name\": \"Mobile_No\",\n" + 
				"          \"value\": \"919619194811\",\n" + 
				"          \"channel\": \"SMS\"\n" + 
				"        }\n" + 
				"      ]\n" + 
				"    }\n" + 
				"  ],\n" + 
				"  \"channelQualifier\": \"140084\"\n" + 
				"}";


		sendEncoding = "utf-8";

		try {
			logger.info("Getting token");
			if (proxyHost.isEmpty() || proxyPort.isEmpty()) {
				token = accessTokenRetriever.retrieveToken(clientID ,clientSecretID, clientRefreshToken, proxyHost, Integer.parseInt(proxyPort));
				proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
				isProxyset = true;
			} else {				
				isProxyset = false;
				token = accessTokenRetriever.retrieveToken(clientID ,clientSecretID, clientRefreshToken);		
			}
			if (token==null) {
				return;
			}
			httpsURLConn = sendEmail(new URL(emailURL));
			response = getHttpsResponse(httpsURLConn, token, emailRequest);
			
			httpURLConn = sendSMS(new URL(smsURL));
			response = getHttpResponse(httpURLConn, token, smsRequest);
			System.out.println(response);
			session.transfer(flowFile, success);

		} catch (MalformedURLException  e) {
			session.transfer(flowFile, failure);
			logger.error("Error" + e.getMessage());
		} 
	}

	/**
	 * Set email properties
	 */

	public HttpsURLConnection sendEmail(URL emailURL) {

		HttpsURLConnection urlConnection;
		try {
			if (isProxyset) {
				urlConnection = (HttpsURLConnection)emailURL.openConnection(proxy);
			}
			else {
				urlConnection = (HttpsURLConnection)emailURL.openConnection();
			}
			urlConnection.setRequestMethod("POST");
			urlConnection.setDoOutput(true);
			urlConnection.setRequestProperty("Content-Type","text/xml;charset=" + sendEncoding);
			return urlConnection;
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
			return null;
		}
	}

	/**
	 * Set SMS properties
	 */
	public HttpURLConnection sendSMS(URL smsURL) {

		HttpURLConnection smsURLConn;
		try {
			if (isProxyset) {
				smsURLConn = (HttpURLConnection)smsURL.openConnection(proxy);
			}
			else {
				smsURLConn = (HttpURLConnection)smsURL.openConnection();
			}
			smsURLConn.setRequestMethod("POST");
			smsURLConn.setDoOutput(true);
			smsURLConn.setRequestProperty("Content-Type","application/json");
			logger.info("##### In sendSMS");
			return smsURLConn;
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
			return null;
		}
	}

	/**
	 * Get https response of email
	 */
	public String getHttpsResponse( HttpsURLConnection httpsURLConnection, String accessToken, String request) {
		OutputStream out;
		InputStream in;
		if (accessTokenSet) {
			httpsURLConnection.setRequestProperty("Authorization", "Bearer " + accessToken );
			accessTokenSet=false;
		}

		try {
			httpsURLConnection.connect();
			httpsURLConnection.getResponseCode();
			out = httpsURLConnection.getOutputStream();
			out.write(request.getBytes(sendEncoding));
			out.flush();
			System.out.println(httpsURLConnection.getResponseCode());
			in = httpsURLConnection.getInputStream();
			InputStreamReader inReader = new InputStreamReader(in, sendEncoding);
			StringBuffer responseBuffer = new StringBuffer();
			char[] buffer = new char[1024];
			int bytes;
			while ((bytes = inReader.read(buffer)) != -1) {
				responseBuffer.append(buffer, 0, bytes);
			}
			String response = responseBuffer.toString();
			httpsURLConnection.disconnect();
			return response;

		} catch (MalformedURLException  e) {
			logger.error("Error" + e.getMessage());
			return null;
		} catch (ProtocolException e) {
			logger.error("Error" + e.getMessage());
			return null;
		} catch (IOException e) {
			logger.error("Error" + e.getMessage());
			return null;
		}
	}

	/**
	 * Get http response of SMS
	 */
	public String getHttpResponse( HttpURLConnection httpURLConnection, String accessToken, String request) {
		OutputStream out;
		InputStream in;
		if (accessTokenSet) {
			httpURLConnection.setRequestProperty("Authorization", "Bearer " + accessToken );
			accessTokenSet=false;
		}
		try {
			httpURLConnection.connect();
			httpURLConnection.getResponseCode();
			out = httpURLConnection.getOutputStream();
			out.write(request.getBytes(sendEncoding));
			out.flush();
			System.out.println(httpURLConnection.getResponseCode());
			in = httpURLConnection.getInputStream();
			InputStreamReader inReader = new InputStreamReader(in, sendEncoding);
			StringBuffer responseBuffer = new StringBuffer();
			char[] buffer = new char[1024];
			int bytes;
			while ((bytes = inReader.read(buffer)) != -1) {
				responseBuffer.append(buffer, 0, bytes);
			}
			String response = responseBuffer.toString();
			httpURLConnection.disconnect();
			return response;

		} catch (MalformedURLException  e) {
			logger.error("Error" + e.getMessage());
			return null;
		} catch (ProtocolException e) {
			logger.error("Error" + e.getMessage());
			return null;
		} catch (IOException e) {
			logger.error("Error" + e.getMessage());
			return null;
		}
	}
}
