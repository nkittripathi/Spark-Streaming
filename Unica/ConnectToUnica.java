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

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor EMAIL_ID = new PropertyDescriptor
			.Builder().name("Email Id")
			.displayName("Email Ids")
			.description("Provide Email Id")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor CONTACT_NUMBER = new PropertyDescriptor
			.Builder().name("Contact Number")
			.displayName("Contact Number")
			.description("Provide Contact Number")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor CONTENT = new PropertyDescriptor
			.Builder().name("Content")
			.displayName("Content")
			.description("Content to be send")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor
			.Builder().name("PROXY_PORT")
			.displayName("Proxy Port")
			.description("Provide Proxy Port")
			.required(false)
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
		descriptors.add(EMAIL_ID);
		descriptors.add(CONTACT_NUMBER);
		descriptors.add(CONTENT);
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
	String sendEncoding = "utf-8";
	String wcaAuthURL;
	String accessToken;
	Proxy proxy;
	boolean isProxyset;
	boolean accessTokenSet = true;
	boolean validToken = false;
	ComponentLog logger;
	final String SUCCESS = "<SUCCESS>(.+?)</SUCCESS>";
	final String FAULT_CODE = "<FaultCode>(.+?)</FaultCode>";
	final String FAULT_MESSAGE = "<FaultString>(.+?)</FaultString>";
	final String SPACE ="";

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		logger = getLogger();
		FlowFile flowFile = session.get();

		if (flowFile == null) {
			return;
		}

		String response;
		String emailURL = context.getProperty(EMAIL_URL).evaluateAttributeExpressions(flowFile).getValue().trim();
		String smsURL = context.getProperty(SMS_URL).evaluateAttributeExpressions(flowFile).getValue().trim();
		String clientID = context.getProperty(CLIENT_ID).evaluateAttributeExpressions(flowFile).getValue().trim();
		String clientSecretID = context.getProperty(CLIENT_SECRET_ID).evaluateAttributeExpressions(flowFile).getValue().trim();
		String clientRefreshToken = context.getProperty(CLIENT_REFRESH_TOKEN).evaluateAttributeExpressions(flowFile).getValue().trim();		
		String proxyPort ="";
		String proxyHost = "";
		wcaAuthURL = context.getProperty(WCA_AUTH_URL).evaluateAttributeExpressions(flowFile).getValue().trim();

		if (context.getProperty(PROXY_HOST).isSet()) {
			proxyPort = context.getProperty(PROXY_HOST).evaluateAttributeExpressions(flowFile).getValue().trim();			
		}
		if (context.getProperty(PROXY_PORT).isSet()) {
			proxyHost = context.getProperty(PROXY_PORT).evaluateAttributeExpressions(flowFile).getValue().trim();		
		}

		String content = context.getProperty(CONTENT).evaluateAttributeExpressions(flowFile).getValue().trim();
		String emailId = context.getProperty(CONTENT).evaluateAttributeExpressions(flowFile).getValue().trim();
		String contactNumber = context.getProperty(CONTENT).evaluateAttributeExpressions(flowFile).getValue().trim();

		String emailRequest = formRequestXMLBody(content);

		String smsRequest = formRequestJSONBody(content);

		logger.info(" clientID "+ clientID);
		logger.info(" clientSecretID "+ clientSecretID);
		logger.info(" clientRefreshToken "+ clientRefreshToken);
		logger.info("Email send time : "+ System.currentTimeMillis());

		try {
			logger.info("Getting access token");
			if (validToken) {	
				logger.info("Already have valid access token");

			} else {		
				accessToken = getTokenResponse(clientID,clientSecretID, clientRefreshToken, proxyHost, proxyPort);
				validToken = true;
				logger.info("Token recieving time : "+ System.currentTimeMillis());
				logger.info("Token Response is : " + accessToken);
			}
			response = sendEmail(new URL(emailURL), accessToken, emailRequest);
			if (getSuccessResult(response).equals("true")) {

				logger.info("response is : " + response);

			} else if (getFailResult(response).equals("invalid_token")){

				accessToken = getTokenResponse(clientID,clientSecretID, clientRefreshToken, proxyHost, proxyPort);	
				response = sendEmail(new URL(emailURL), accessToken, emailRequest);
			}

			logger.info("Email Delivered Time : "+ System.currentTimeMillis());
			logger.info("response is : " + response);

			logger.info("Sending SMS");
			response = null;
			response = sendSMS(new URL(smsURL), accessToken, smsRequest);
			logger.info("SMS response is : " + response);
			if (parseJson(response).get("generalErrors")== null) {
				if (parseJson(response).get("fieldErrors")== null) {
					logger.info("SMS sent successfully");									
				} else {
					logger.info("SMS was not sent because " + (parseJson(response).get("fieldErrors"))) ;	
				}
			} else {
				logger.info("SMS was not sent because " + (parseJson(response).get("generalErrors"))) ;
			};
			System.out.println("SMS Reponse is " + response);	
			response = null;
			session.transfer(flowFile, success);
		} catch (MalformedURLException  e) {
			session.transfer(flowFile, failure);
			logger.error("Error" + e.getMessage());
		} catch (JSONException  e) {
			session.transfer(flowFile, failure);
			logger.error("Error : Invalid format of token return" + e.getMessage());
		} catch (RuntimeException  e) {
			session.transfer(flowFile, failure);
			logger.error("Error" + e.getMessage());
		} 
	}

	public String formRequestXMLBody(String request) {
		String req;
		req = "<Envelope><Body><SendMailing>\r\n" +
				"<MailingId>1248923</MailingId>\r\n" +
				"<RecipientEmail>trip.ankit87@gmail.com</RecipientEmail>\r\n" +
				"</SendMailing>\r\n" +
				"</Body></Envelope>";

		return req;
	}

	public String formRequestJSONBody(String request) {
		String req;
		req = "{\"content\": \"Hey Ankit! You have suucessfully tested your NiFi processor.\"," +
				" \"contacts\": [" +
				" { " +                                               
				"  \"contactLookup\": [ "+
				"   { "+
				"    \"name\": \"Mobile_No\", "+
				"    \"value\": \"918095956000\", "+
				"   \"channel\": \"SMS\" "+
				"  } "+
				"  ] "+
				"  } "+
				" ], "+
				"  \"channelQualifier\": \"145024\" " +
				"}" ;

		return req;
	}

	/*
	 * Read JSON response
	 */

	public JSONObject parseJson(String response)  {
		System.out.println(response);
		JSONObject json = (JSONObject) JSONSerializer.toJSON(response);
		return json;        
	}   
	/*
	 * Parse XML response to get Success Status
	 */

	public String getSuccessResult(String response) {
		String result="result";
		Pattern pattern = Pattern.compile(SUCCESS);
		Matcher match = pattern.matcher(response);
		if (match.find(1)) {
			result= (match.group(1));
		}
		return result;
	}

	/*
	 * Parse XML response to get fault code
	 */
	public String getFailResult(String response) {

		String result="failed";
		Pattern pattern = Pattern.compile(FAULT_CODE);

		Matcher match = pattern.matcher(response);
		if (match.find(1)) {
			result= (match.group(1));
		}
		return result;	
	}

	/*
	 * Parse XML response to get fault detail
	 */
	public String getFailDetail(String response) {

		String detailReason="Reason";
		Pattern pattern = Pattern.compile(FAULT_MESSAGE);
		Matcher match = pattern.matcher(response);
		if (match.find(1)) {
			detailReason= (match.group(1));
		}
		return detailReason;	
	}

	/**
	 * Set email properties
	 */

	public String getTokenResponse(String clientID, String clientSecretID, String clientRefreshToken, String proxyHost, String proxyPort) {

		String tokenResponse =null;
		AccessTokenRetriever accessTokenRetriever = new AccessTokenRetriever(wcaAuthURL);
		try {
			if (!proxyHost.equals(SPACE)) {
				tokenResponse = accessTokenRetriever.retrieveToken(clientID ,clientSecretID, clientRefreshToken, proxyHost, Integer.parseInt(proxyPort));
				logger.info("Token with proxy" + tokenResponse);
				proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
				isProxyset = true;
			} else {		
				logger.info("Token without proxy");
				isProxyset = false;
				tokenResponse = accessTokenRetriever.retrieveToken(clientID ,clientSecretID, clientRefreshToken);	
				logger.info("Token without proxy" + tokenResponse);
			}

		} catch (MalformedURLException  e) {
			logger.error("Error" + e.getMessage());
		} catch (JSONException  e) {
			logger.error("Error : Invalid format of token return" + e.getMessage());
		} catch (IOException  e) {
			logger.error("Error" + e.getMessage());
		} catch (RuntimeException  e) {
			logger.error("Error" + e.getMessage());
		}

		JSONObject readTokenResponse = (JSONObject) JSONSerializer.toJSON(tokenResponse);

		if (readTokenResponse.containsKey("error")) {
			logger.error(readTokenResponse.getString("error") + " Reason : " + readTokenResponse.getString("error_description"));
			return readTokenResponse.getString("error");
		}
		return readTokenResponse.getString("access_token");
	}

	public String sendEmail(URL emailURL, String accessToken, String request) {
		OutputStream out;
		InputStream in;
		HttpsURLConnection httpsURLConnection;
		try {
			if (isProxyset) {
				httpsURLConnection = (HttpsURLConnection)emailURL.openConnection(proxy);
			}
			else {
				httpsURLConnection = (HttpsURLConnection)emailURL.openConnection();
			}
			httpsURLConnection.setRequestMethod("POST");
			httpsURLConnection.setDoOutput(true);
			httpsURLConnection.setRequestProperty("Content-Type","text/xml;charset=" + sendEncoding);
			httpsURLConnection.setRequestProperty("Authorization", "Bearer " + accessToken );
			httpsURLConnection.connect();
			out = httpsURLConnection.getOutputStream();
			out.write(request.getBytes(sendEncoding));
			out.flush();
			logger.info("Email Response code is " + httpsURLConnection.getResponseCode());
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
		} catch (Exception e) {
			logger.error("Error" + e.getMessage());
			return null;
		}
	}

	/**
	 * Set SMS properties
	 */
	public String sendSMS(URL smsURLConn, String accessToken, String request) {
		OutputStream out;
		InputStream in;
		HttpURLConnection httpURLConnection;
		try {
			if (isProxyset) {
				httpURLConnection = (HttpURLConnection)smsURLConn.openConnection(proxy);
			}
			else {
				httpURLConnection = (HttpURLConnection)smsURLConn.openConnection();
			}
			httpURLConnection.setRequestMethod("POST");
			httpURLConnection.setDoOutput(true);
			httpURLConnection.setRequestProperty("Content-Type","application/json;charset=" + sendEncoding);
			httpURLConnection.setRequestProperty("Authorization", "Bearer " + accessToken );
			httpURLConnection.connect();
			out = httpURLConnection.getOutputStream();
			out.write(request.getBytes(sendEncoding));
			out.flush();
			logger.info("SMS Response code is " + httpURLConnection.getResponseCode());
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
