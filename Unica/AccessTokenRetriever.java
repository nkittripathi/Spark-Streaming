package com.bank.nifi.processors.unica;

import net.sf.json.JSONException;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created with IntelliJ IDEA.
 * User: Ankit Tripathi
 * Date: 25/04/18
 * Time: 3:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class AccessTokenRetriever {
	public static final String PARAM_CLIENT_ID ="client_id";
	public static final String PARAM_CLIENT_SECRET = "client_secret";
	public static final String PARAM_REFRESH_TOKEN = "refresh_token";
	public static final String PARAM_GRANT_TYPE = "grant_type";
	public static final String GRANT_TYPE = "refresh_token";
	private String url;
	private HttpClient httpClient;
	private String responseText;
	public AccessTokenRetriever(String url) {
		this(url, new HttpClient());
		
	}

	AccessTokenRetriever(String url, HttpClient httpClient) {
		this.url = url;
		this.httpClient = httpClient;
	}
	public String retrieveToken(String clientId, String clientSecret, String refereshToken) throws JSONException, IOException, RuntimeException {
		PostMethod post = createPost(clientId, clientSecret, refereshToken);
		httpClient.executeMethod(post);
		responseText = getResponseText(post);
		return responseText;
	}
	
	public String retrieveToken(String clientId, String clientSecret, String refereshToken, String PROXY_HOST, int PROXY_PORT) throws JSONException, RuntimeException, IOException {
		PostMethod post = createPost(clientId, clientSecret, refereshToken);
		HostConfiguration config = httpClient.getHostConfiguration();
		config.setProxy(PROXY_HOST, PROXY_PORT);
		httpClient.executeMethod(post);
		responseText = getResponseText(post);
		return responseText;
		
	}
	private PostMethod createPost(String clientId, String clientSecret, String refereshToken) {
		PostMethod post = new PostMethod(url);
		post.setParameter(PARAM_CLIENT_ID, clientId);
		post.setParameter(PARAM_CLIENT_SECRET, clientSecret);
		post.setParameter(PARAM_REFRESH_TOKEN, refereshToken);
		post.setParameter(PARAM_GRANT_TYPE, GRANT_TYPE);
		return post;
	}
	private String getResponseText(PostMethod post) throws IOException {
        InputStream is = post.getResponseBodyAsStream();
        InputStreamReader inReader = new InputStreamReader(is);
        StringBuffer responseBuffer = new StringBuffer();
        char[] buffer = new char[1024];
        int bytes;
        while ((bytes = inReader.read(buffer)) != -1) {
        responseBuffer.append(buffer, 0, bytes);
        }    
        return responseBuffer.toString();
	}

}
