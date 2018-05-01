package com.bank.spark.kafka.streaming;

import sun.misc.BASE64Encoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

@SuppressWarnings("restriction")
public class GetProcessorDetails {
	
	private static final Logger log = LoggerFactory.getLogger(GetProcessorDetails.class);	

	/**
     * getFirstProcessorName method return first processor id of the flow.
     *
     * @param baseurl kylo rest url.
     * @param username  admin username.
     * @param password  admin password.
     * @param feedname Kylo feedname.
     * @param category Kylo category name.
     */

	public static String getFirstProcessorName (String baseurl, String username, String password, String feedname, String category) {
		log.info("getting first processor ID");
		String url = baseurl + "/feedmgr/nifi/flow/feed/" + category + "." + feedname;
		log.info("getting first processor ID from " + url);
		String authString = username + ":" + password;
        String authStringEnc = new BASE64Encoder().encode(authString.getBytes());
        System.out.println("Base64 encoded auth string: " + authStringEnc);
        Client restClient = Client.create();
        WebResource webResource = restClient.resource(url);
        ClientResponse resp = webResource.accept("application/json")
                                         .header("Authorization", "Basic " + authStringEnc)
                                         .get(ClientResponse.class);
        if(resp.getStatus() != 200){
        		log.error("returning first processor ID");
        }
        
        log.info("status code of kylo rest call" + resp.getStatus());
        
        String output = resp.getEntity(String.class);
        JSONObject obj = parseJson(output);
        log.info("first processor ID is : " + obj.getJSONArray("startingProcessors").getJSONObject(0).getString("id"));
		return obj.getJSONArray("startingProcessors").getJSONObject(0).getString("id");
	}
	
	/**
     * parseJson method return json object of response.
     *
     * @param baseurl kylo rest url.
     * @param username  admin username.
     * @param password  admin password.
     * @param feedname Kylo feedname.
     * @param category Kylo category name.
     */
	
	public static JSONObject parseJson(String response)  {
        JSONObject json = (JSONObject) JSONSerializer.toJSON(response);
        return json;        
	} 
}
