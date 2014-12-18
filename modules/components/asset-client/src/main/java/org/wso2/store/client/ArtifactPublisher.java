/*
	* Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
	* WSO2 Inc. licenses this file to you under the Apache License,
	* Version 2.0 (the "License"); you may not use this file except
	* in compliance with the License.
	* You may obtain a copy of the License at
	* http://www.apache.org/licenses/LICENSE-2.0
	* Unless required by applicable law or agreed to in writing,
	* software distributed under the License is distributed on an
	* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	* KIND, either express or implied. See the License for the
	* specific language governing permissions and limitations
	* under the License.
*/

package org.wso2.store.client;

import com.google.code.commons.cli.annotations.CliParser;
import com.google.code.commons.cli.annotations.ParserException;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import org.apache.commons.cli.BasicParser;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Use as a command line tool to upload artifacts to ES.
 * ES configurations read as command line arguments and if not present
 * set default settings.
 * The artifact details store as JSON format.
 * Reads folder structure under the current directory and upload artifacts store under samples directory
 * physical files related to artifacts also upload.
 */
public class ArtifactPublisher {

    protected static final Logger LOG = Logger.getLogger(ArtifactPublisher.class);

    static CloseableHttpClient httpClient = null;
    static HttpPost httppost = null;
    static HttpGet httpGet = null;
    static CloseableHttpResponse response = null;
    static SSLConnectionSocketFactory sslConnectionSocketFactory = null;
    static HttpContext httpContext = null;

    static Gson gson = null;
    static HashMap<String, List<String>> rxtFileAttributesMap = null;
    static String hostName = "";
    static String port = "";
    static String userName = "";
    static String pwd = "";
    static String sessionId = "";
    static String context = "";
    static String location;

    /**
     * Parse the command line arguments and set ES host settings
     * Login using user credentials
     * Read folder structure under the current directory
     * Fetch rxt types supported by ES
     * Fetch "file" type attributes for each rxt type
     *
     * @param args
     * @throws StoreAssetClientException
     */
    public static void main(String args[]) throws StoreAssetClientException {

        CliParser parser = new CliParser(new BasicParser());
        CliOptions cliOptions = null;

        try {
            cliOptions = parser.parse(CliOptions.class, args);

        } catch (ParserException parseException) {
            String errorMsg = "command line arguments parsing error";
            LOG.error(errorMsg, parseException);
            throw new StoreAssetClientException(errorMsg, parseException);
        }
        hostName = cliOptions.getHostName();
        port = cliOptions.getPort();
        userName = cliOptions.getUserName();
        pwd = cliOptions.getPwd();
        context = cliOptions.getContext();
        location = cliOptions.getLocation();

        File samplesDirectory = new File(location);

        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            sslConnectionSocketFactory = new SSLConnectionSocketFactory(builder.build());

            httpContext = new BasicHttpContext();
            sessionId = getSession(sslConnectionSocketFactory, httpContext, hostName, port, userName, pwd);

        } catch (Exception ex) {
            String errorMsg = "logging session fetching error";
            LOG.error(errorMsg, ex);
            throw new StoreAssetClientException(errorMsg, ex);
        }

        String[] rxtArr = null;
        try {
            rxtArr = getRxtTypes();
        } catch (Exception ex) {
            LOG.error("Error in get rxt types");
            return;
        }
        rxtFileAttributesMap = new HashMap<String, List<String>>();
        for (String rxtType : rxtArr) {
            rxtFileAttributesMap.put(rxtType, getAttributesForType(rxtType, "file"));
        }
        readAssets(samplesDirectory);
    }

    /**
     * Connect to authentication url and get a valid session for
     * valid user credentials.
     *
     * @param sslsf
     * @param httpContext
     * @param hostName
     * @param port
     * @param userName
     * @param pwd
     * @return
     * @throws StoreAssetClientException
     */
    private static String getSession(SSLConnectionSocketFactory sslsf, HttpContext httpContext,
            String hostName, String port, String userName, String pwd)
            throws StoreAssetClientException {

        gson = new Gson();

        StringBuilder urlBuilder = new StringBuilder();
        urlBuilder.append("https://");
        urlBuilder.append(hostName).append(":");
        urlBuilder.append(port).append("/");
        urlBuilder.append(context);
        urlBuilder.append(Constants.PUBLISHER_AUTHORIZATION_URL);
        urlBuilder.append("?username=");
        urlBuilder.append(userName);
        urlBuilder.append("&password=");
        urlBuilder.append(pwd);

        String authUrl = urlBuilder.toString();

        if (LOG.isDebugEnabled()) {
            LOG.debug("log in url:" + authUrl);
        }

        httppost = new HttpPost(authUrl);
        httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

        String sessionId = "";
        response = null;

        try {
            response = httpClient.execute(httppost, httpContext);

        } catch (ClientProtocolException clientProtocolException) {
            String errorMsg = "client protocol exception in login " + authUrl;
            LOG.error(errorMsg, clientProtocolException);
            throw new StoreAssetClientException("client protocol exception in logging" + authUrl,
                    clientProtocolException);
        } catch (IOException ioException) {
            String errorMsg = "io error in login" + authUrl;
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);
        }

        try {
            String responseJson = EntityUtils.toString(response.getEntity());
            Authorize authorize = gson.fromJson(responseJson, Authorize.class);

            if (authorize.getData() != null) {
                sessionId = authorize.getData().getSessionId();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Logged:" + sessionId);
                }
            } else {
                LOG.info("login failure!!!" + responseJson);
            }

        } catch (IOException ioException) {
            String msg = "io error in decode response of login";
            LOG.error(msg, ioException);
            throw new StoreAssetClientException(msg, ioException);

        } finally {
            try {
                httpClient.close();
            } catch (IOException ioEx) {
                LOG.error(ioEx);
            }
        }

        return sessionId;
    }

    /**
     * fetch all rxt types supported by ES
     * ex -: ebook, gadgets
     * @return
     * @throws StoreAssetClientException
     */
    private static String[] getRxtTypes() throws StoreAssetClientException {

        StringBuilder apiUrlbBuilder = new StringBuilder();
        apiUrlbBuilder.append("https://");
        apiUrlbBuilder.append(hostName).append(":");
        apiUrlbBuilder.append(port);
        apiUrlbBuilder.append("/");
        apiUrlbBuilder.append(context);
        apiUrlbBuilder.append(Constants.RXT_URL);

        String apiUrl = apiUrlbBuilder.toString();
        if (LOG.isDebugEnabled()){
            LOG.debug("apiUrl:"+ apiUrl);
        }

        httpGet = new HttpGet(apiUrl);
        httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
        response = null;

        try {
            response = httpClient.execute(httpGet, httpContext);

        } catch (ClientProtocolException clientProtocolException) {
            StringBuilder errorBuilder = new StringBuilder();
            String errorMsg = errorBuilder.append("client protocol exception connecting to RXT API:").append
                    (apiUrl).toString();
            LOG.error(errorMsg, clientProtocolException);
            throw new StoreAssetClientException(errorMsg, clientProtocolException);

        } catch (IOException ioException) {
            StringBuilder errorBuilder = new StringBuilder();
            String errorMsg = errorBuilder.append("error connecting RXT API:").append(apiUrl).toString();
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);
        }

        String responseJson = null;
        String[] arrRxt = null;

        try {
            responseJson = EntityUtils.toString(response.getEntity());
            arrRxt = gson.fromJson(responseJson, String[].class);

        } catch (Exception ioException) {
            StringBuilder errorBuilder = new StringBuilder();
            String errorMsg = errorBuilder.append("io error decoding response:").append(responseJson).toString();
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);

        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error(e);
            }
        }
        return arrRxt;
    }

    /**
     * Fetch given type attributes for given rxt type.
     * ex -: rxt type is gadget and type is "file"
     * returns thumbnail_image and banner_image.
     * @param rxtType
     * @param type
     * @return
     * @throws StoreAssetClientException
     */
    private static List<String> getAttributesForType(String rxtType, String type) throws StoreAssetClientException {

        StringBuilder apiUrlBuilder = new StringBuilder();
        apiUrlBuilder.append("https://").append(hostName).append(":").append(port).append("/").append(context);
        apiUrlBuilder.append(Constants.RXT_ATTRIBUTES_FOR_GIVEN_TYPE).append("/").append(rxtType).append("/").append
                (type);

        String apiUrl =  apiUrlBuilder.toString();
        if (LOG.isDebugEnabled()){
            LOG.debug("apiUrl:"+apiUrl);
        }

        httpGet = new HttpGet(apiUrl);

        CloseableHttpClient httpClient =
                HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();

        response = null;
        gson = new Gson();

        try {
            response = httpClient.execute(httpGet, httpContext);

        } catch (ClientProtocolException clientProtocolException) {
            StringBuilder errorMsgBuilder = new StringBuilder();
            errorMsgBuilder.append("client protocol error in get RXT attributes for rxt type:");
            errorMsgBuilder.append(apiUrl);
            String errorMsg = errorMsgBuilder.toString();
            LOG.error(errorMsg, clientProtocolException);
            throw new StoreAssetClientException(errorMsg, clientProtocolException);

        } catch (IOException ioException) {
            StringBuilder errorMsgBuilder = new StringBuilder();
            errorMsgBuilder.append("io error in get RXT attributes for rxt type:");
            errorMsgBuilder.append(apiUrl);
            String errorMsg = errorMsgBuilder.toString();
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);
        }

        String responseJson = null;
        String[] attrArr = null;

        try {
            responseJson = EntityUtils.toString(response.getEntity());
            attrArr = gson.fromJson(responseJson, String[].class);

        } catch (Exception ioException) {
            StringBuilder errorMsgBuilder = new StringBuilder();
            errorMsgBuilder.append("io error in response json get RXT attributes for rxt type:");
            errorMsgBuilder.append(apiUrl);
            String errorMsg = errorMsgBuilder.toString();
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);

        } finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                LOG.error(e);
            }
        }
        return Arrays.asList(attrArr);

    }

    /**
     * Reads the folder structure under the passed folder.
     * If json file found, call upload assets method.
     *
     * @param dir
     */
    private static void readAssets(File dir) {

        Asset[] assetArr = null;
        BufferedReader br = null;

        for (final File file : dir.listFiles()) {
            if (file.isFile() && file.getName().substring(file.getName().lastIndexOf(".") + 1).equals("json")) {
                try {
                    br = new BufferedReader(new FileReader(file));
                    JsonParser parser = new JsonParser();
                    JsonArray jarray = (JsonArray) parser.parse(br).getAsJsonObject().get("assets");
                    assetArr = gson.fromJson(jarray, Asset[].class);
                    uploadAssets(assetArr, dir);

                } catch (Exception ex) {
                    LOG.error("file not completely uploaded " + file.getName());
                }
            }
            if (file.list() != null && file.list().length > 0) {
                readAssets(file);
            }
        }
    }

    /**
     * upload assets to ES
     * If attribute is a physical file seek a file in a resources directory and upload.
     *
     *
     * @param assetArr
     * @param dir
     */
    private static void uploadAssets(Asset[] assetArr, File dir) {

        HashMap<String, String> attrMap = null;
        MultipartEntityBuilder multiPartBuilder = null;
        List<String> fileAttributes = null;

        File imageFile = null;
        String responseJson = null;
        StringBuffer publisherUrlBuff = null;

        for (Asset asset : assetArr) {
            publisherUrlBuff = new StringBuffer();
            if (asset.getId() != null) {
                publisherUrlBuff.append("https://" + hostName + ":" + port + "/" + context +
                        Constants.PUBLISHER_URL + "/" + asset.getId() + "?type=" +
                        asset.getType());
            } else {
                publisherUrlBuff.append("https://" + hostName + ":" + port + "/" + context +
                        Constants.PUBLISHER_URL + "?type=" + asset.getType());
            }
            multiPartBuilder = MultipartEntityBuilder.create();
            multiPartBuilder.addTextBody("sessionId", sessionId);
            multiPartBuilder.addTextBody("asset", gson.toJson(asset));

            attrMap = asset.getAttributes();

            for (String attrKey : attrMap.keySet()) {
                httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory)
                        .build();
                httppost = new HttpPost(publisherUrlBuff.toString());
                fileAttributes = rxtFileAttributesMap.get(asset.getType());

                if (fileAttributes != null && fileAttributes.contains(attrKey)) {
                    imageFile = new File(
                            dir + File.separator + Constants.RESOURCE_DIR_NAME + File.separator +
                                    attrMap.get(attrKey));
                    multiPartBuilder.addBinaryBody(attrKey, imageFile);
                    httppost.setEntity(multiPartBuilder.build());
                }
            }
            try {
                response = httpClient.execute(httppost, httpContext);
                responseJson = EntityUtils.toString(response.getEntity());
                LOG.info(responseJson.toString());
            } catch (Exception ex) {
                LOG.error(asset);
                LOG.error("error in asset Upload", ex);
            } finally {
                try {
                    httpClient.close();
                } catch (IOException e) {
                }
            }

        }
    }

}
