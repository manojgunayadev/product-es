/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
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
import org.wso2.store.client.common.StoreAssetClientException;
import org.wso2.store.client.data.Asset;
import org.wso2.store.client.data.AuthenticationData;

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
 * The artifact details read as JSON format.
 * Reads folder structure under the samples directory and upload artifacts.
 * physical files related to artifacts also upload.
 */
public class ArtifactPublisher {

    private static final Logger LOG = Logger.getLogger(ArtifactPublisher.class);

    private static CloseableHttpClient httpClient;
    private static HttpPost httppost;
    private static HttpGet httpGet;
    private static CloseableHttpResponse response;
    private static SSLConnectionSocketFactory sslConnectionSocketFactory;
    private static HttpContext httpContext;

    private static HashMap<String, List<String>> rxtFileAttributesMap;
    private static String hostName;
    private static String port;

    private static String sessionId;
    private static String context;

    private static Gson gson;

    /**
     * Parse the command line arguments and set ES host settings.
     * Login using user credentials given as command line arguments.
     * Get all rxt types supported by ES.
     * Get "file" type attributes for each rxt type.
     * Read folder structure under the current directory.
     * @param args Command Line arguments parameter.
     * @throws StoreAssetClientException
     */
    public static void main(String args[]) throws StoreAssetClientException {

        CliParser parser = new CliParser(new BasicParser());
        CliOptions cliOptions;

        String userName;
        String pwd;
        String location;

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

        if (LOG.isDebugEnabled()) {
            LOG.debug("hostName:" + hostName + " port:" + port + " userName:" + userName + " pwd:" + pwd + " context:"
                    + context + " " +
                    "location:" + location);
        }

        File samplesDirectory = new File(location);

        /**
         * following code block throws various security related exceptions.
         * Catch each and every exception is not useful.Catch generic exception and throw to the caller.
         */
        try {
            SSLContextBuilder builder = new SSLContextBuilder();
            builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
            sslConnectionSocketFactory = new SSLConnectionSocketFactory(builder.build());

        } catch (Exception ex) {
            String errorMsg = "ssl connection builder error";
            LOG.error(errorMsg, ex);
            throw new StoreAssetClientException(errorMsg, ex);
        }

        init();
        sessionId = getSession(sslConnectionSocketFactory, hostName, port, userName, pwd);

        String[] rxtArr = getRxtTypes();
        rxtFileAttributesMap = new HashMap<String, List<String>>();

        for (String rxtType : rxtArr) {
            rxtFileAttributesMap.put(rxtType, getAttributesForType(rxtType, "file"));
        }
        readAssets(samplesDirectory);
    }

    private static void init() {
        httpContext = new BasicHttpContext();
        gson = new Gson();
    }

    /**
     * Connect to authentication url and authenticate user credentials.
     *
     * @param sslsf SSL connection factory
     * @param hostName Host Name
     * @param port port
     * @param userName user name
     * @param pwd password
     * @return session id
     * @throws StoreAssetClientException
     */
    private static String getSession(SSLConnectionSocketFactory sslsf, String hostName, String port, String userName,
            String pwd) throws StoreAssetClientException {

        String authUrl = "https://" + hostName + ":" + port + "/" + context + Constants.PUBLISHER_AUTHORIZATION_URL +
                "?username=" + userName + "&password=" + pwd;

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
            throw new StoreAssetClientException(errorMsg + authUrl, clientProtocolException);
        } catch (IOException ioException) {
            String errorMsg = "io error in login" + authUrl;
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);
        }

        try {
            String responseJson = EntityUtils.toString(response.getEntity());
            AuthenticationData authorizeObj = gson.fromJson(responseJson, AuthenticationData.class);

            if (authorizeObj.getData() != null) {
                sessionId = authorizeObj.getData().getSessionId();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Logged:" + sessionId);
                }
            } else {
                LOG.info("login failure!!!" + responseJson);
            }

            return sessionId;

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
    }

    /**
     * fetch all rxt types supported by ES.
     * ex -: ebook, gadgets.
     * @return String array
     * @throws StoreAssetClientException
     */
    private static String[] getRxtTypes() throws StoreAssetClientException {

        String apiUrl = "https://" + hostName + ":" + port + "/" + context + Constants.RXT_URL;

        if (LOG.isDebugEnabled()) {
            LOG.debug("apiUrl:" + apiUrl);
        }

        httpGet = new HttpGet(apiUrl);
        httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
        response = null;

        try {
            response = httpClient.execute(httpGet, httpContext);

        } catch (ClientProtocolException clientProtocolException) {
            String errorMsg = "client protocol exception connecting to RXT API:" + apiUrl;
            LOG.error(errorMsg, clientProtocolException);
            throw new StoreAssetClientException(errorMsg, clientProtocolException);

        } catch (IOException ioException) {
            String errorMsg = "error connecting RXT API:" + apiUrl;
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);
        }

        String responseJson = null;
        String[] arrRxt = null;

        try {
            responseJson = EntityUtils.toString(response.getEntity());
            arrRxt = gson.fromJson(responseJson, String[].class);

        } catch (Exception ioException) {
            String errorMsg = "io error decoding response:" + responseJson;
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
     * Fetch attributes of given type for given rxt type.
     * ex -: rxt type is gadget and type is "file"
     *       returns thumbnail_image and banner_image.
     * @param rxtType RXT Type
     * @param type Type need to check ex -: file, text
     * @return List of rxt attributes which related to passed type
     * @throws StoreAssetClientException
     */
    private static List<String> getAttributesForType(String rxtType, String type) throws StoreAssetClientException {

        String apiUrl = "https://" + hostName + ":" + port + "/" + context + Constants.RXT_ATTRIBUTES_FOR_GIVEN_TYPE
                + "/" + rxtType + "/" + type;

        if (LOG.isDebugEnabled()) {
            LOG.debug("apiUrl:" + apiUrl);
        }

        httpGet = new HttpGet(apiUrl);
        httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();

        response = null;

        try {
            response = httpClient.execute(httpGet, httpContext);

        } catch (ClientProtocolException clientProtocolException) {
            String errorMsg = "client protocol error in get RXT attributes for rxt type:" + apiUrl;
            LOG.error(errorMsg, clientProtocolException);
            throw new StoreAssetClientException(errorMsg, clientProtocolException);

        } catch (IOException ioException) {
            String errorMsg = "io error in get RXT attributes for rxt type:" + apiUrl;
            LOG.error(errorMsg, ioException);
            throw new StoreAssetClientException(errorMsg, ioException);
        }

        String responseJson;
        String[] attrArr;

        try {
            responseJson = EntityUtils.toString(response.getEntity());
            attrArr = gson.fromJson(responseJson, String[].class);

        } catch (Exception ioException) {
            String errorMsg = "io error in response json get RXT attributes for rxt type:" + apiUrl;
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
     * Reads the folder structure under the resources directory.
     * If json file found, read and unmarshalling asset details into array.
     * File can contains multiple asset details as an array.
     * Call upload asset method.
     *
     * @param dir resources directory
     */
    private static void readAssets(File dir) {

        Asset[] assetArr;
        BufferedReader br;

        for (final File file : dir.listFiles()) {
            if (file.isFile()) {
                if (file.getName().substring(file.getName().lastIndexOf(".") + 1).equals("json")) {
                    /**
                     * catch generic exception. if a resource file fails to extract asset details
                     * due to incorrect JSON format or any other error continue the upload of other assets.
                     */
                    try {
                        br = new BufferedReader(new FileReader(file));
                        JsonParser parser = new JsonParser();
                        JsonArray jsonArray = (JsonArray) parser.parse(br).getAsJsonObject().get("assets");
                        assetArr = gson.fromJson(jsonArray, Asset[].class);
                        uploadAssets(assetArr, dir);

                    } catch (Exception ex) {
                        LOG.error("file not completely uploaded " + file.getName());
                    }
                }
            }
            if (file.list() != null && file.list().length > 0) {
                readAssets(file);
            }
        }
    }

    /**
     * upload assets to ES
     * POST asset details to asset upload REST API
     * If attribute is a physical file seek a file in a resources directory and upload as multipart attachment.
     *
     *
     * @param assetArr Array of assets
     * @param dir resource files directory
     */
    private static void uploadAssets(Asset[] assetArr, File dir) {

        HashMap<String, String> attrMap;
        MultipartEntityBuilder multiPartBuilder;
        List<String> fileAttributes;

        File imageFile;
        String responseJson;
        StringBuilder publisherUrlBuilder;

        String uploadUrl = "https://" + hostName + ":" + port + "/" + context + Constants.PUBLISHER_URL + "/";

        for (Asset asset : assetArr) {
            publisherUrlBuilder = new StringBuilder();
            if (asset.getId() != null) {
                publisherUrlBuilder.append(uploadUrl).append(asset.getId()).append("?type=").append(asset.getType());
            } else {
                publisherUrlBuilder.append(uploadUrl).append("?type=").append(asset.getType());
            }
            multiPartBuilder = MultipartEntityBuilder.create();
            multiPartBuilder.addTextBody("sessionId", sessionId);
            multiPartBuilder.addTextBody("asset", gson.toJson(asset));

            attrMap = asset.getAttributes();
            httppost = new HttpPost(publisherUrlBuilder.toString());
            httpClient = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();

            for (String attrKey : attrMap.keySet()) {
                fileAttributes = rxtFileAttributesMap.get(asset.getType());
                if (fileAttributes != null && fileAttributes.contains(attrKey)) {
                    imageFile = new File(dir + File.separator + Constants.RESOURCE_DIR_NAME + File.separator +
                            attrMap.get(attrKey));
                    multiPartBuilder.addBinaryBody(attrKey, imageFile);
                }
            }
            httppost.setEntity(multiPartBuilder.build());
            /**
             * catch generic exception.
             * exceptions can be thrown by httpclient and json reading due to wrong json format of artifacts or ES
             * host down.
             * Not useful to throw those errors to caller. Log the error.
             */
            try {
                response = httpClient.execute(httppost, httpContext);
                responseJson = EntityUtils.toString(response.getEntity());
                LOG.info(responseJson);
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
