/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.store.bamclient;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;
import org.wso2.carbon.base.ServerConfiguration;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.agent.thrift.lb.DataPublisherHolder;
import org.wso2.carbon.databridge.agent.thrift.lb.LoadBalancingDataPublisher;
import org.wso2.carbon.databridge.agent.thrift.lb.ReceiverGroup;
import org.wso2.carbon.databridge.agent.thrift.util.DataPublisherUtil;
import org.wso2.carbon.utils.CarbonUtils;
import org.wso2.store.bamclient.common.StoreBAMClientException;
import org.wso2.store.util.Configuration;
import org.wso2.store.util.ConfigurationConstants;
import org.wso2.store.util.StoreConfigurationsException;

import java.io.*;
import java.util.ArrayList;

/**
 * Publish ES events to BAM.
 * Publish data to single or multiple BAM nodes in a cluster in load balanced manner.
 * Multiple BAM nodes can be configure in es-bam.xml.
 * Provides default stream definition.
 * Creates custom stream definition for ES.
 */
public class EventPublisher {

    private static final Logger log = Logger.getLogger(EventPublisher.class);
    private static EventPublisher instance = null;
    private static String assetStatisticsDefaultStream;
    private static LoadBalancingDataPublisher loadBalancingDataPublisher;
    private static final String BAM_CONFIG_DIR = "bam";
    private static final String ES_BAM_CONFIG_FILE_NAME = "es-bam.xml";

    /**
     * Reads BAM nodes configurations from es-bam.xml file stored in ES conf directory.
     * Initialize receiver groups according to receiver urls define in the es-bam.xml.
     * Receiver urls can be configure as comma separated urls.
     * Initialize Load balancing data publisher with receiver urls.
     * @throws StoreBAMClientException
     */
    private EventPublisher() throws StoreBAMClientException {

        InputStream inputStream = null;
        // TODO: This default stream definition should move to a configuration file
        assetStatisticsDefaultStream = "{\"name\":" + ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_NAME +
                "\"version\":" + ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_VERSION + "," +
                "\"nickName\":\"asseteventsStream\",\"description\":\"assets events stream\"" + "," +
                "\"metaData\":[{\"name\":\"clientType\",\"type\":\"STRING\"}]," +
                "\"payloadData\":[{\"name\":\"userstore\",\"type\":\"STRING\"},{\"name\":\"tenant\"," +
                "\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"},{\"name\":\"event\"," +
                "\"type\":\"STRING\"},{\"name\":\"assetId\",\"type\":\"STRING\"},{\"name\":\"assetType\"," +
                "\"type\":\"STRING\"},{\"name\":\"description\",\"type\":\"STRING\"}]}";

        String carbonConfigDirPath = CarbonUtils.getCarbonConfigDirPath();
        Configuration configuration;

        try {
            inputStream = new FileInputStream(new File(carbonConfigDirPath + File.separator + BAM_CONFIG_DIR +
                    File.separator + ES_BAM_CONFIG_FILE_NAME));
            configuration = new Configuration(inputStream);

        } catch (FileNotFoundException fileNotFoundException) {
            String msg = "BAM conf file not found";
            log.error(msg, fileNotFoundException);
            throw new StoreBAMClientException(msg, fileNotFoundException);
        } catch (StoreConfigurationsException storeConfEx) {
            String msg = "BAM configuration error";
            log.error(msg, storeConfEx);
            throw new StoreBAMClientException(msg, storeConfEx);
        } finally {
            try {
                if (inputStream != null) {
                    inputStream.close();
                }
            } catch (IOException e) {
                log.error(e);
            }
        }

        ServerConfiguration serverConfiguration = CarbonUtils.getServerConfiguration();
        String trustStoreFilePath = serverConfiguration.getFirstProperty("Security.TrustStore.Location");
        String trustStorePwd = serverConfiguration.getFirstProperty("Security.TrustStore.Password");

        String receiverUrls = configuration.getFirstProperty(ConfigurationConstants.BAM_HOST);
        Boolean failOver = Boolean.parseBoolean(configuration.getFirstProperty(ConfigurationConstants.BAM_FAILOBER));
        String userName = configuration.getFirstProperty(ConfigurationConstants.BAM_USERNAME);
        String password = configuration.getFirstProperty(ConfigurationConstants.BAM_PWD);

        if (log.isDebugEnabled()) {
            log.debug("Trust store file Path:" + trustStoreFilePath);
        }

        System.setProperty("javax.net.ssl.trustStore", trustStoreFilePath);
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePwd);

        if (log.isDebugEnabled()) {
            log.debug("Receiver urls: " + receiverUrls);
        }

        ArrayList<ReceiverGroup> allReceiverGroups = new ArrayList<ReceiverGroup>();
        ArrayList<String> receiverGroupUrls = DataPublisherUtil.getReceiverGroups(receiverUrls);

        for (String aReceiverGroupURL : receiverGroupUrls) {

            ArrayList<DataPublisherHolder> dataPublisherHolders = new ArrayList<DataPublisherHolder>();
            String[] urls = aReceiverGroupURL.split(",");
            for (String receiverUrl : urls) {
                if (log.isDebugEnabled()) {
                    log.debug("Receiver url:" + receiverUrl);
                }
                //set null to authentication url
                DataPublisherHolder aNode = new DataPublisherHolder(null, receiverUrl.trim(), userName, password);
                dataPublisherHolders.add(aNode);
            }
            ReceiverGroup group = new ReceiverGroup(dataPublisherHolders, failOver);
            allReceiverGroups.add(group);
        }
        loadBalancingDataPublisher = new LoadBalancingDataPublisher(allReceiverGroups);
    }

    public static EventPublisher getInstance() throws StoreBAMClientException {

        if (instance == null) {
            synchronized (EventPublisher.class) {
                if (instance == null) {
                    instance = new EventPublisher();
                }
            }
        }
        return instance;
    }

    public static void shutDownPublisher() {

        if (loadBalancingDataPublisher != null) {
            loadBalancingDataPublisher.stop();
            loadBalancingDataPublisher = null;
        }
    }

    /**
     * publish events to custom stream.
     * if stream doesn't exists create stream with given definition.
     * @param streamName Name of the stream
     * @param streamVersion version of the stream
     * @param streamDefinition Definition of the stream
     * @param metaData Meta data of the stream
     * @param data Data
     * @throws StoreBAMClientException
     */
    public void publishEvents(String streamName, String streamVersion, String streamDefinition,
            String metaData, String data) throws StoreBAMClientException {

        if (log.isDebugEnabled()) {
            log.debug("Stream Name:" + streamName);
            log.debug("Stream Version:" + streamVersion);
            log.debug("Stream Definition:" + streamDefinition);
            log.debug("Meta Data:" + metaData);
            log.debug("Data:" + data);
        }
        if (data !=null && !data.isEmpty()) {
            if (!loadBalancingDataPublisher.isStreamDefinitionAdded(streamName, streamVersion)) {
                loadBalancingDataPublisher.addStreamDefinition(streamDefinition, streamName, streamVersion);
                if (log.isDebugEnabled()) {
                    log.debug("Stream created:" + streamName);
                }
            }
            try {
                loadBalancingDataPublisher.publish(streamName, streamVersion, System.currentTimeMillis(), null,
                        new Object[] { "es" }, data.split(","));
            } catch (AgentException e) {
                log.error("Data publish error", e);
                throw new StoreBAMClientException("Data publish error", e);
            }
        }
    }

    /**
     * This method is use to publish asset statistics to BAM
     * Publish to default stream.
     * @param eventName event name Ex -: add gadget, edit ebook
     * @param tenantId  Tenant Id
     * @param userStore user store name
     * @param username  user name
     * @param assetUUID asset uuid
     * @param assetType asset type
     *                  ex-: gadget
     * @param description Description of the event
     * @throws StoreBAMClientException
     */
    public void publishAssetStatistics(String eventName, String tenantId, String userStore, String username,
            String assetUUID, String assetType, String description)
            throws StoreBAMClientException {

        JsonParser parser = new JsonParser();
        JsonObject streamDefinition = (JsonObject) parser.parse(assetStatisticsDefaultStream);
        String strData = userStore + "," + tenantId + "," + username + "," + eventName + "," + assetUUID + ","
                + assetType + "," + description;
        if (log.isDebugEnabled()) {
            log.debug("asset statistics publish data:" + strData);
        }
        publishEvents(ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_NAME,
                ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_VERSION, assetStatisticsDefaultStream,
                streamDefinition.get("metaData").toString(),
                strData);
    }

}
