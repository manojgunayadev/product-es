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

package org.wso2.store.bamclient;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.log4j.Logger;
import org.wso2.carbon.databridge.agent.thrift.exception.AgentException;
import org.wso2.carbon.databridge.agent.thrift.lb.DataPublisherHolder;
import org.wso2.carbon.databridge.agent.thrift.lb.LoadBalancingDataPublisher;
import org.wso2.carbon.databridge.agent.thrift.lb.ReceiverGroup;
import org.wso2.carbon.databridge.agent.thrift.util.DataPublisherUtil;
import org.wso2.store.bamclient.common.StoreBAMClientException;
import org.wso2.store.bamclient.usage.ESBamPublisherUsageConstants;
import org.wso2.store.util.Configuration;
import org.wso2.store.util.ConfigurationConstants;
import org.wso2.store.util.StoreConfigurationsException;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Publish ES events to BAM.
 * Maintain single instance for ES instance.
 * The class provides default stream definition or
 * Client can send its own stream definition, meta data and data to publish.
 */
public class EventPublisher {

    private static final Logger LOG = Logger.getLogger(EventPublisher.class);
    private static EventPublisher instance = null;
    private StringBuilder str = null;
    private static String assetStatisticsDefaultStream = null;
    private static LoadBalancingDataPublisher loadBalancingDataPublisher = null;

    private EventPublisher() throws StoreBAMClientException {

        InputStream inputStream = null;

        str = new StringBuilder();
        str.append("{\"name\":").append(ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_NAME).append("\"version\":");
        str.append(ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_VERSION);
        str.append(",\"nickName\":\"asseteventsStream\",\"description\":\"assets events stream\"");
        str.append(",\"metaData\":[{\"name\":\"clientType\",\"type\":\"STRING\"}],");
        str.append("\"payloadData\":[{\"name\":\"userstore\",\"type\":\"STRING\"},{\"name\":\"tenant\",");
        str.append("\"type\":\"STRING\"},{\"name\":\"user\",\"type\":\"STRING\"},{\"name\":\"event\",");
        str.append("\"type\":\"STRING\"},{\"name\":\"assetId\",\"type\":\"STRING\"},{\"name\":\"assetType\",");
        str.append("\"type\":\"STRING\"},{\"name\":\"description\",\"type\":\"STRING\"}]}");

        assetStatisticsDefaultStream = str.toString();

        try {
            inputStream = new FileInputStream(new File(System.getProperty("carbon.home") + File.separator
                    + "repository" + File.separator + "conf" + File.separator + "bam" +
                    File.separator + "es-bam.xml"));

        } catch (FileNotFoundException fileNotFoundException) {
            String msg = "bam conf file not found";
            LOG.error(msg, fileNotFoundException);
            throw new StoreBAMClientException(msg, fileNotFoundException);
        }

        Configuration configuration = null;

        try {
            configuration = new Configuration(inputStream);

        } catch (StoreConfigurationsException storeConfEx) {
            String msg = "Bam configuration error";
            LOG.error(msg, storeConfEx);
            throw new StoreBAMClientException(msg, storeConfEx);
        }

        String trustStoreFilePath =
                System.getProperty("carbon.home") + File.separator + "repository" + File.separator + "resources" +
                        File.separator + "security" +
                        File.separator + "client-truststore.jks";

        String trustStorePwd = configuration.getFirstProperty(ConfigurationConstants.BAM_TRUST_STORE_PWD);
        String receiverUrls = configuration.getFirstProperty(ConfigurationConstants.BAM_HOST);
        Boolean failOver = Boolean.parseBoolean(configuration.getFirstProperty(ConfigurationConstants.BAM_FAILOBER));

        String userName = configuration.getFirstProperty(ConfigurationConstants.BAM_USERNAME);
        String password = configuration.getFirstProperty(ConfigurationConstants.BAM_PWD);

        if (LOG.isDebugEnabled()) {
            LOG.debug("trustStoreFilePath:" + trustStoreFilePath);
        }

        System.setProperty("javax.net.ssl.trustStore", trustStoreFilePath);
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePwd);

        if (LOG.isDebugEnabled()) {
            LOG.debug("receiverUrls" + receiverUrls);
        }

        ArrayList<ReceiverGroup> allReceiverGroups = new ArrayList<ReceiverGroup>();
        ArrayList<String> receiverGroupUrls = DataPublisherUtil.getReceiverGroups(receiverUrls);

        for (String aReceiverGroupURL : receiverGroupUrls) {
            ArrayList<DataPublisherHolder> dataPublisherHolders = new ArrayList<DataPublisherHolder>();
            String[] urls = aReceiverGroupURL.split(",");

            for (String aUrl : urls) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("aUrl:" + aUrl);
                }
                DataPublisherHolder aNode = new DataPublisherHolder(null, aUrl.trim(), userName, password);
                dataPublisherHolders.add(aNode);
            }
            ReceiverGroup group = new ReceiverGroup(dataPublisherHolders, failOver);
            allReceiverGroups.add(group);
        }
        loadBalancingDataPublisher = new LoadBalancingDataPublisher(allReceiverGroups);
    }

    public static EventPublisher getInstance() throws StoreBAMClientException {

        synchronized (EventPublisher.class) {
            if (instance == null) {
                instance = new EventPublisher();
            }
        }
        return instance;
    }

    public static void shutDownPublisher() {

        if (loadBalancingDataPublisher != null) {
            loadBalancingDataPublisher.stop();
        }
    }

    /**
     * publish events to passed stream.
     * if stream doesn't exists create stream with given definition.
     * This is a generic method use to publish any events
     * The metadata and data accepts
     *
     * @param streamName
     * @param streamVersion
     * @param streamDefinition
     * @param metaData
     * @param data
     * @throws StoreBAMClientException
     */
    public void publishEvents(String streamName, String streamVersion, String streamDefinition,
            String metaData, String data) throws StoreBAMClientException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("streamName:" + streamName);
            LOG.debug("streamVersion:" + streamVersion);
            LOG.debug("streamDefinition:" + streamDefinition);
            LOG.debug(metaData);
            LOG.debug(data);
        }
        if (!loadBalancingDataPublisher.isStreamDefinitionAdded(streamName, streamVersion)) {
            loadBalancingDataPublisher.addStreamDefinition(streamDefinition, streamName, streamVersion);
            if (LOG.isDebugEnabled()) {
                LOG.debug("stream created:");
            }
        }
        try {
            loadBalancingDataPublisher.publish(streamName, streamVersion, System.currentTimeMillis(), null,
                    new Object[] { "es" }, data.split(","));
        } catch (AgentException e) {
            LOG.error("data publish error", e);
            throw new StoreBAMClientException("data publish error", e);
        }

    }

    /**
     * This method is use to publish asset statistics to BAM
     * stream name, stream definition accepts as parameters.
     *
     * @param eventName
     * @param tenantId
     * @param userStore
     * @param username
     * @param assetUDID
     * @param assetType
     * @param description
     * @throws StoreBAMClientException
     */
    public void publishAssetStatistics(String eventName, String tenantId, String userStore, String username,
            String assetUDID, String assetType, String description)
            throws StoreBAMClientException {

        JsonObject streamDefinition = (JsonObject) new JsonParser().parse(assetStatisticsDefaultStream);

        StringBuilder strDataBuilder = new StringBuilder();

        strDataBuilder.append(userStore).append(",");
        strDataBuilder.append(tenantId).append(",");
        strDataBuilder.append(username).append(",");
        strDataBuilder.append(eventName).append(",");
        strDataBuilder.append(assetUDID).append(",");
        strDataBuilder.append(assetType).append(",");
        strDataBuilder.append(description);

        String strData = strDataBuilder.toString();

        if (LOG.isDebugEnabled()) {
            LOG.debug("asset statistics publish data:" + strData);
        }

        publishEvents(ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_NAME,
                ESBamPublisherUsageConstants.ES_STATISTICS_STREAM_VERSION,
                assetStatisticsDefaultStream, streamDefinition.get("metaData").toString(),
                strData);
    }

}
