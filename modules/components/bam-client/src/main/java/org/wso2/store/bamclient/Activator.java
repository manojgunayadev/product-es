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

import org.apache.log4j.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;

/**
 *  BAM client OSGI Bundle activator.
 *  The stop method calls shut down method and kills all connections to BAM.
 */
public class Activator implements BundleActivator {

	private static final Logger log = Logger.getLogger(EventPublisher.class);

	@Override
	public void start(BundleContext bundleContext) throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("Start BAM client bundle");
        }
	}

	@Override
	public void stop(BundleContext bundleContext) {
        if (log.isDebugEnabled()) {
            log.debug("BAM client bundle shut down");
        }
		EventPublisher.shutDownPublisher();
	}
}
