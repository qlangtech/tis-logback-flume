/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.flume;

import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.node.AbstractConfigurationProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import com.qlangtech.tis.config.BasicConfig;
import com.qlangtech.tis.manage.common.PropertyPlaceholderHelper;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2016年4月18日
 */
public class TisPropertiesFileConfigurationProvider extends AbstractConfigurationProvider {

    /**
     *
     */
    public TisPropertiesFileConfigurationProvider(String agentName) {
        super(agentName);
    }

    @Override
    protected FlumeConfiguration getFlumeConfiguration() {
        InputStream reader = null;
        try {
            reader = getConfigResource();
            Properties properties = new Properties();
            properties.load(reader);
            return new FlumeConfiguration(toMap(properties));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ex) {
                }
            }
        }
    }

    protected InputStream getConfigResource() throws Exception {
        return this.getClass().getResourceAsStream("/flume.properties");
    }

    @Override
    protected Map<String, String> toMap(Properties properties) {
        PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");
        final BasicConfig config = BasicConfig.getBaseConfig();
        PropertyPlaceholderHelper.PlaceholderResolver resolver = new PropertyPlaceholderHelper.PlaceholderResolver() {
            @Override
            public String resolvePlaceholder(String placeholderName) {
                return config.getAllKV().get(placeholderName);
            }
        };
        Map<String, String> result = super.toMap(properties);
        return result.entrySet().stream().collect(
                Collectors.toMap((e) -> e.getKey(), (e) -> helper.replacePlaceholders(e.getValue(), resolver)));
    }
}
