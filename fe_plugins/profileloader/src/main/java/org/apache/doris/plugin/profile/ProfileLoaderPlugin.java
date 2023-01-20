// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.plugin.profile;

import com.google.common.collect.Queues;
import org.apache.doris.plugin.Plugin;
import org.apache.doris.plugin.PluginContext;
import org.apache.doris.plugin.PluginException;
import org.apache.doris.plugin.PluginInfo;
import org.apache.doris.plugin.ProfileEvent;
import org.apache.doris.plugin.ProfilePlugin;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/*
 * This plugin will load profile log to specified doris table at specified interval
 */
public class ProfileLoaderPlugin extends Plugin implements ProfilePlugin {
    private final static Logger LOG = LogManager.getLogger(ProfileLoaderPlugin.class);

    private static final ThreadLocal<SimpleDateFormat> dateFormatContainer = ThreadLocal.withInitial(
        () -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

    private StringBuilder profileLogBuffer = new StringBuilder();
    private long lastLoadTimeProfileLog = 0;

    private BlockingQueue<ProfileEvent> profileEventQueue;
    private DorisStreamLoader streamLoader;
    private Thread loadThread;

    private ProfileLoaderConf conf;
    private volatile boolean isClosed = false;
    private volatile boolean isInit = false;

    @Override
    public void init(PluginInfo info, PluginContext ctx) throws PluginException {
        super.init(info, ctx);

        synchronized (this) {
            if (isInit) {
                return;
            }
            this.lastLoadTimeProfileLog = System.currentTimeMillis();

            loadConfig(ctx, info.getProperties());

            this.profileEventQueue = Queues.newLinkedBlockingDeque(conf.maxQueueSize);
            this.streamLoader = new DorisStreamLoader(conf);
            this.loadThread = new Thread(new LoadWorker(this.streamLoader), "profile loader thread");
            this.loadThread.start();

            isInit = true;
        }
    }

    private void loadConfig(PluginContext ctx, Map<String, String> pluginInfoProperties) throws PluginException {
        Path pluginPath = FileSystems.getDefault().getPath(ctx.getPluginPath());
        if (!Files.exists(pluginPath)) {
            throw new PluginException("plugin path does not exist: " + pluginPath);
        }

        Path confFile = pluginPath.resolve("plugin.conf");
        if (!Files.exists(confFile)) {
            throw new PluginException("plugin conf file does not exist: " + confFile);
        }

        final Properties props = new Properties();
        try (InputStream stream = Files.newInputStream(confFile)) {
            props.load(stream);
        } catch (IOException e) {
            throw new PluginException(e.getMessage());
        }

        for (Map.Entry<String, String> entry : pluginInfoProperties.entrySet()) {
            props.setProperty(entry.getKey(), entry.getValue());
        }

        final Map<String, String> properties = props.stringPropertyNames().stream()
            .collect(Collectors.toMap(Function.identity(), props::getProperty));
        conf = new ProfileLoaderConf();
        conf.init(properties);
        conf.feIdentity = ctx.getFeIdentity();
    }

    @Override
    public void close() throws IOException {
        super.close();
        isClosed = true;
        if (loadThread != null) {
            try {
                loadThread.join();
            } catch (InterruptedException e) {
                LOG.debug("encounter exception when closing the audit loader", e);
            }
        }
    }

    public void exec(ProfileEvent event) {
        try {
            profileEventQueue.add(event);
        } catch (Exception e) {
            // In order to ensure that the system can run normally, here we directly
            // discard the current audit_event. If this problem occurs frequently,
            // improvement can be considered.
            LOG.debug("encounter exception when putting current audit batch, discard current audit event", e);
        }
    }

    private void fillLogBuffer(ProfileEvent event, StringBuilder logBuffer) {
        logBuffer.append(event.jobId).append("\t");
        logBuffer.append(event.queryId).append("\t");
        logBuffer.append(event.user).append("\t");
        logBuffer.append(event.defaultDb).append("\t");
        logBuffer.append(event.queryType).append("\t");
        logBuffer.append(event.startTime).append("\t");
        logBuffer.append(event.endTime).append("\t");
        logBuffer.append(event.totalTime).append("\t");
        logBuffer.append(event.queryState).append("\t");
        logBuffer.append(event.traceId).append("\t");
        // trim the stmt to avoid too long
        // use `getBytes().length` to get real byte length
        String stmt = truncateByBytes(event.stmt, conf.max_stmt_length).replace("\t", " ");
        logBuffer.append(stmt).append("\t");
        // trim the profile to avoid too long
        // use `getBytes().length` to get real byte length
        String profile = truncateByBytes(event.profile, conf.max_profile_length).replace("\t", " ");
        byte[] delimiter = new byte[1];
        delimiter[0] = 0x01;
        logBuffer.append(profile).append(new String(delimiter));
    }

    private String truncateByBytes(String str, int length) {
        int maxLen = Math.min(length, str.getBytes().length);
        if (maxLen >= str.getBytes().length) {
            return str;
        }
        Charset utf8Charset = Charset.forName("UTF-8");
        CharsetDecoder decoder = utf8Charset.newDecoder();
        byte[] sb = str.getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(sb, 0, maxLen);
        CharBuffer charBuffer = CharBuffer.allocate(maxLen);
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        decoder.decode(buffer, charBuffer, true);
        decoder.flush(charBuffer);
        return new String(charBuffer.array(), 0, charBuffer.position());
    }

    private void loadIfNecessary(DorisStreamLoader loader) {
        StringBuilder logBuffer = profileLogBuffer;
        long lastLoadTime = lastLoadTimeProfileLog;
        long currentTime = System.currentTimeMillis();

        if (logBuffer.length() >= conf.maxBatchSize || currentTime - lastLoadTime >= conf.maxBatchIntervalSec * 1000) {
            // begin to load
            try {
                DorisStreamLoader.LoadResponse response = loader.loadBatch(logBuffer.toString().getBytes());
                LOG.info("profile loader response: {}", response);
            } catch (Exception e) {
                LOG.warn("encounter exception when putting current profile batch, discard current batch", e);
            } finally {
                // make a new string builder to receive following events.
                resetLogBufferAndLastLoadTime(currentTime);
            }
        }
    }

    private void resetLogBufferAndLastLoadTime(long currentTime) {
        this.profileLogBuffer = new StringBuilder();
        lastLoadTimeProfileLog = currentTime;
    }

    public static class ProfileLoaderConf {
        public static final String PROP_MAX_BATCH_SIZE = "max_batch_size";
        public static final String PROP_MAX_BATCH_INTERVAL_SEC = "max_batch_interval_sec";
        public static final String PROP_MAX_QUEUE_SIZE = "max_queue_size";
        public static final String PROP_FRONTEND_HOST_PORT = "frontend_host_port";
        public static final String PROP_USER = "user";
        public static final String PROP_PASSWORD = "password";
        public static final String PROP_DATABASE = "database";
        public static final String PROP_PROFILE_LOG_TABLE = "profile_log_table";
        // the max stmt and profile length to be loaded in profile table.
        public static final String MAX_STMT_LENGTH = "max_stmt_length";
        public static final String MAX_PROFILE_LENGTH = "max_profile_length";

        public long maxBatchSize = 50 * 1024 * 1024;
        public long maxBatchIntervalSec = 60;
        public int maxQueueSize = 1000;
        public String frontendHostPort = "127.0.0.1:8030";
        public String user = "root";
        public String password = "";
        public String database = "doris_audit_db__";
        public String profileLogTable = "doris_profile_log_tbl__";
        // the identity of FE which run this plugin
        public String feIdentity = "";
        public int max_stmt_length = 4096;
        public int max_profile_length = 81920;

        public void init(Map<String, String> properties) throws PluginException {
            try {
                if (properties.containsKey(PROP_MAX_BATCH_SIZE)) {
                    maxBatchSize = Long.valueOf(properties.get(PROP_MAX_BATCH_SIZE));
                }
                if (properties.containsKey(PROP_MAX_BATCH_INTERVAL_SEC)) {
                    maxBatchIntervalSec = Long.valueOf(properties.get(PROP_MAX_BATCH_INTERVAL_SEC));
                }
                if (properties.containsKey(PROP_MAX_QUEUE_SIZE)) {
                    maxQueueSize = Integer.valueOf(properties.get(PROP_MAX_QUEUE_SIZE));
                }
                if (properties.containsKey(PROP_FRONTEND_HOST_PORT)) {
                    frontendHostPort = properties.get(PROP_FRONTEND_HOST_PORT);
                }
                if (properties.containsKey(PROP_USER)) {
                    user = properties.get(PROP_USER);
                }
                if (properties.containsKey(PROP_PASSWORD)) {
                    password = properties.get(PROP_PASSWORD);
                }
                if (properties.containsKey(PROP_DATABASE)) {
                    database = properties.get(PROP_DATABASE);
                }
                if (properties.containsKey(PROP_PROFILE_LOG_TABLE)) {
                    profileLogTable = properties.get(PROP_PROFILE_LOG_TABLE);
                }
                if (properties.containsKey(MAX_STMT_LENGTH)) {
                    max_stmt_length = Integer.parseInt(properties.get(MAX_STMT_LENGTH));
                }
                if (properties.containsKey(MAX_PROFILE_LENGTH)) {
                    max_profile_length = Integer.parseInt(properties.get(MAX_PROFILE_LENGTH));
                }
            } catch (Exception e) {
                throw new PluginException(e.getMessage());
            }
        }
    }

    private class LoadWorker implements Runnable {
        private DorisStreamLoader loader;

        public LoadWorker(DorisStreamLoader loader) {
            this.loader = loader;
        }

        public void run() {
            while (!isClosed) {
                try {
                    ProfileEvent event = profileEventQueue.poll(5, TimeUnit.SECONDS);
                    if (event != null) {
                        fillLogBuffer(event, profileLogBuffer);
                        loadIfNecessary(loader);
                    }
                } catch (InterruptedException ie) {
                    LOG.debug("encounter exception when loading current profile batch", ie);
                } catch (Exception e) {
                    LOG.error("LoadWorker run failed for profile loader, error:", e);
                }
            }
        }
    }

}
