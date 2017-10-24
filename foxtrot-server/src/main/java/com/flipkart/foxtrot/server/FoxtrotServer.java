/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.filter.ThresholdFilter;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.FixedWindowRollingPolicy;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.SizeBasedTriggeringPolicy;
import ch.qos.logback.core.spi.FilterAttachable;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.SubtypeResolver;
import com.fasterxml.jackson.databind.jsontype.impl.StdSubtypeResolver;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.common.DataDeletionManager;
import com.flipkart.foxtrot.core.common.DataDeletionManagerConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseDataStore;
import com.flipkart.foxtrot.core.datastore.impl.hbase.HbaseTableConnection;
import com.flipkart.foxtrot.core.querystore.DocumentTranslator;
import com.flipkart.foxtrot.core.querystore.QueryExecutor;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.DistributedTableMetadataManager;
import com.flipkart.foxtrot.core.table.impl.FoxtrotTableManager;
import com.flipkart.foxtrot.server.cluster.ClusterManager;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.healthcheck.ElasticSearchHealthCheck;
import com.flipkart.foxtrot.server.providers.FlatResponseCsvProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseErrorTextProvider;
import com.flipkart.foxtrot.server.providers.FlatResponseTextProvider;
import com.flipkart.foxtrot.server.providers.exception.FoxtrotExceptionMapper;
import com.flipkart.foxtrot.server.resources.*;
import com.flipkart.foxtrot.sql.FqlEngine;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.assets.AssetsBundle;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;
import com.yammer.dropwizard.config.LoggingConfiguration;
import com.yammer.dropwizard.logging.AsyncAppender;
import com.yammer.dropwizard.logging.LogFormatter;
import com.yammer.metrics.core.HealthCheck;
import net.sourceforge.cobertura.CoverageIgnore;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * User: Santanu Sinha (santanu.sinha@flipkart.com)
 * Date: 15/03/14
 * Time: 9:38 PM
 */

@CoverageIgnore
public class FoxtrotServer extends Service<FoxtrotServerConfiguration> {
    @Override
    public void initialize(Bootstrap<FoxtrotServerConfiguration> bootstrap) {
        bootstrap.setName("foxtrot");
        bootstrap.addBundle(new AssetsBundle("/console/", "/"));
        bootstrap.addCommand(new InitializerCommand());
    }

    @Override
    public void run(FoxtrotServerConfiguration configuration, Environment environment) throws Exception {
        configuration.getHttpConfiguration().setRootPath("/foxtrot/*");
        configureObjectMapper(environment);

        Logger root =  (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        LoggingConfiguration loggingConfiguration  = configuration.getLoggingConfiguration();
        LoggingConfiguration.FileConfiguration file = loggingConfiguration.getFileConfiguration();
        root.addAppender(AsyncAppender.wrap(setupLogging(file, root.getLoggerContext(), file.getLogFormat())));

        ObjectMapper objectMapper = environment.getObjectMapperFactory().build();
        ExecutorService executorService = environment.managedExecutorService("query-executor-%s", 20, 40, 30, TimeUnit.SECONDS);

        HbaseTableConnection HBaseTableConnection = new HbaseTableConnection(configuration.getHbase());
        ElasticsearchConnection elasticsearchConnection = new ElasticsearchConnection(configuration.getElasticsearch());
        HazelcastConnection hazelcastConnection = new HazelcastConnection(configuration.getCluster());
        ElasticsearchUtils.setTableNamePrefix(configuration.getElasticsearch());

        TableMetadataManager tableMetadataManager = new DistributedTableMetadataManager(hazelcastConnection, elasticsearchConnection);
        DataStore dataStore = new HBaseDataStore(HBaseTableConnection,
                objectMapper, new DocumentTranslator(configuration.getHbase()));
        QueryStore queryStore = new ElasticsearchQueryStore(tableMetadataManager, elasticsearchConnection, dataStore, objectMapper);
        FoxtrotTableManager tableManager = new FoxtrotTableManager(tableMetadataManager, queryStore, dataStore);
        CacheManager cacheManager = new CacheManager(new DistributedCacheFactory(hazelcastConnection, objectMapper));
        AnalyticsLoader analyticsLoader = new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager, objectMapper);
        QueryExecutor executor = new QueryExecutor(analyticsLoader, executorService);
        DataDeletionManagerConfig dataDeletionManagerConfig = configuration.getTableDataManagerConfig();
        DataDeletionManager dataDeletionManager = new DataDeletionManager(dataDeletionManagerConfig, queryStore);

        List<HealthCheck> healthChecks = new ArrayList<>();
        healthChecks.add(new ElasticSearchHealthCheck("ES Health Check", elasticsearchConnection));
        ClusterManager clusterManager = new ClusterManager(hazelcastConnection, healthChecks, configuration.getHttpConfiguration());

        environment.manage(HBaseTableConnection);
        environment.manage(elasticsearchConnection);
        environment.manage(hazelcastConnection);
        environment.manage(tableMetadataManager);
        environment.manage(analyticsLoader);
        environment.manage(dataDeletionManager);
        environment.manage(clusterManager);

        environment.addResource(new DocumentResource(queryStore));
        environment.addResource(new AsyncResource(cacheManager));
        environment.addResource(new AnalyticsResource(executor));
        environment.addResource(new TableManagerResource(tableManager));
        environment.addResource(new TableFieldMappingResource(queryStore));
        environment.addResource(new ConsoleResource(
                new ElasticsearchConsolePersistence(elasticsearchConnection, objectMapper)));
        FqlEngine fqlEngine = new FqlEngine(tableMetadataManager, queryStore, executor, objectMapper);
        environment.addResource(new FqlResource(fqlEngine));
        environment.addResource(new ClusterInfoResource(clusterManager));
        environment.addResource(new UtilResource(configuration));
        environment.addResource(new ClusterHealthResource(queryStore));
        healthChecks.forEach(environment::addHealthCheck);

        environment.addProvider(new FoxtrotExceptionMapper());
        environment.addProvider(new FlatResponseTextProvider());
        environment.addProvider(new FlatResponseCsvProvider());
        environment.addProvider(new FlatResponseErrorTextProvider());

        environment.addFilter(CrossOriginFilter.class, "/*");
    }

    private void configureObjectMapper(Environment environment) {
        environment.getObjectMapperFactory().setSerializationInclusion(JsonInclude.Include.NON_NULL);
        environment.getObjectMapperFactory().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        SubtypeResolver subtypeResolver = new StdSubtypeResolver();
        environment.getObjectMapperFactory().setSubtypeResolver(subtypeResolver);
    }

    private FileAppender<ILoggingEvent> setupLogging(LoggingConfiguration.FileConfiguration file,
                                                     LoggerContext context,
                                                     com.google.common.base.Optional<String> logFormat) {

        final LogFormatter formatter = new LogFormatter(context, file.getTimeZone());

        for (String format : logFormat.asSet()) {
            formatter.setPattern(format);
        }
        formatter.start();

        final FileAppender<ILoggingEvent> appender =
                file.isArchive() ? new RollingFileAppender<>() :
                        new FileAppender<ILoggingEvent>();

        appender.setAppend(true);
        appender.setContext(context);
        appender.setLayout(formatter);
        appender.setFile(file.getCurrentLogFilename());
        appender.setPrudent(false);

        addThresholdFilter(appender, file.getThreshold());

        if (file.isArchive()) {

            SizeBasedTriggeringPolicy triggeringPolicy = new SizeBasedTriggeringPolicy("100MB");

            final FixedWindowRollingPolicy windowRollingPolicy = new FixedWindowRollingPolicy();
            windowRollingPolicy.setMinIndex(0);
            windowRollingPolicy.setMaxIndex(file.getArchivedFileCount());
            String filePattern = file.getArchivedLogFilenamePattern().replaceAll("-%d\\{.*\\}","");
            windowRollingPolicy.setFileNamePattern(filePattern);
            windowRollingPolicy.setContext(context);

            ((RollingFileAppender<ILoggingEvent>) appender).setRollingPolicy(windowRollingPolicy);
            ((RollingFileAppender<ILoggingEvent>) appender).setTriggeringPolicy(triggeringPolicy);

            windowRollingPolicy.setParent(appender);
            windowRollingPolicy.start();
        }

        appender.stop();
        appender.start();

        return appender;

    }

    private static void addThresholdFilter(FilterAttachable<ILoggingEvent> appender, Level threshold) {
        final ThresholdFilter filter = new ThresholdFilter();
        filter.setLevel(threshold.toString());
        filter.start();
        appender.addFilter(filter);
    }

}
