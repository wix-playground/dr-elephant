/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.analysis.HeuristicResult;
import com.linkedin.drelephant.configurations.heuristic.HeuristicConfigurationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceApplicationData;
import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.Properties;


/**
 * This Heuristic analyses the skewness in the mapper input data
 */
public class MapperSkewHeuristic extends GenericSkewHeuristic {
    private static final Logger logger = Logger.getLogger(MapperSkewHeuristic.class);

    public MapperSkewHeuristic(HeuristicConfigurationData heuristicConfData) {
        super(Arrays.asList(
                MapReduceCounterData.CounterName.HDFS_BYTES_READ,
                MapReduceCounterData.CounterName.S3_BYTES_READ,
                MapReduceCounterData.CounterName.S3A_BYTES_READ,
                MapReduceCounterData.CounterName.S3N_BYTES_READ
        ), heuristicConfData);
    }

    @Override
    protected MapReduceTaskData[] getTasks(MapReduceApplicationData data) {
        return data.getMapperData();
    }

    @Override
    public HeuristicResult apply(MapReduceApplicationData data) {
        HeuristicResult result = super.apply(data);
        if (result != null) {
            String appId = data.getAppId();
            String jobId = data.getJobId();
            if (jobId != null && !jobId.isEmpty()) {
                Properties conf = data.getConf();
                try {
                    String jobName = "default-job-name";
                    String user = "no-user-found";
                    if (conf != null) {
                        jobName = URLEncoder.encode(conf.getProperty("mapreduce.job.name", "default-job-name"), "ASCII");
                        user = conf.getProperty("mapreduce.job.user.name", "no-user-found");
                    }
                    MapReduceTaskData[] mappers = data.getMapperData();
                    MapReduceTaskData[] reducers = data.getReducerData();

                    long bytesReadHdfs = 0;
                    long bytesReadS3 = 0;
                    if (mappers != null) {
                        for (MapReduceTaskData mapper : mappers) {
                            if (mapper.isCounterDataPresent()) {
                                MapReduceCounterData counters = mapper.getCounters();
                                bytesReadHdfs += counters.get(MapReduceCounterData.CounterName.HDFS_BYTES_READ);
                                bytesReadS3 += counters.get(MapReduceCounterData.CounterName.S3_BYTES_READ);
                                bytesReadS3 += counters.get(MapReduceCounterData.CounterName.S3A_BYTES_READ);
                                bytesReadS3 += counters.get(MapReduceCounterData.CounterName.S3N_BYTES_READ);
                            }
                        }
                    }

                    long bytesWrittenHdfs = 0;
                    long bytesWrittenS3 = 0;
                    if (reducers != null) {
                        for (MapReduceTaskData reducer : reducers) {
                            if (reducer.isCounterDataPresent()) {
                                MapReduceCounterData counters = reducer.getCounters();
                                bytesWrittenHdfs += counters.get(MapReduceCounterData.CounterName.HDFS_BYTES_WRITTEN);
                                bytesWrittenS3 += counters.get(MapReduceCounterData.CounterName.S3_BYTES_WRITTEN);
                                bytesWrittenS3 += counters.get(MapReduceCounterData.CounterName.S3A_BYTES_WRITTEN);
                                bytesWrittenS3 += counters.get(MapReduceCounterData.CounterName.S3N_BYTES_WRITTEN);
                            }
                        }
                    }


                    HttpClient client = HttpClientBuilder.create().build();
                    HttpGet get = new HttpGet("http://frog.wix.com/quix?src=11&evid=2025" +
                            "&application_id=" + appId +
                            "&bytesReadHDFS=" + bytesReadHdfs +
                            "&bytesReadS3=" + bytesReadS3 +
                            "&bytesWrittenHDFS=" + bytesWrittenHdfs +
                            "&bytesWrittenS3=" + bytesWrittenS3 +
                            "&jobId=" + jobId +
                            "&jobName=" + jobName +
                            "&userEmail=" + user +
                            "&userId=" + user +
                            "&ver=1.0");
                    get.setHeader("User-Agent", "Mozilla/5.0");
                    client.execute(get);
                } catch (IOException e) {
                    logger.warn("Failed send BI event for job: " + jobId, e);
                }
            }
        }
        return result;
    }
}
