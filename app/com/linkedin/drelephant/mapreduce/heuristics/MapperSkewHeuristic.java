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
import org.joda.time.DateTimeConstants;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


/**
 * This Heuristic analyses the skewness in the mapper input data
 */
public class MapperSkewHeuristic extends GenericSkewHeuristic {
    private static final Logger logger = Logger.getLogger(MapperSkewHeuristic.class);

    private static final HttpClient client = HttpClientBuilder.create().build();


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
                    List<MapReduceTaskData> mappersAndReducers = new ArrayList<MapReduceTaskData>();
                    List<MapReduceTaskData> mappers = Arrays.asList(data.getMapperData() != null ? data.getMapperData() : new MapReduceTaskData[]{});
                    mappersAndReducers.addAll(mappers);
                    List<MapReduceTaskData> reducers = Arrays.asList(data.getReducerData() != null ? data.getReducerData() : new MapReduceTaskData[]{});
                    mappersAndReducers.addAll(reducers);

                    HbaseStats hbaseStats = new HbaseStats().update(mappersAndReducers);

                    long bytesReadHdfs = 0;
                    long bytesReadS3 = 0;
                    long bytesWrittenHdfs = 0;
                    long bytesWrittenS3 = 0;
                    for (MapReduceTaskData mapperOrReducer : mappersAndReducers) {
                        if (mapperOrReducer.isCounterDataPresent()) {
                            MapReduceCounterData counters = mapperOrReducer.getCounters();
                            bytesReadHdfs += counters.get(MapReduceCounterData.CounterName.HDFS_BYTES_READ);
                            bytesReadS3 += counters.get(MapReduceCounterData.CounterName.S3_BYTES_READ);
                            bytesReadS3 += counters.get(MapReduceCounterData.CounterName.S3A_BYTES_READ);
                            bytesReadS3 += counters.get(MapReduceCounterData.CounterName.S3N_BYTES_READ);
                            bytesWrittenHdfs += counters.get(MapReduceCounterData.CounterName.HDFS_BYTES_WRITTEN);
                            bytesWrittenS3 += counters.get(MapReduceCounterData.CounterName.S3_BYTES_WRITTEN);
                            bytesWrittenS3 += counters.get(MapReduceCounterData.CounterName.S3A_BYTES_WRITTEN);
                            bytesWrittenS3 += counters.get(MapReduceCounterData.CounterName.S3N_BYTES_WRITTEN);
                        }
                    }

                    double mapGbH = 0;
                    double redGbH = 0;
                    double totGbH = 0;
                    double mapVCpuH = 0;
                    double redVCpuH = 0;
                    double totVCpuH = 0;
                    if (conf != null) {
                        double mapMemGb = Integer.parseInt(conf.getProperty("mapreduce.map.memory.mb")) / (double) 1024;
                        double redMemGb = Integer.parseInt(conf.getProperty("mapreduce.reduce.memory.mb")) / (double) 1024;
                        int mapVCores = Integer.parseInt(conf.getProperty("mapreduce.map.cpu.vcores"));
                        int redVCores = Integer.parseInt(conf.getProperty("mapreduce.reduce.cpu.vcores"));

                        double mapRunHours = 0;
                        for (MapReduceTaskData mapper : mappers) {
                            mapRunHours += mapper.getTotalRunTimeMs() / (double) DateTimeConstants.MILLIS_PER_HOUR;
                        }

                        double redRunHours = 0;
                        for (MapReduceTaskData reducer : reducers) {
                            redRunHours += reducer.getTotalRunTimeMs() / (double) DateTimeConstants.MILLIS_PER_HOUR;
                        }

                        mapGbH = mapMemGb * mapRunHours;
                        redGbH = redMemGb * redRunHours;
                        totGbH = mapGbH + redGbH;

                        mapVCpuH = mapVCores * mapGbH;
                        redVCpuH = redVCores * redGbH;
                        totVCpuH = mapVCpuH + redVCpuH;
                    }

                    HttpGet get = new HttpGet("http://frog.wix.com/quix?src=11&evid=2025" +
                            "&application_id=" + appId +
                            "&bytesReadHDFS=" + bytesReadHdfs +
                            "&bytesReadS3=" + bytesReadS3 +
                            "&bytesWrittenHDFS=" + bytesWrittenHdfs +
                            "&bytesWrittenS3=" + bytesWrittenS3 +
                            "&gb_hours_total=" + (int) (totGbH * 1000) +
                            "&gb_hours_reducer=" + (int) (redGbH * 1000) +
                            "&gb_hours_mapper=" + (int) (mapGbH * 1000) +
                            "&jobId=" + jobId +
                            "&jobName=" + jobName +
                            "&userEmail=" + user +
                            "&userId=" + user +
                            "&vcpu_hours_total=" + (int) (totVCpuH * 1000) +
                            "&vcpu_hours_reducer=" + (int) (redVCpuH * 1000) +
                            "&vcpu_hours_mapper=" + (int) (mapVCpuH * 1000) +
                            "&jobStatus=" + (data.getSucceeded() ? "SUCCEEDED" : "FAILED") +
                            "&jobStopTime=" + data.getFinishTime() +
                            "&jobStartTime=" + data.getStartTime() +
                            "&jobSubmitTime=" + data.getSubmitTime() +
                            "&ver=1.0");

                    get.setHeader("User-Agent", "Mozilla/5.0");
                    client.execute(get);

                    for (String uri : hbaseStats.buildRequests(appId, jobId, jobName, user)) {
                        HttpGet req = new HttpGet(uri);
                        req.setHeader("User-Agent", "Mozilla/5.0");
                        try {
                            client.execute(req);
                        } catch (IOException e) {
                            logger.warn("Failed send BI event for hbase; job: " + jobId, e);
                        }
                    }

                } catch (IOException e) {
                    logger.warn("Failed send BI event for job: " + jobId, e);
                }
            }
        }
        return result;
    }
}
