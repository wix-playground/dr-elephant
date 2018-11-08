package com.linkedin.drelephant.mapreduce.heuristics;

import com.linkedin.drelephant.mapreduce.data.MapReduceCounterData;
import com.linkedin.drelephant.mapreduce.data.MapReduceTaskData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.BYTES_IN_REMOTE_RESULTS;
import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.BYTES_IN_RESULTS;
import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.MILLIS_BETWEEN_NEXTS;
import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.REMOTE_RPC_CALLS;
import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.ROWS_FILTERED;
import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.ROWS_SCANNED;
import static com.linkedin.drelephant.mapreduce.data.MapReduceCounterData.CounterName.RPC_CALLS;

public class HbaseStats {

    private static final MapReduceCounterData.CounterName[] hbaseCounters = new MapReduceCounterData.CounterName[]{
            BYTES_IN_REMOTE_RESULTS,
            BYTES_IN_RESULTS,
            MILLIS_BETWEEN_NEXTS,
            REMOTE_RPC_CALLS,
            RPC_CALLS,
            ROWS_FILTERED,
            ROWS_SCANNED
    };

    private Map<String, Map<String, AtomicLong>> hbaseCounterValuesByTable = new HashMap<String, Map<String, AtomicLong>>();


    public HbaseStats update(List<MapReduceTaskData> taskData) {
        for (MapReduceTaskData stats : taskData) {
            if (stats.isCounterDataPresent()) {
                update(stats.getCounters());
            }
        }

        return this;
    }

    private void update(MapReduceCounterData counterData) {
        Map<String, Long> counters = counterData.getAllCountersInGroup("BI_HBASE_TABLE_NAME");
        if (counters.isEmpty()) {
            updateCountersForTable("undefined", counterData);
        } else {
            for (String tableName : counters.keySet()) {
                updateCountersForTable(tableName, counterData);
            }
        }
    }

    private void updateCountersForTable(String tableName, MapReduceCounterData counter) {
        Map<String, AtomicLong> counterValues = hbaseCounterValuesByTable.get(tableName);
        if (counterValues == null) {
            counterValues = new HashMap<String, AtomicLong>();
            hbaseCounterValuesByTable.put(tableName, counterValues);
        }

        for (MapReduceCounterData.CounterName hbase : hbaseCounters) {
            AtomicLong value = counterValues.get(hbase.getDisplayName());
            if (value == null) {
                value = new AtomicLong();
                counterValues.put(hbase.getDisplayName(), value);
            }
            value.addAndGet(counter.get(hbase));
        }
    }

    public List<String> buildRequests(String appId, String jobId, String jobName, String user) {
        List<String> requests = new ArrayList<String>();

        for (Map.Entry<String, Map<String, AtomicLong>> tableStats : hbaseCounterValuesByTable.entrySet()) {
            StringBuilder hbaseMetrics = new StringBuilder();
            for (Map.Entry<String, AtomicLong> entry : tableStats.getValue().entrySet()) {
                hbaseMetrics.append("&").append(entry.getKey()).append("=").append(entry.getValue());
            }

            String request = "http://frog.wix.com/quix?src=11&evid=2025" +
                    "&application_id=" + appId +
                    "&jobId=" + jobId +
                    "&jobName=" + jobName +
                    "&hbaseTableName=" + tableStats.getKey() +
                    "&userEmail=" + user +
                    "&userId=" + user +
                    "&ver=1.0" + hbaseMetrics.toString();

            requests.add(request);
        }

        return requests;
    }

    //test
    public static void main(String[] args) {
        List<MapReduceTaskData> taskData = new ArrayList<MapReduceTaskData>();
        MapReduceTaskData taskDatum = new MapReduceTaskData("taskId", "taskAttempt");
        MapReduceCounterData counterData = new MapReduceCounterData();
        counterData.set("HBASE_METRICS", "BYTES_IN_REMOTE_RESULTS", 10);
        counterData.set("HBASE_METRICS", "BYTES_IN_RESULTS", 10);
        counterData.set("HBASE_METRICS", "MILLIS_BETWEEN_NEXTS", 10);
        counterData.set("HBASE_METRICS", "ROWS_FILTERED", 10);
        counterData.set("HBASE_METRICS", "ROWS_SCANNED", 10);
        taskDatum.setTimeAndCounter(null, counterData);

        taskData.add(taskDatum);
        taskData.add(taskDatum);

        System.out.println(new HbaseStats().update(taskData).buildRequests("appId", "jobId", "jobName", "user"));
    }

}
