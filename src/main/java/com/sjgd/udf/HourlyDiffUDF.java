package com.sjgd.udf;

import org.apache.iotdb.udf.api.UDTF;
import org.apache.iotdb.udf.api.access.Row;
import org.apache.iotdb.udf.api.collector.PointCollector;
import org.apache.iotdb.udf.api.customizer.config.UDTFConfigurations;
import org.apache.iotdb.udf.api.customizer.parameter.UDFParameters;
import org.apache.iotdb.udf.api.customizer.strategy.RowByRowAccessStrategy;
import org.apache.iotdb.udf.api.type.Type;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * 整点差值计算UDF
 * 功能：计算相邻整点数据的差值，支持拉格朗日插值和延迟计算
 * 
 * 使用示例:
 * SELECT hourly_diff(totalSum, 'offset_minutes'='30', 'sample_range_minutes'='120') FROM root.device.totalSum
 */
public class HourlyDiffUDF implements UDTF {
    
    // 配置参数
    private int offsetMinutes = 30;        // 偏移时间（分钟）
    private int sampleRangeMinutes = 120;  // 采样范围（分钟）  
    private int intervalMinutes = 10;      // 采集间隔（分钟）
    
    // 数据存储
    private List<DataPoint> dataPoints = new ArrayList<>();
    
    @Override
    public void beforeStart(UDFParameters parameters, UDTFConfigurations configurations) throws Exception {
        // 读取配置参数
        if (parameters.hasAttribute("offset_minutes")) {
            this.offsetMinutes = Integer.parseInt(parameters.getString("offset_minutes"));
        }
        
        if (parameters.hasAttribute("sample_range_minutes")) {
            this.sampleRangeMinutes = Integer.parseInt(parameters.getString("sample_range_minutes"));
        }
        
        if (parameters.hasAttribute("interval_minutes")) {
            this.intervalMinutes = Integer.parseInt(parameters.getString("interval_minutes"));
        }
        
        configurations.setAccessStrategy(new RowByRowAccessStrategy()).setOutputDataType(Type.DOUBLE);
    }

    @Override
    public void transform(Row row, PointCollector collector) throws Exception {
        // 收集所有数据点
        if (!row.isNull(0)) {
            long timestamp = row.getTime();
            double value = getDoubleValue(row, 0);
            dataPoints.add(new DataPoint(timestamp, value));
        }
    }

    @Override
    public void terminate(PointCollector collector) throws Exception {
        if (dataPoints.isEmpty()) {
            return;
        }
        
        // 按时间排序
        dataPoints.sort(Comparator.comparing(dp -> dp.timestamp));
        
        // 计算差值
        calculateHourlyDiffs(collector);
    }
    
    /**
     * 计算整点差值的主要逻辑
     */
    private void calculateHourlyDiffs(PointCollector collector) throws Exception {
        // 找到数据的时间范围
        long startTime = dataPoints.get(0).timestamp;
        long endTime = dataPoints.get(dataPoints.size() - 1).timestamp;
        
        // 转换为小时边界
        LocalDateTime startHour = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(startTime), ZoneOffset.UTC)
            .truncatedTo(ChronoUnit.HOURS);
        
        LocalDateTime endHour = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(endTime), ZoneOffset.UTC)
            .truncatedTo(ChronoUnit.HOURS).plusHours(1);
        
        // 遍历每个整点，计算差值
        LocalDateTime currentHour = startHour.plusHours(1); // 从第二个整点开始
        
        while (!currentHour.isAfter(endHour)) {
            // 检查是否到了计算时间（整点 + 偏移时间）
            LocalDateTime triggerTime = currentHour.plusMinutes(offsetMinutes);
            LocalDateTime now = LocalDateTime.now(ZoneOffset.UTC);
            
            if (now.isBefore(triggerTime)) {
                currentHour = currentHour.plusHours(1);
                continue;
            }
            
            // 计算当前小时和前一小时的整点值
            LocalDateTime prevHour = currentHour.minusHours(1);
            
            Double prevHourValue = getHourlyValue(prevHour);
            Double currentHourValue = getHourlyValue(currentHour);
            
            if (prevHourValue != null && currentHourValue != null) {
                double diff = currentHourValue - prevHourValue;
                long diffTimestamp = currentHour.toInstant(ZoneOffset.UTC).toEpochMilli();
                
                collector.putDouble(diffTimestamp, diff);
            }
            
            currentHour = currentHour.plusHours(1);
        }
    }
    
    /**
     * 获取指定整点的值，如果没有准确的整点数据，使用拉格朗日插值
     */
    private Double getHourlyValue(LocalDateTime hourTime) {
        long hourTimestamp = hourTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        
        // 先查找准确的整点数据
        for (DataPoint dp : dataPoints) {
            if (dp.timestamp == hourTimestamp) {
                return dp.value;
            }
        }
        
        // 没有准确数据，使用拉格朗日插值
        return interpolateValue(hourTimestamp, hourTime);
    }
    
    /**
     * 拉格朗日插值计算整点值
     */
    private Double interpolateValue(long targetTimestamp, LocalDateTime hourTime) {
        // 定义采样范围
        LocalDateTime rangeStart = hourTime.minusMinutes(sampleRangeMinutes / 2);
        LocalDateTime rangeEnd = hourTime.plusMinutes(sampleRangeMinutes / 2);
        
        long rangeStartMs = rangeStart.toInstant(ZoneOffset.UTC).toEpochMilli();
        long rangeEndMs = rangeEnd.toInstant(ZoneOffset.UTC).toEpochMilli();
        
        // 筛选采样范围内的数据点
        List<DataPoint> samplePoints = new ArrayList<>();
        for (DataPoint dp : dataPoints) {
            if (dp.timestamp >= rangeStartMs && dp.timestamp <= rangeEndMs) {
                samplePoints.add(dp);
            }
        }
        
        if (samplePoints.size() < 2) {
            return null; // 数据不足，无法插值
        }
        
        // 限制使用的数据点数量，避免龙格现象（建议4-8个点）
        if (samplePoints.size() > 6) {
            // 选择距离目标时间最近的6个点
            samplePoints = selectClosestPoints(samplePoints, targetTimestamp, 6);
        }
        
        // 使用拉格朗日插值
        return lagrangeInterpolation(targetTimestamp, samplePoints);
    }
    
    /**
     * 拉格朗日插值法
     */
    private Double lagrangeInterpolation(long targetTimestamp, List<DataPoint> points) {
        if (points.size() < 2) return null;
        
        double result = 0.0;
        int n = points.size();
        
        for (int i = 0; i < n; i++) {
            double term = points.get(i).value;
            
            // 计算拉格朗日基函数 L_i(x)
            for (int j = 0; j < n; j++) {
                if (i != j) {
                    term *= (double)(targetTimestamp - points.get(j).timestamp) / 
                           (points.get(i).timestamp - points.get(j).timestamp);
                }
            }
            result += term;
        }
        
        return result;
    }
    
    /**
     * 选择距离目标时间最近的N个数据点
     */
    private List<DataPoint> selectClosestPoints(List<DataPoint> points, long targetTimestamp, int maxPoints) {
        // 按距离目标时间的远近排序
        List<DataPoint> sortedPoints = new ArrayList<>(points);
        sortedPoints.sort((a, b) -> {
            long distA = Math.abs(a.timestamp - targetTimestamp);
            long distB = Math.abs(b.timestamp - targetTimestamp);
            return Long.compare(distA, distB);
        });
        
        // 取前maxPoints个点，然后按时间重新排序
        List<DataPoint> selectedPoints = sortedPoints.subList(0, Math.min(maxPoints, sortedPoints.size()));
        selectedPoints.sort(Comparator.comparing(dp -> dp.timestamp));
        
        return selectedPoints;
    }
    
    /**
     * 从Row中获取double值
     * 修正：添加IOException处理
     */
    private double getDoubleValue(Row row, int index) throws IOException {
        switch (row.getDataType(index)) {
            case DOUBLE:
                return row.getDouble(index);
            case FLOAT:
                return row.getFloat(index);
            case INT32:
                return row.getInt(index);
            case INT64:
                return row.getLong(index);
            default:
                throw new IllegalArgumentException("Unsupported data type: " + row.getDataType(index));
        }
    }
    
    /**
     * 数据点类
     */
    private static class DataPoint {
        final long timestamp;
        final double value;
        
        DataPoint(long timestamp, double value) {
            this.timestamp = timestamp;
            this.value = value;
        }
    }
}