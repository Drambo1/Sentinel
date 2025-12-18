package com.alibaba.csp.sentinel.dashboard.repository.metric;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.Metric;
import com.alibaba.csp.sentinel.dashboard.datasource.entity.MetricEntity;
import com.alibaba.csp.sentinel.dashboard.datasource.mapper.MetricMapper;
import com.alibaba.csp.sentinel.util.StringUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * mysql数据存储定义
 *
 * @author chenjunfeng
 * @version 1.0
 */
@Component
public class InMysqlMetricsRepository implements MetricsRepository<MetricEntity> {

    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    @Resource
    private MetricMapper metricMapper;

    public InMysqlMetricsRepository() {
    }

    @Override
    public void save(MetricEntity entity) {
        if (entity == null || StringUtil.isBlank(entity.getApp())) {
            return;
        }
        readWriteLock.writeLock().lock();
        try {
            metricMapper.insert(toPo(entity));
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public void saveAll(Iterable<MetricEntity> entities) {
        if (entities == null) {
            return;
        }
        readWriteLock.writeLock().lock();
        try {
            List<Metric> metrics = new ArrayList<>();
            entities.forEach(entity -> metrics.add(toPo(entity)));
            metricMapper.batchInsert(metrics);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    @Override
    public List<MetricEntity> queryByAppAndResourceBetween(String app, String resource, long startTime, long endTime) {
        List<MetricEntity> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }
        readWriteLock.readLock().lock();
        try {
            QueryWrapper<Metric> queryWrapper = new QueryWrapper<>();
            queryWrapper.lambda().between(Metric::getTimestamp, new Date(startTime), new Date(endTime));
            List<Metric> metrics = metricMapper.selectList(queryWrapper);
            if (CollectionUtils.isEmpty(metrics)) {
                return results;
            }
            metrics.forEach(m -> results.add(toPo(m)));
            return results;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    @Override
    public List<String> listResourcesOfApp(String app) {
        List<String> results = new ArrayList<>();
        if (StringUtil.isBlank(app)) {
            return results;
        }

        final long minTimeMs = System.currentTimeMillis() - 1000 * 60;
        Map<String, MetricEntity> resourceCount = new ConcurrentHashMap<>(32);
        readWriteLock.readLock().lock();
        try {
            QueryWrapper<Metric> queryWrapper = new QueryWrapper<>();
            queryWrapper.lambda().eq(Metric::getApp, app).ge(Metric::getTimestamp, new Date(minTimeMs));
            List<Metric> metrics = metricMapper.selectList(queryWrapper);
            List<MetricEntity> metricEntityList = new ArrayList<>();
            metrics.forEach(m -> metricEntityList.add(toPo(m)));

            if (CollectionUtils.isEmpty(metricEntityList)) {
                return results;
            }
            for (MetricEntity newEntity : metricEntityList) {
                String resource = newEntity.getResource();
                if (!resourceCount.containsKey(resource)) {
                    resourceCount.put(resource, MetricEntity.copyOf(newEntity));
                } else {
                    MetricEntity oldEntity = resourceCount.get(resource);
                    oldEntity.addPassQps(newEntity.getPassQps());
                    oldEntity.addRtAndSuccessQps(newEntity.getRt(), newEntity.getSuccessQps());
                    oldEntity.addBlockQps(newEntity.getBlockQps());
                    oldEntity.addExceptionQps(newEntity.getExceptionQps());
                    oldEntity.addCount(1);
                }
            }
            return resourceCount.entrySet().stream().sorted((o1, o2) -> {
                MetricEntity e1 = o1.getValue();
                MetricEntity e2 = o2.getValue();
                int t = e2.getBlockQps().compareTo(e1.getBlockQps());
                if (t != 0) {
                    return t;
                }
                return e2.getPassQps().compareTo(e1.getBlockQps());
            }).map(Map.Entry::getKey).collect(Collectors.toList());
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private Metric toPo(MetricEntity metricEntity) {
        Metric entity = new Metric();
        BeanUtils.copyProperties(metricEntity, entity, Metric.class);
        return entity;
    }

    private MetricEntity toPo(Metric entity) {
        MetricEntity metricEntity = new MetricEntity();
        BeanUtils.copyProperties(entity, metricEntity, MetricEntity.class);
        return metricEntity;
    }
}
