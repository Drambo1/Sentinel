package com.alibaba.csp.sentinel.dashboard.datasource.mapper;

import com.alibaba.csp.sentinel.dashboard.datasource.entity.Metric;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * @author chenjunfeng
 * @version 1.0
 */
@Mapper
public interface MetricMapper extends BaseMapper<Metric> {

    int batchInsert(List<Metric> metrics);

}
