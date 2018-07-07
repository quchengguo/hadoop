package com.study.dao.mapper;

import com.study.domain.LogAnalyzeJob;
import com.study.domain.LogAnalyzeJobDetail;

import java.util.List;

/**
 * Created by quchengguo on 2018/7/7.
 */
public interface LogAnalyzerMapper {
    List<LogAnalyzeJob> findAllJobList();
    List<LogAnalyzeJobDetail> findJobDetailList();
}
