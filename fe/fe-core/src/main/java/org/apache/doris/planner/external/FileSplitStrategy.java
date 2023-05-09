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

package org.apache.doris.planner.external;

import com.google.common.base.Preconditions;
import org.apache.doris.common.Config;

import org.apache.doris.thrift.TFileFormatType;
import org.apache.hadoop.mapred.FileSplit;

public class FileSplitStrategy {
    private long totalSplitSize;
    private int splitNum;
    private TFileFormatType fileFormatType;
    FileSplitStrategy(TFileFormatType fileFormatType) {
        Preconditions.checkNotNull(fileFormatType);
        this.totalSplitSize = 0;
        this.splitNum = 0;
        this.fileFormatType = fileFormatType;
    }

    public void update(FileSplit split) {
        totalSplitSize += split.getLength();
        splitNum++;
    }

    public boolean hasNext(TFileFormatType fileFormatType) {
        return fileFormatType != this.fileFormatType
                || totalSplitSize > Config.file_scan_node_split_size
                || splitNum > Config.file_scan_node_split_num;
    }

    public void next(TFileFormatType newFileFormatType) {
        totalSplitSize = 0;
        splitNum = 0;
        fileFormatType = newFileFormatType;
    }

}
