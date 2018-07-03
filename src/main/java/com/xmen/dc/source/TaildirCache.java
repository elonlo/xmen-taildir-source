/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.xmen.dc.source;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ElonLo on 1/19/2018.
 */
public class TaildirCache {
    private static final Logger logger = LoggerFactory.getLogger(TaildirCache.class);
    private static final FileSystem FS = FileSystems.getDefault();
    private ConcurrentHashMap<String, TaildirMatcher> dirCache = new ConcurrentHashMap<>();
    private Map<String, String> filePaths;
    private List<RegexFilePath> regexFilePaths = Lists.newArrayList();
    private boolean cachePatternMatching;

    public TaildirCache(Map<String, String> filePaths, boolean cachePatternMatching) {
        this.filePaths = filePaths;
        this.cachePatternMatching = cachePatternMatching;
        init();
    }

    public final Collection<TaildirMatcher> getDirCache() {
        return dirCache.values();
    }

    public void close() {

    }

    private void init() {
        initDirCache();
    }

    private void initDirCache() {
        Pattern pattern = Pattern.compile(".*[\\*\\?\\%]");
        for (Map.Entry<String, String> e : filePaths.entrySet()) {
            File file = new File(e.getValue());
            Matcher matcher = pattern.matcher(file.getParent());
            if (matcher.matches()) {
                regexFilePaths.add(new RegexFilePath(file.getParent(), file.getName()));
            } else {
                dirCache.put(e.getKey(), new TaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));
            }
        }
        updateCache();
    }

    public void updateCache() {
        for (RegexFilePath regexFilePath : regexFilePaths) {
            Map<String, String> dirs = regexFilePath.getMatchingDirs();
            if (dirs.size() <= 0)
                continue;

            for (Map.Entry<String, String> e : dirs.entrySet()) {
                if (dirCache.containsKey(e.getKey()))
                    continue;
                dirCache.put(e.getKey(), new TaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));
            }
        }
        logger.info("TailDirCache: " + dirCache.toString());
    }

    private class RegexFilePath {
        private File parentDir;
        private String nameRegex;
        private final DirectoryStream.Filter<Path> fileFilter;

        public RegexFilePath(String path, String nameRegex) {
            this.nameRegex = nameRegex;

            File file = new File(path);
            this.parentDir = file.getParentFile();

            final PathMatcher matcher = FS.getPathMatcher("regex:" + file.getName());
            this.fileFilter = new DirectoryStream.Filter<Path>() {
                @Override
                public boolean accept(Path entry) throws IOException {
                    return matcher.matches(entry.getFileName()) && Files.isDirectory(entry);
                }
            };

            Preconditions.checkState(parentDir.exists(),
                    "Directory does not exist: " + parentDir.getAbsolutePath());
        }

        public Map<String, String> getMatchingDirs() {
            Map<String, String> result = Maps.newHashMap();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(parentDir.toPath(), fileFilter)) {

                for (Path entry : stream) {
                    File file = entry.toFile();
                    result.put(file.getName(),
                            file.getAbsolutePath() + System.getProperty("file.separator") + nameRegex);
                }
            } catch (IOException e) {
                logger.error("I/O exception occurred while listing parent directory. " +
                        "Files already matched will be returned. " + parentDir.toPath(), e);
            }
            return result;
        }
    }
}
