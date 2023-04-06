/*
 * Copyright (c) 2023 Tide-CAS contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 *
 * This file is also available under the MIT License:
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 */

use lazy_static::lazy_static;
use std::collections::HashMap;
use tonic::{Code, Status};
use crate::build::bazel::remote::execution::v2;

lazy_static! {
    static ref COMPRESSOR_ENUM_TO_PATH: HashMap<v2::compressor::Value, &'static str> = {
        let mut m = HashMap::new();
        m.insert(v2::compressor::Value::Identity, "blobs");
        m.insert(v2::compressor::Value::Zstd, "compressed-blobs/zstd");
        m.insert(v2::compressor::Value::Deflate, "compressed-blobs/deflate");
        m
    };
    static ref COMPRESSOR_NAME_TO_ENUM: HashMap<&'static str, v2::compressor::Value> = {
        let mut m = HashMap::new();
        m.insert("zstd", v2::compressor::Value::Zstd);
        m.insert("deflate", v2::compressor::Value::Deflate);
        m
    };
}

pub fn parse_bs_read_path(path: &str) -> (v2::Digest, String, v2::compressor::Value, Option<Status>) {
    let fields: Vec<&str> = path.split('/').collect();
    if fields.len() < 3 {
        return (
            v2::Digest {
                hash: "".to_string(),
                size_bytes: 0,
            },
            "".to_string(),
            v2::compressor::Value::Identity,
            Some(Status::new(Code::InvalidArgument, "Invalid resource name"))
        );
    }

    let mut split = fields.len() - 3;
    if fields[split] != "blobs" {
        if fields.len() < 4 {
            return (
                v2::Digest {
                    hash: "".to_string(),
                    size_bytes: 0,
                },
                "".to_string(),
                v2::compressor::Value::Identity,
                Some(Status::new(Code::InvalidArgument, "Invalid resource name"))
            );
        }
        split = fields.len() - 4;
    }

    return parse_bs_path_common(fields.as_slice()[..split].to_vec(), fields.as_slice()[split..].to_vec());
}


pub fn parse_bs_write_path(path: &str) -> (v2::Digest, String, v2::compressor::Value, Option<Status>) {
    let fields: Vec<&str> = path.split('/').collect();
    if fields.len() < 5 {
        return (
            v2::Digest {
                hash: "".to_string(),
                size_bytes: 0,
            },
            "".to_string(),
            v2::compressor::Value::Identity,
            Some(Status::new(Code::InvalidArgument, "Invalid resource name"))
        );
    }
    let mut split = 0;
    while fields[split] != "uploads" {
        split += 1;
        if split > fields.len() -5 {
            return (
                v2::Digest {
                    hash: "".to_string(),
                    size_bytes: 0,
                },
                "".to_string(),
                v2::compressor::Value::Identity,
                Some(Status::new(Code::InvalidArgument, "Invalid resource name"))
            );
        }
    }

    return parse_bs_path_common(fields.as_slice()[..split].to_vec(), fields.as_slice()[split + 2..].to_vec());
}

fn parse_bs_path_common(_header: Vec<&str>, mut trailer: Vec<&str>) -> (v2::Digest, String, v2::compressor::Value, Option<Status>) {
    let mut compressor = v2::compressor::Value::Identity;
    if trailer[0] == "blobs" {
        trailer.rotate_left(1);
        trailer.pop();
    } else if trailer[0] == "compressed-blobs" {
        if !COMPRESSOR_NAME_TO_ENUM.contains_key(trailer[1]) {
            return (
                v2::Digest {
                    hash: "".to_string(),
                    size_bytes: 0,
                }, "".to_string(),
                v2::compressor::Value::Identity,
                Some(Status::new(Code::InvalidArgument, "Unsupported compression scheme")));
        }
        compressor = *COMPRESSOR_NAME_TO_ENUM.get(trailer[1]).unwrap();
        trailer.rotate_left(2);
        trailer.pop();
        trailer.pop();
        if trailer.len() < 2 {
            return (v2::Digest {
                hash: "".to_string(),
                size_bytes: 0
            }, "".to_string(),
                    v2::compressor::Value::Identity,
                    Some(Status::new(Code::InvalidArgument, "Invalid resource name")));
        }
    }

    let size_bytes = trailer[1].parse::<i64>().unwrap();
    let d = v2::Digest {
        hash: trailer[0].to_string(),
        size_bytes,
    };
    (d, "".to_string(), compressor, None)
}