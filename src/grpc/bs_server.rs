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

use std::sync::Arc;
use log::error;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status, Streaming};
use crate::build::bazel::remote::execution::v2;
use crate::google::bytestream::byte_stream_server::ByteStream;
use crate::google::bytestream::{QueryWriteStatusRequest, QueryWriteStatusResponse, ReadRequest, ReadResponse, WriteRequest, WriteResponse};
use tide_cas::mem::block::Block;
use crate::utils;

pub struct BSServer {
    block: Arc<Block>,
}

impl BSServer {
    pub fn new(block: Arc<Block>) -> Self {
        BSServer {
            block
        }
    }
}

#[tonic::async_trait]
impl ByteStream for BSServer {
    type ReadStream = ReceiverStream<Result<ReadResponse, Status>>;

    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<Self::ReadStream>, Status> {
        let request = request.into_inner();
        if request.read_limit != 0 {
            return Err(tonic::Status::new(Code::Unimplemented, "We don't support read limit != 0"))
        }

        let (d, _, compressor, err) = utils::digest::parse_bs_read_path(&request.resource_name);
        if err.is_some() {
            return Err(tonic::Status::new(Code::InvalidArgument, format!("Wrong resource name: {}", request.resource_name)))
        }
        if compressor != v2::compressor::Value::Identity {
            return Err(tonic::Status::new(Code::InvalidArgument, "We don't support compressed message"))
        }
        let name = format!("{}-{}", d.hash, d.size_bytes);
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        let inner_block = self.block.clone();
        let read_limit = 2 * 1024 * 1024_u64;
        tokio::spawn(async move {
            let mut offset_inner = request.read_offset as u64;
            loop {
                let r = inner_block.read(name.as_str(), offset_inner, read_limit).await;
                match r {
                    Ok(data) => {
                        let data_len = data.len() as u64;
                        if data_len == 0 {
                            //reach end
                            return
                        }
                        if let Err(err) = tx.send(Ok(ReadResponse {
                            data,
                        })).await {
                            error!("send data error: {}", err);
                            return
                        }
                        if data_len < read_limit {
                            //reach end
                            return
                        }
                        offset_inner += read_limit;
                    }
                    Err(err) => {
                        if let Err(err) = tx.send(Err(err)).await {
                            error!("send error error: {}", err);
                        }
                        return
                    }
                }
            }

        });


        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn write(&self, request: Request<Streaming<WriteRequest>>) -> Result<Response<WriteResponse>, Status> {
        let mut in_stream = request.into_inner();
        let mut resource_name: String = "".to_string();
        let mut write_offset: u64 = 0;
        let mut data: Vec<u8> = vec![];
        let mut finish_write = false;

        let r = in_stream.message().await;
        match r {
            Ok(Some(req)) => {
                resource_name = req.resource_name;
                write_offset = req.write_offset as u64;
                data = req.data;
                finish_write = req.finish_write;
            }
            Ok(None) => {

            }
            Err(err) => {
                return Err(err)
            }

        }
        if resource_name.is_empty() {
            return Err(Status::new(Code::InvalidArgument, "Cannot get resource name"));
        }
        let (d, _, compressor, err) = utils::digest::parse_bs_write_path(resource_name.as_str());
        if err.is_some() {
            return Err(Status::new(Code::InvalidArgument, format!("Invalid byte stream path error: {:?}", err.unwrap())))
        }
        if compressor != v2::compressor::Value::Identity {
            return Err(Status::new(Code::InvalidArgument, "We don't support compression".to_string()))
        }

        let name = format!("{}-{}", d.hash, d.size_bytes);

        if !data.is_empty() {
            let dl = data.len() as u64;
            self.block.write(data, name.as_str(), write_offset).await?;
            write_offset += dl;
        }

        loop {
            match in_stream.message().await {
                Ok(Some(req)) => {
                    if finish_write {
                        return Err(Status::new(Code::InvalidArgument, "Client closed stream twice"))
                    }
                    if req.write_offset as u64 != write_offset {
                        return Err(Status::new(Code::InvalidArgument, format!("Attempted to write at offset {}, while {} was expected, resource name is {}", req.write_offset, write_offset, resource_name)))
                    }
                    finish_write = req.finish_write;
                    data = req.data;
                    if data.is_empty() {
                        break
                    }
                    let dl = data.len() as u64;
                    self.block.write(data, name.as_str(), write_offset).await?;
                    write_offset += dl;
                }
                Ok(None) => {
                    if !finish_write {
                        return Err(Status::new(Code::InvalidArgument, "Client close stream withou finishing write"));
                    }
                    //this is the right exit
                    break
                }
                Err(error) => {
                    return Err(error)
                }
            }
        }

        Ok(Response::new(WriteResponse {
            committed_size: d.size_bytes,
        }))
    }

    async fn query_write_status(&self, _request: Request<QueryWriteStatusRequest>) -> Result<Response<QueryWriteStatusResponse>, Status> {
        Err(Status::new(Code::Unimplemented, "query_write_status is not implemented".to_string()))
    }
}

