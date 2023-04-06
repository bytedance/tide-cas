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
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};
use crate::build::bazel::remote::execution::v2;
use crate::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorage;
use crate::build::bazel::remote::execution::v2::{BatchReadBlobsRequest, BatchReadBlobsResponse, BatchUpdateBlobsRequest, BatchUpdateBlobsResponse, FindMissingBlobsRequest, FindMissingBlobsResponse, GetTreeRequest, GetTreeResponse};
use crate::google::rpc;
use tide_cas::mem::block::Block;

pub struct CASServer {
    block: Arc<Block>,
}

impl CASServer {
    pub fn new(block: Arc<Block>) -> Self {
        CASServer {
            // block: RwLock::new(block)
            block
        }
    }
}

#[tonic::async_trait]
impl ContentAddressableStorage for CASServer {
    async fn find_missing_blobs(&self, request: Request<FindMissingBlobsRequest>) -> Result<Response<FindMissingBlobsResponse>, Status> {
        let find_request = request.into_inner();
        let blob_digests : Vec<v2::Digest>  = find_request.blob_digests;
        let mut missing: Vec<v2::Digest> = vec![];
        for blob_digest in blob_digests {
            // let n = self.block.read().await;
            // if !n.find(format!("{}-{}", blob_digest.hash, blob_digest.size_bytes)).await {
            //     missing.push(blob_digest);
            // }
            if !self.block.find(format!("{}-{}", blob_digest.hash, blob_digest.size_bytes).as_str()).await {
                missing.push(blob_digest);
            }
        }

        Ok(Response::new(FindMissingBlobsResponse {
            missing_blob_digests: missing,
        }))
    }

    async fn batch_update_blobs(&self, request: Request<BatchUpdateBlobsRequest>) -> Result<Response<BatchUpdateBlobsResponse>, Status> {
        let request = request.into_inner();
        let requests: Vec<v2::batch_update_blobs_request::Request> = request.requests;
        let mut response_list: Vec<v2::batch_update_blobs_response::Response> = vec![];
        for req in requests {
            let d = req.digest.clone().unwrap();
            let name = format!("{}-{}", d.hash, d.size_bytes);
            // let mut n = self.block.write().await;
            match self.block.write(req.data, name.as_str(), 0).await {
                Ok(_) => {
                    response_list.push(v2::batch_update_blobs_response::Response {
                        digest: req.digest,
                        status: Some(rpc::Status {
                            code: 0,
                            message: "".to_string(),
                            details: vec![]
                        })
                    });
                }
                Err(err) => {
                    response_list.push(v2::batch_update_blobs_response::Response {
                        digest: req.digest,
                        status: Some(rpc::Status {
                            code: err.code() as i32,
                            message: err.message().to_string(),
                            details: vec![]
                        })
                    });
                }
            }

        }

        Ok(Response::new(BatchUpdateBlobsResponse{
            responses: response_list
        }))
    }

    async fn batch_read_blobs(&self, request: Request<BatchReadBlobsRequest>) -> Result<Response<BatchReadBlobsResponse>, Status> {
        let request = request.into_inner();
        let digests = request.digests;
        let mut responses: Vec<v2::batch_read_blobs_response::Response> = vec![];
        for d in digests {
            let name = format!("{}-{}", d.hash, d.size_bytes);
            match self.block.read(name.as_str(), 0, d.size_bytes as u64).await {
                Ok(buffer) => {
                    responses.push(v2::batch_read_blobs_response::Response {
                        digest: Some(d.clone()),
                        data: buffer,
                        compressor: 0,
                        status: None,
                    })
                }
                Err(err) => {
                    responses.push(v2::batch_read_blobs_response::Response {
                        digest: Some(d.clone()),
                        data: vec![],
                        compressor: 0,
                        status: Some(rpc::Status {
                            code: err.code() as i32,
                            message: err.message().to_string(),
                            details: vec![],
                        }),
                    })
                }
            }

        }

        Ok(Response::new(v2::BatchReadBlobsResponse {
            responses
        }))
    }

    type GetTreeStream = ReceiverStream<Result<GetTreeResponse, Status>>;

    async fn get_tree(&self, _request: Request<GetTreeRequest>) -> Result<Response<Self::GetTreeStream>, Status> {
        Err(Status::new(Code::Unimplemented, "get_tree is not implemented".to_string()))
    }
}