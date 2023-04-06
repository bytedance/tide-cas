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

use std::io::Cursor;
use std::sync::Arc;
use log::error;
use prost::Message;
use tonic::{Code, Request, Response, Status};
use crate::build::bazel::remote::execution::v2::action_cache_server::ActionCache;
use crate::build::bazel::remote::execution::v2::{ActionResult, GetActionResultRequest, UpdateActionResultRequest};
use tide_cas::mem::block::Block;

pub struct AcServer {
    block: Arc<Block>
}

impl AcServer {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block
        }
    }
}

#[tonic::async_trait]
impl ActionCache for AcServer {
    async fn get_action_result(&self, request: Request<GetActionResultRequest>) -> Result<Response<ActionResult>, Status> {
        let request = request.into_inner();
        let action_digest = request.action_digest;
        if action_digest.is_none() {
            return Err(Status::new(Code::InvalidArgument, "Action digest cannot be empty".to_string()))
        }
        let action_digest = action_digest.unwrap();
        let name = format!("{}-{}-ac", action_digest.hash, action_digest.size_bytes);
        let buffer = self.block.read_ac(name.as_str()).await?;
        let ac = ActionResult::decode(Cursor::new(buffer.clone()));
        if let Err(err) = ac {
            error!("buffer is {:?}", buffer);
            return Err(Status::new(Code::Internal, format!("Cannot decode action result, error: {}", err)))
        }
        let ac = ac.unwrap();
        Ok(Response::new(ac))

    }

    async fn update_action_result(&self, request: Request<UpdateActionResultRequest>) -> Result<Response<ActionResult>, Status> {
        let request = request.into_inner();
        let action_digest = request.action_digest;
        if action_digest.is_none() {
            return Err(Status::new(Code::InvalidArgument, "Action digest cannot be empty".to_string()))
        }
        let action_digest = action_digest.unwrap();
        let name = format!("{}-{}-ac", action_digest.hash, action_digest.size_bytes);
        let action_result = request.action_result;
        if action_result.is_none() {
            return Err(Status::new(Code::InvalidArgument, "Action result cannot be empty".to_string()))
        }
        let action_result = action_result.unwrap();
        let mut data = vec![];
        if let Err(err) = prost::Message::encode(&action_result, &mut data) {
            return Err(Status::new(Code::Internal, format!("Cannot encode action result, error: {}", err)))
        }
        self.block.write_ac(data, name.as_str()).await?;

        Ok(Response::new(action_result))
    }
}