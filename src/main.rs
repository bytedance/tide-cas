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

mod grpc;
mod utils;

pub mod build {
    pub mod bazel {
        pub mod remote {
            pub mod execution {
                pub mod v2 {
                    // tonic::include_proto!("build.bazel.remote.execution.v2");
                    include!(concat!(env!("OUT_DIR"), "/build.bazel.remote.execution.v2.rs"));
                    pub const FILE_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("descriptor");
                }
            }
        }

        pub mod semver {
            // tonic::include_proto!("build.bazel.semver");
            include!(concat!(env!("OUT_DIR"), "/build.bazel.semver.rs"));
        }
    }
}

pub mod google {
    pub mod rpc {
        // tonic::include_proto!("google.rpc");
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
    pub mod longrunning {
        // tonic::include_proto!("google.longrunning");
        include!(concat!(env!("OUT_DIR"), "/google.longrunning.rs"));
    }
    pub mod bytestream {
        // tonic::include_proto!("google.bytestream");
        include!(concat!(env!("OUT_DIR"), "/google.bytestream.rs"));
    }
}

use std::sync::Arc;
use clap::{arg, ArgAction, command};
use clap::builder::{NonEmptyStringValueParser, RangedU64ValueParser};
use log::info;
use tonic::transport::Server;
use crate::build::bazel::remote::execution::v2::action_cache_server::ActionCacheServer;
use crate::build::bazel::remote::execution::v2::content_addressable_storage_server::ContentAddressableStorageServer;
use crate::build::bazel::remote::execution::v2::FILE_DESCRIPTOR_SET;
use crate::google::bytestream::byte_stream_server::ByteStreamServer;
use crate::grpc::ac_server::AcServer;
use crate::grpc::bs_server::BSServer;
use crate::grpc::cas_server::CASServer;
use tide_cas::mem::block::Block;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>  {

    env_logger::init();

    let reflect_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(FILE_DESCRIPTOR_SET)
        .build()
        .unwrap();

    let matches = command!()
        .arg(
            arg!(--path <PATH> "Set where the block file is")
                .value_parser(NonEmptyStringValueParser::new())
                .action(ArgAction::Set)
                .default_value("block")
        )
        .arg(
            arg!(--"cas-size" <SIZE> "Set the cas size (GB)")
                .value_parser(RangedU64ValueParser::<u64>::new())
                .action(ArgAction::Set)
                .required(true)
        )
        .arg(
            arg!(--"promotion-size" <SIZE> "Set the promotion size (GB). Should be smaller than 1/4 of block size. See README for more information.")
                .value_parser(RangedU64ValueParser::<u64>::new())
                .action(ArgAction::Set)
                .required(true)
        )
        .arg(
            arg!(--"size-limit" <SIZE> "Set a size limit (GB) of files in this storage. Should be smaller than promotion size.")
                .value_parser(RangedU64ValueParser::<u64>::new())
                .action(ArgAction::Set)
                .required(true)
        )
        .arg(
            arg!(--port <PORT> "Set grpc server port")
                .value_parser(RangedU64ValueParser::<u64>::new())
                .action(ArgAction::Set)
                .default_value("28981")
        )
        .get_matches();

    let block_file_path = matches.get_one::<String>("path").unwrap();
    let max_size = *matches.get_one::<u64>("cas-size").unwrap()*1024*1024*1024;
    let promotion_size = *matches.get_one::<u64>("promotion-size").unwrap()*1024*1024*1024;
    let size_limit = *matches.get_one::<u64>("size-limit").unwrap()*1024*1024*1024;
    let port = *matches.get_one::<u64>("port").unwrap();

    info!("block file path is {}, block size is {}, promotion size is {}, size limit is {}, port is {}", block_file_path, max_size, promotion_size, size_limit, port);

    let block = Arc::new(Block::new(promotion_size, max_size, size_limit, block_file_path));
    let block_cas_server = CASServer::new(block.clone());
    let block_cas_svc = ContentAddressableStorageServer::new(block_cas_server);

    let block_bs_server = BSServer::new(block.clone());
    let block_bs_svc = ByteStreamServer::new(block_bs_server);

    let block_ac_server = AcServer::new(block.clone());
    let block_ac_svc = ActionCacheServer::new(block_ac_server);

    let addr = format!("[::]:{}", port).parse().unwrap();
    Server::builder()
        .add_service(reflect_service)
        .add_service(block_cas_svc)
        .add_service(block_bs_svc)
        .add_service(block_ac_svc)
        .serve(addr).await?;
    Ok(())
}
