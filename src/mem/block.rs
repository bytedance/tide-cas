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

//! This module provides a CAS implementation that utilizes a file-backed memory-mapped buffer.
//! The buffer is organized as a circular buffer (ring buffer). When writing data that exceeds the
//! end of the buffer, it will wrap around and start overwriting data from the beginning of the buffer.
use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use dashmap::DashMap;
use log::{error, debug};
use memmap2::{Mmap, MmapOptions};
use tonic::{Code, Status};

const PAGE_SIZE: u64 = 4096;

pub struct Block {
    block: Mmap,
    current_block_index: AtomicU64,
    block_map: DashMap<String, (u64, u64)>,
    // name -> (start_index, write_finished_size)
    write_map: DashMap<String, (u64, u64)>,
    max_size: u64,
    promotion_size: u64,
    size_limit: u64,
    block_file: Arc<std::fs::File>,
}

impl Block {

    /// create a new block using mmap
    /// ## Example
    /// ```rust
    /// use tide_cas::mem::block::Block;
    /// # fn main() {
    /// let promotion_size = /* promotion_size, should be smaller than 1/4 max_size, see README for details */
    /// #     4 * 1024;
    /// let max_size = /* size of this CAS */
    /// #     64 * 1024;
    /// let size_limit = /* file maximum size, should be smaller than promotion size */
    /// #     1024;
    /// let block_path = /* the block file path */
    /// #     "block";
    /// let block = Block::new(promotion_size, max_size, size_limit, block_path);
    /// # let r = std::fs::remove_file(block_path);
    /// # assert!(r.is_ok())
    /// # }
    /// ```
    pub fn new(promotion_size: u64, max_size: u64, size_limit: u64, file_path: &str) -> Self {
        let block_file_path = PathBuf::from(file_path);
        let block_file = OpenOptions::new().create(true).read(true).write(true).open(block_file_path).unwrap();
        block_file.set_len(max_size).unwrap();
        let block = unsafe { MmapOptions::new().map(&block_file) }.unwrap();
        Self {
            block,
            current_block_index: AtomicU64::new(0),
            block_map: DashMap::new(),
            write_map: DashMap::new(),
            max_size,
            promotion_size,
            size_limit,
            block_file: Arc::new(block_file),
        }
    }

    /// write data to CAS
    /// ## Example
    /// ```rust
    /// # use tide_cas::mem::block::Block;
    /// # tokio_test::block_on(async {
    /// let block = Block::new(
    /// /* args */
    /// # 4*1024, 64*1024, 1024, "block"
    /// );
    /// let data = /* data to write */
    /// # vec![63, 64];
    /// let name = /* name should be in this format "{hash}-{size}" */
    /// # "xxx-2";
    /// let offset = /* the write offset */
    /// # 0;
    /// # let r =
    /// block.write(data, name, offset).await;
    /// # assert!(r.is_ok());
    /// # std::fs::remove_file("block").unwrap();
    /// # })
    /// ```
    pub async fn write(&self, data: Vec<u8>, name: &str, offset: u64) -> Result<(u64, u64), Status> {
        debug!("write data len {}, name {}, offset {}", data.len(), name, offset);
        let (_, total_size) = get_hash_size_from_name(name);
        if total_size > self.size_limit {
            return Err(Status::new(Code::InvalidArgument, format!("data length max be smaller size_limit {}", self.size_limit)));
        }
        let size = data.len() as u64;
        if offset + size > total_size {
            return Err(Status::new(Code::InvalidArgument, format!("data written with offset {} size {}, expected total size {}", offset, size, total_size)));
        }

        let last_loop = offset + size == total_size;
        let start_index: u64;

        if offset == 0 {
            //align with 4k page size
            let current_index = self.current_block_index.fetch_add(total_size+PAGE_SIZE, Ordering::SeqCst);
            let mo = current_index % PAGE_SIZE;
            start_index = if mo == 0 {current_index} else {current_index + PAGE_SIZE - mo};
            let end_index = start_index + size;

            if !last_loop {
                self.write_map.insert(name.to_string(), (start_index, size));
            }

            //self.read_ahead(start_index, total_size, Advice::WillNeed);

            // self.write_block(data, start_index, end_index).await;
            if size > 0 {
                self.write_block_direct(data, start_index, end_index).await;
            }


        } else {
            {
                let r = self.write_map.get(name);
                if r.is_none() {
                    return Err(Status::new(Code::InvalidArgument, format!("write to a file {} with offset {}, but file is not created", name, offset)));
                }
                let r = r.unwrap();
                start_index = r.value().0;
            }
            let write_start = start_index + offset;
            let write_end = start_index + offset + size;
            if size > 0 {
                self.write_block_direct(data, write_start, write_end).await;
            }
            if last_loop {
                self.write_map.remove(name);
            } else {
                let mut r = self.write_map.get_mut(name).unwrap();
                let temp = *r;
                *r = (temp.0, temp.1 + size);
            }
        }

        if last_loop {
            self.block_map.insert(name.to_string(), (start_index, total_size));
        }



        Ok((start_index, total_size))
    }

    /// write data to AC
    /// ## Example
    /// ```rust
    /// # use tide_cas::mem::block::Block;
    /// # tokio_test::block_on(async {
    /// let block = Block::new(
    /// /* args */
    /// # 4*1024, 64*1024, 1024, "block"
    /// );
    /// let data = /* data to write */
    /// # vec![63, 64];
    /// let name = /* name should be in this format "{hash}-{size}" */
    /// # "xxx-2";
    /// # let r =
    /// block.write_ac(data, name).await;
    /// # assert!(r.is_ok());
    /// # std::fs::remove_file("block").unwrap();
    /// # })
    /// ```
    pub async fn write_ac(&self, data: Vec<u8>, name: &str) -> Result<(), Status> {
        debug!("write_ac data len {}, name {}", data.len(), name);
        let size = data.len() as u64;
        let current_index = self.current_block_index.fetch_add(size + PAGE_SIZE, Ordering::SeqCst);
        let mo = current_index % PAGE_SIZE;
        let start_index = if mo == 0 {current_index} else {current_index + PAGE_SIZE - mo};
        // let start_index = current_index + PAGE_SIZE - current_index % PAGE_SIZE;
        let end_index = start_index + size;
        if size > 0 {
            self.write_block_direct(data, start_index, end_index).await;
        }
        self.block_map.insert(name.to_string(), (start_index, size));
        Ok(())
    }

    async fn write_block_direct(&self, mut data: Vec<u8>, start_index: u64, end_index: u64) {
        let start_index = start_index % self.max_size;
        let end_index = end_index % self.max_size;
        let max_size = self.max_size;
        let block_file = self.block_file.clone();
        let data_len = data.len();
        let align = data_len % PAGE_SIZE as usize;
        if align !=0 {
            let data_len = data_len + PAGE_SIZE as usize - align;
            data.resize(data_len, 0);
        }
        let data = data.as_slice();
        if start_index < end_index {
            #[cfg(unix)]
            if let Err(err) = block_file.write_at(data, start_index) {
                error!("write_block_direct error: {}, start {}, end {}", err, start_index, end_index);
            }
            #[cfg(windows)]
            if let Err(err) = block_file.seek_write(data, start_index) {
                error!("write_block_direct error: {}, start {}, end {}", err, start_index, end_index);
            }

        } else {
            #[cfg(unix)]
            if let Err(err) = block_file.write_at(&data[..(max_size - start_index) as usize], start_index) {
                error!("write_block_direct error: {}, start {}, end {}", err, start_index, end_index);
            }
            #[cfg(windows)]
            if let Err(err) = block_file.seek_write(&data[..(max_size - start_index) as usize], start_index) {
                error!("write_block_direct error: {}, start {}, end {}", err, start_index, end_index);
            }

            #[cfg(unix)]
            if let Err(err) = block_file.write_at(&data[(max_size-start_index) as usize..], 0) {
                error!("write_block_direct error: {}, start {}, end {}", err, start_index, end_index);
            }

            #[cfg(windows)]
            if let Err(err) = block_file.seek_write(&data[(max_size-start_index) as usize..], 0) {
                error!("write_block_direct error: {}, start {}, end {}", err, start_index, end_index);
            }
        }

    }

    /// find if an entry is missing in CAS
    /// ## Example
    /// ```rust
    /// # use tide_cas::mem::block::Block;
    /// # tokio_test::block_on(async {
    /// let block = Block::new(
    /// /* args */
    /// # 4*1024, 64*1024, 1024, "block"
    /// );
    /// /* write some data */
    /// # let data = /* data to write */
    /// # vec![63, 64];
    /// let name = /* name should be in this format "{hash}-{size}" */
    /// # "xxx-2";
    /// # let offset = /* the write offset */
    /// # 0;
    /// # let r = block.write(data, name, offset).await;
    /// # assert!(r.is_ok());
    /// # let r =
    /// block.find(name).await;
    /// # assert!(r);
    /// # std::fs::remove_file("block").unwrap();
    /// # })
    /// ```
    pub async fn find(&self, name: &str) -> bool {
        debug!("find name {}", name);
        let start_index;
        let size;
        {
            let r = self.block_map.get(name);
            if r.is_none() {
                return false
            }
            let r = r.unwrap();
            start_index = r.value().0;
            size = r.value().1;
        }
        let cur_index = self.current_block_index.load(Ordering::SeqCst);
        if start_index + self.max_size < self.size_limit + cur_index{
            debug!("find name {} got LRUed", name);
            return false
        }
        if start_index + self.max_size < self.promotion_size + cur_index{
            debug!("find name {} got promotion", name);
            let buffer = self.read_block(start_index, start_index + size).await;
            if let Err(err) = self.write(buffer, name, 0).await {
                error!("promotion error: {}", err);
                return false
            }
        }
        true
    }

    /// read data from CAS
    /// ## Example
    /// ```rust
    /// # use tide_cas::mem::block::Block;
    /// # tokio_test::block_on(async {
    /// let block = Block::new(
    /// /* args */
    /// # 4*1024, 64*1024, 1024, "block"
    /// );
    /// /* write some data */
    /// # let data = /* data to write */
    /// # vec![63, 64];
    /// let name = /* name should be in this format "{hash}-{size}" */
    /// # "xxx-2";
    /// let offset = /* the read offset */
    /// # 0;
    /// # let r = block.write(data.clone(), name, offset).await;
    /// # assert!(r.is_ok());
    /// let length = /* the read length */
    /// # 2;
    /// # let r =
    /// block.read(name, offset, length).await;
    /// # assert!(r.is_ok());
    /// # assert_eq!(r.unwrap(), data);
    /// # std::fs::remove_file("block").unwrap();
    /// # })
    /// ```
    pub async fn read(&self, name: &str, offset: u64, length: u64) -> Result<Vec<u8>, Status> {
        debug!("read name {}, offset {}, length {}", name, offset, length);
        if name == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855-0" {
            return Ok(vec![])
        }
        let start_index;
        let size;
        {
            let r = self.block_map.get(name);
            if r.is_none() {
                return Err(Status::new(Code::NotFound, format!("data with name {} not found", name)));
            }
            let r = r.unwrap();
            start_index = r.value().0;
            size = r.value().1;
        }
        let start_index_with_offset = start_index + offset;
        let mut actual_length = length;
        if offset + length > size {
            actual_length = size - offset;
        }
        if actual_length == 0 {
            return Ok(vec![])
        }
        //validate before read
        let cur_index = self.current_block_index.load(Ordering::SeqCst);
        if start_index + self.max_size < self.size_limit + cur_index{
            debug!("read name {} got LRUed before read, offset {}, length {}", name, offset, length);
            return Err(Status::new(Code::NotFound, format!("data with name {} was in storage, but got LRUed", name)));
        }

        let end_index_with_offset_length = start_index_with_offset + actual_length;
        let buffer = self.read_block(start_index_with_offset, end_index_with_offset_length).await;

        //validate after read
        let cur_index = self.current_block_index.load(Ordering::SeqCst);
        if start_index + self.max_size < self.size_limit + cur_index{
            debug!("read name {} got LRUed after read, offset {}, length {}", name, offset, length);
            return Err(Status::new(Code::NotFound, format!("data with name {} was in storage, but got LRUed", name)));
        }

        //promotion
        if start_index + self.max_size < self.promotion_size + cur_index {
            debug!("read name {} got promotion, offset {}, length {}", name, offset, length);
            if let Err(err) = self.write(buffer.clone(), name, offset).await {
                error!("promotion error: {}", err);
            }
        }

        Ok(buffer)
    }

    /// read data from AC
    /// ## Example
    /// ```rust
    /// # use tide_cas::mem::block::Block;
    /// # tokio_test::block_on(async {
    /// let block = Block::new(
    /// /* args */
    /// # 4*1024, 64*1024, 1024, "block"
    /// );
    /// /* write some data into ac */
    /// let data = /* data to write */
    /// # vec![63, 64];
    /// let name = /* name should be in this format "{hash}-{size}" */
    /// # "xxx-2";
    /// # let r =
    /// # block.write_ac(data.clone(), name).await;
    /// # assert!(r.is_ok());
    /// # let r =
    /// block.read_ac(name).await;
    /// # assert!(r.is_ok());
    /// # assert_eq!(r.unwrap(), data);
    /// # std::fs::remove_file("block").unwrap();
    /// # })
    /// ```
    pub async fn read_ac(&self, name: &str) -> Result<Vec<u8>, Status> {
        debug!("read_ac name {}", name);
        let start_index;
        let size;
        {
            let r = self.block_map.get(name);
            if r.is_none() {
                return Err(Status::new(Code::NotFound, format!("ac with name {} not found", name)));
            }
            let r = r.unwrap();
            start_index = r.value().0;
            size = r.value().1;
        }
        //validate before read
        let cur_index = self.current_block_index.load(Ordering::SeqCst);
        if start_index + self.max_size < self.size_limit + cur_index{
            debug!("read_ac name {} got LRUed before read", name);
            return Err(Status::new(Code::NotFound, format!("ac with name {} was in storage, but got LRUed", name)));
        }

        let buffer = self.read_block(start_index, start_index + size).await;

        //validate after read
        let cur_index = self.current_block_index.load(Ordering::SeqCst);
        if start_index + self.max_size < self.size_limit + cur_index{
            debug!("read_ac name {} got LRUed after read", name);
            return Err(Status::new(Code::NotFound, format!("ac with name {} was in storage, but got LRUed", name)));
        }

        //promotion
        if start_index + self.max_size < self.promotion_size + cur_index {
            debug!("read_ac name {} got promotion", name);
            if let Err(err) = self.write_ac(buffer.clone(), name).await {
                error!("promotion error: {}", err);
            }
        }

        Ok(buffer)

    }



    async fn read_block(&self, start_index: u64, end_index: u64) -> Vec<u8> {
        let start_index = start_index % self.max_size;
        let end_index = end_index % self.max_size;
        if start_index < end_index {
            self.block[start_index as usize..end_index as usize].to_vec()
        } else {
            let mut buffer = self.block[start_index as usize..self.max_size as usize].to_vec();
            let mut buffer2= self.block[..end_index as usize].to_vec();
            buffer.append(&mut buffer2);
            buffer
        }
    }

}

#[inline]
fn get_hash_size_from_name(name: &str) -> (String, u64) {
    let eles: Vec<&str> = name.split('-').collect();
    (eles[0].to_string(), eles[1].parse().unwrap())
}