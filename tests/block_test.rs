use tide_cas::mem::block::Block;

#[tokio::test(flavor = "multi_thread")]
async fn test () {
    let max_size = 64 * 1024;
    let promotion_size = 1024;
    let size_limit = 100;
    let block_path = "block";
    let block = Block::new(promotion_size, max_size, size_limit, block_path);
    let data = "hello".as_bytes();
    let name = "xxx-5";
    let r = block.write(data.to_vec(), name, 0).await;
    assert!(r.is_ok());
    let r = block.read(name, 0, 5).await;
    assert!(r.is_ok());
    let r = r.unwrap();
    assert_eq!(data, r.as_slice());
    tokio::fs::remove_file(block_path).await.expect("failed to remove block file");
}