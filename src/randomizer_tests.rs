#[cfg(test)]
mod tests {
    use crate::randomizer::mine_random_block;

    #[test]
    fn test_mine_random_block_with_no_last_block() {
        let block = mine_random_block(None, 0);

        // assert_eq!(block.index, 1);
        // assert_eq!(block.prev_hash, vec![]);
    }
}
