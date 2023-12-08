use std::{cmp::min, iter::Iterator};

pub struct PartIterator {
    seek: u64,
    chunk_size: u64,
    file_size: u64,
    number: u16,
}

impl PartIterator {
    #[must_use]
    pub const fn new(file_size: u64, chunk_size: u64) -> Self {
        Self {
            seek: 0,
            chunk_size,
            file_size,
            number: 1,
        }
    }
}

impl Iterator for PartIterator {
    type Item = (u16, u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.seek >= self.file_size || self.chunk_size == 0 {
            return None;
        }

        let chunk = min(self.chunk_size, self.file_size - self.seek);

        let result = Some((self.number, self.seek, chunk));

        log::debug!(
            "PartIterator::next() -> number: {}, seek: {}, chunk: {}",
            self.number,
            self.seek,
            chunk
        );

        self.seek += chunk;
        self.number += 1;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::s3::tools;

    #[test]
    fn test_part_iterator_with_zero_file_size() {
        let file_size = 0;
        let chunk_size = 100;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_zero_chunk_size_and_file_size() {
        let file_size = 0;
        let chunk_size = 0;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_zero_chunk_size() {
        let file_size = 100;
        let chunk_size = 0;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_30() {
        let file_size = 100;
        let chunk_size = 30;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), Some((1, 0, 30)));
        assert_eq!(part_iterator.next(), Some((2, 30, 30)));
        assert_eq!(part_iterator.next(), Some((3, 60, 30)));
        assert_eq!(part_iterator.next(), Some((4, 90, 10)));
        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    // test iterate 50GB file with 10MB chunk chunk_size
    fn test_part_iterator_with_large_file() {
        let file_size = 50 * 1024 * 1024 * 1000;
        let chunk_size = 10 * 1024 * 1024;

        let (_number, seek, chunk) = PartIterator::new(file_size, chunk_size).last().unwrap();
        assert_eq!(file_size, seek + chunk);
    }

    #[test]
    fn test_part_iterator_with_100() {
        let file_size = 100;
        let chunk_size = 100;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), Some((1, 0, 100)));
        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_1000() {
        let file_size = 100;
        let chunk_size = 1000;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), Some((1, 0, 100)));
        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_1000_and_100() {
        let file_size = 1000;
        let chunk_size = 100;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), Some((1, 0, 100)));
        assert_eq!(part_iterator.next(), Some((2, 100, 100)));
        assert_eq!(part_iterator.next(), Some((3, 200, 100)));
        assert_eq!(part_iterator.next(), Some((4, 300, 100)));
        assert_eq!(part_iterator.next(), Some((5, 400, 100)));
        assert_eq!(part_iterator.next(), Some((6, 500, 100)));
        assert_eq!(part_iterator.next(), Some((7, 600, 100)));
        assert_eq!(part_iterator.next(), Some((8, 700, 100)));
        assert_eq!(part_iterator.next(), Some((9, 800, 100)));
        assert_eq!(part_iterator.next(), Some((10, 900, 100)));
        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_1000_and_1000() {
        let file_size = 1000;
        let chunk_size = 1000;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), Some((1, 0, 1000)));
        assert_eq!(part_iterator.next(), None);
    }

    #[test]
    fn test_part_iterator_with_1000_and_10000() {
        let file_size = 1000;
        let chunk_size = 10000;

        let mut part_iterator = PartIterator::new(file_size, chunk_size);

        assert_eq!(part_iterator.next(), Some((1, 0, 1000)));
        assert_eq!(part_iterator.next(), None);
    }

    // test number of parts with 5TB file
    // <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    #[test]
    fn test_part_iterator_with_5tb_file() {
        let file_size = 5 * 1024 * 1024 * 1000 * 1000;
        let buf_size = 10 * 1024 * 1024;

        let part_size = tools::calculate_part_size(file_size, buf_size).unwrap();

        let (number, seek, chunk) = PartIterator::new(file_size, part_size).last().unwrap();
        assert_eq!(file_size, seek + chunk);
        assert_eq!((file_size + part_size - 1) / part_size, number.into());
    }
}
