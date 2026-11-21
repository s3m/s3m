use super::calculate_max_requests;

#[test]
fn resume_number_preserves_requested_bound_with_zero_clamped_to_one() {
    for (number, expected) in [(0, 1), (1, 1), (2, 2), (u8::MAX, u8::MAX)] {
        assert_eq!(calculate_max_requests(number), expected);
    }
}
