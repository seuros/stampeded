use super::*;

#[test]
fn a_hold_is_given_back_when_its_job_is_done_with() {
    let holdings = Holdings::default();
    let first = holdings.hold("extraction");
    let second = holdings.hold("extraction");
    let other = holdings.hold("mail");
    assert_eq!(holdings.count("extraction"), 2);
    assert_eq!(holdings.count("mail"), 1);

    drop(first);
    assert_eq!(holdings.count("extraction"), 1);
    drop((second, other));
    assert_eq!(holdings.count("extraction"), 0);
    assert_eq!(holdings.count("mail"), 0);
}
