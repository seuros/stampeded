use super::*;

#[test]
fn a_queue_spec_parses_or_says_why_not() {
    let named = |n: &[&str]| Ok(Queues::Named(n.iter().map(ToString::to_string).collect()));
    let cases = [
        ("mail", named(&["mail"])),
        (" default , mail,default ", named(&["default", "mail"])),
        ("*", Ok(Queues::All { except: Vec::new() })),
    ];
    for (spec, expected) in cases {
        assert_eq!(Queues::parse(spec), expected, "spec {spec:?}");
    }

    for bad in ["default,,mail", "", " ", "default,*"] {
        assert!(
            Queues::parse(bad).is_err(),
            "spec {bad:?} should be refused"
        );
    }
}

#[test]
fn except_leaves_queues_out_of_every_queue_only() {
    let all = Queues::parse("*")
        .and_then(|q| q.except("dlq, audit"))
        .expect("parses");
    assert!(all.serves("mail"));
    assert!(!all.serves("dlq"));
    assert!(!all.serves("audit"));

    assert!(Queues::one("mail").except("dlq").is_err());
}

#[test]
fn a_named_selection_serves_only_its_names() {
    let q = Queues::parse("mail,reports").expect("parses");
    assert!(q.serves("reports"));
    assert!(!q.serves("default"));
}

#[test]
fn the_workers_table_and_labels_say_what_is_served() {
    assert_eq!(
        Queues::parse("a,b").expect("parses").registered(),
        ["a", "b"]
    );
    assert_eq!(Queues::parse("*").expect("parses").registered(), ["*"]);
    assert_eq!(Queues::parse("a,b").expect("parses").label(), "a,b");
}
