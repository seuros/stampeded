//! Fixtures shared by the http submodules' tests.

/// Every `src`/`href` in `html` that the browser would fetch from the network.
pub fn network_fetches(html: &str) -> Vec<String> {
    ["src=\"", "href=\""]
        .into_iter()
        .flat_map(|attr| {
            html.match_indices(attr)
                .map(move |(i, _)| &html[i + attr.len()..])
        })
        .map(|rest| rest.chars().take_while(|c| *c != '"').collect::<String>())
        .filter(|v| v.starts_with("http://") || v.starts_with("https://") || v.starts_with("//"))
        .collect()
}
