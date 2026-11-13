use typed_endpoint::{Download, Get, Post, Query, Stream};

struct Search;
impl Query for Search {
    fn pairs(&self) -> Vec<(&'static str, String)> {
        vec![("q &=", "Việt +/?#%".into()), ("empty", String::new())]
    }
    fn parse(_: &[(String, String)]) -> Result<Self, String> {
        Ok(Self)
    }
}

#[test]
fn query_urls_encode_keys_values_utf8_and_empty_values_for_every_get_transport() {
    let get = Get::<Search, String>::new("GET", "/search", None);
    let stream = Stream::<Search, String>::new("STREAM", "/events", None);
    let file = Download::<Search, Vec<u8>>::new("FILE", "/file", None);
    let suffix = "?q%20%26%3D=Vi%E1%BB%87t%20%2B%2F%3F%23%25&empty=";
    assert_eq!(get.url(&Search), format!("/search{suffix}"));
    assert_eq!(stream.url(&Search), format!("/events{suffix}"));
    assert_eq!(file.url(&Search), format!("/file{suffix}"));
    assert_eq!(
        Get::<(), String>::new("GET", "/search", None).url(&()),
        "/search"
    );
    assert_eq!(
        Post::<String, String>::new("POST", "/search", None).url(),
        "/search"
    );
}
