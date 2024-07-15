fn get_llama3_tokens() -> Vec<String> {
    let mut res: Vec<String> = vec![
        "<|begin_of_text|>",
        "<|end_of_text|>",
        "<|reserved_special_token_0|>",
        "<|reserved_special_token_1|>",
        "<|reserved_special_token_2|>",
        "<|reserved_special_token_3|>",
        "<|start_header_id|>",
        "<|end_header_id|>",
        "<|reserved_special_token_4|>",
        "<|eot_id|>",
    ]
    .into_iter()
    .map(str::to_string)
    .collect();
    for i in 5..256 {
        res.push(format!("<|reserved_special_token_{}|>", i));
    }
    res
}

pub fn get_special_tokens(name: &str) -> Option<Vec<String>> {
    match name {
        "llama3" => Some(get_llama3_tokens()),
        _ => None,
    }
}
