"""TypedDict definitions and key sets for Transformers provider configuration."""

from __future__ import annotations

from typing import Any, TypedDict


class ModelLoadingOptions(TypedDict, total=False):
    """Options passed to model.from_pretrained().

    Attributes:
        model_class: Auto class name (e.g., "AutoModelForCausalLM", "AutoModelForImageTextToText").
                     Default: "AutoModelForCausalLM" for text, "AutoModelForImageTextToText" for vision.
        trust_remote_code: Whether to trust remote code when loading models. Default: True
        torch_dtype: The torch dtype to use (e.g., "float16", "bfloat16", "float32").
        device_map: Device placement strategy (e.g., "auto", "cuda", "cpu").
        attn_implementation: Attention implementation (e.g., "flash_attention_2", "sdpa").
        low_cpu_mem_usage: Whether to use low CPU memory usage mode.
        use_safetensors: Whether to use safetensors format.
    """

    trust_remote_code: bool
    torch_dtype: str
    device_map: str | dict[str, Any]
    attn_implementation: str
    low_cpu_mem_usage: bool
    use_safetensors: bool


class AutoProcessorOptions(TypedDict, total=False):
    """Options for AutoProcessor.from_pretrained().

    Attributes:
        trust_remote_code: Whether to trust remote code when loading the processor.
        use_fast: Whether to use the fast tokenizer.
        return_tensors: Tensor format ("pt", "tf", "np"). Default: "pt"
        return_dict: Return dict with input_ids, attention_mask, etc. Default: True
        padding: Padding strategy for tokenization.
        truncation: Truncation strategy for tokenization.
        max_length: Maximum length of the output.
        min_length: Minimum length of the output.
        do_sample: Whether to sample the output.
        num_beams: Number of beams for beam search.
        repetition_penalty: Repetition penalty for beam search.
        early_stopping: Whether to stop generation early.
        bos_token_id: Token ID of the beginning of sequence.
        eos_token_id: Token ID of the end of sequence.
        pad_token_id: Token ID of the padding token.
        bos_token: Token string of the beginning of sequence.
    """

    trust_remote_code: bool
    use_fast: bool
    return_tensors: str
    return_dict: bool
    padding: bool | str
    truncation: bool | str
    max_length: int
    min_length: int
    do_sample: bool
    num_beams: int
    repetition_penalty: float
    early_stopping: bool
    bos_token_id: int
    eos_token_id: int
    pad_token_id: int
    bos_token: str


class AutoTokenizerOptions(TypedDict, total=False):
    """Options for AutoTokenizer.from_pretrained().

    Attributes:
        trust_remote_code: Whether to trust remote code when loading the tokenizer.
        use_fast: Use a fast Rust-based tokenizer if supported. Default: True
        tokenizer_type: Tokenizer type to be loaded.
        revision: Specific model version (branch, tag, commit).
        subfolder: Subfolder inside the model repo.
        cache_dir: Directory to cache the downloaded tokenizer.
        force_download: Force re-download even if cached.
        local_files_only: Only use local files, no downloading.
        proxies: Proxy servers to use for requests.
        token: Bearer token for remote file access.
        add_prefix_space: Add a prefix space to the first token.
        do_lower_case: Convert text to lowercase.
        return_tensors: Tensor format ("pt", "tf", "np").
        return_token_type_ids: Return token type IDs.
        return_attention_mask: Return attention mask.
        return_overflowing_tokens: Return overflowing tokens.
        return_special_tokens_mask: Return special tokens mask.
        return_offsets_mapping: Return token-to-text offsets.
        return_length: Return sequence length.
        verbose: Print verbose output.
    """

    trust_remote_code: bool
    use_fast: bool
    tokenizer_type: str
    revision: str
    subfolder: str
    cache_dir: str
    force_download: bool
    local_files_only: bool
    proxies: dict[str, str]
    token: str | bool
    add_prefix_space: bool
    do_lower_case: bool
    return_tensors: str
    return_token_type_ids: bool
    return_attention_mask: bool
    return_overflowing_tokens: bool
    return_special_tokens_mask: bool
    return_offsets_mapping: bool
    return_length: bool
    verbose: bool


class ChatTemplateOptions(TypedDict, total=False):
    """Options for processor.apply_chat_template().

    See: https://huggingface.co/docs/transformers/chat_templating

    Attributes:
        tokenize: Whether to tokenize the output. Default: True
        add_generation_prompt: Add tokens for assistant response. Default: True
        return_tensors: Tensor format ("pt", "tf", "np"). Default: "pt"
        return_dict: Return dict with input_ids, attention_mask, etc. Default: True
        continue_final_message: Continue last message instead of new one.
        padding: Padding strategy for tokenization.
        truncation: Truncation strategy for tokenization.
    """

    tokenize: bool
    add_generation_prompt: bool
    return_tensors: str
    return_dict: bool
    continue_final_message: bool
    padding: bool | str
    truncation: bool | str


class GenerationConfigOptions(TypedDict, total=False):
    """Options for GenerationConfig and model.generate().

    See: https://huggingface.co/docs/transformers/main_classes/text_generation

    Attributes:
        max_length: Maximum length of generated tokens (prompt + new tokens).
        max_new_tokens: Maximum number of new tokens to generate.
        min_length: Minimum length of generated sequence (prompt + new tokens).
        min_new_tokens: Minimum number of new tokens to generate.
        early_stopping: Stopping condition for beam search (True/False/"never").
        max_time: Maximum computation time in seconds.
        stop_strings: String or list of strings that terminate generation.
        do_sample: Whether to use sampling (True) or greedy decoding (False).
        num_beams: Number of beams for beam search (1 = no beam search).
        use_cache: Whether to use past key/values for faster decoding.
        cache_implementation: Cache class name ("dynamic", "static", "offloaded", etc.).
        cache_config: Arguments for the cache class.
        return_legacy_cache: Whether to return legacy cache format.
        temperature: Sampling temperature (higher = more random).
        top_k: Number of highest probability tokens to keep for top-k filtering.
        top_p: Cumulative probability threshold for nucleus sampling.
        min_p: Minimum token probability scaled by most likely token.
        typical_p: Local typicality threshold for sampling.
        epsilon_cutoff: Minimum conditional probability for sampling.
        eta_cutoff: Hybrid sampling threshold (typical + epsilon).
        repetition_penalty: Penalty for repeating tokens (1.0 = no penalty).
        encoder_repetition_penalty: Penalty for tokens not in encoder input.
        length_penalty: Exponential penalty for sequence length in beam search.
        no_repeat_ngram_size: Size of ngrams that can only occur once.
        bad_words_ids: List of token id lists that cannot be generated.
        renormalize_logits: Whether to renormalize logits after processors.
        forced_bos_token_id: Token id to force as first generated token.
        forced_eos_token_id: Token id(s) to force as last token at max_length.
        remove_invalid_values: Whether to remove nan/inf outputs.
        exponential_decay_length_penalty: Tuple of (start_index, decay_factor).
        suppress_tokens: List of token ids to suppress during generation.
        begin_suppress_tokens: List of token ids to suppress at start.
        sequence_bias: Dict mapping token sequences to bias terms.
        token_healing: Whether to heal tail tokens of prompts.
        guidance_scale: Guidance scale for classifier-free guidance.
        watermarking_config: Watermarking configuration dict or object.
        num_return_sequences: Number of sequences to return per batch element.
        output_attentions: Whether to return attention tensors.
        output_hidden_states: Whether to return hidden states.
        output_scores: Whether to return prediction scores.
        output_logits: Whether to return unprocessed logits.
        return_dict_in_generate: Whether to return ModelOutput instead of tuple.
        pad_token_id: Token id for padding.
        bos_token_id: Token id for beginning-of-sequence.
        eos_token_id: Token id(s) for end-of-sequence.
        encoder_no_repeat_ngram_size: Size of ngrams from encoder that cannot repeat.
        decoder_start_token_id: Token id(s) to start decoding (encoder-decoder).
        is_assistant: Whether the model is an assistant/draft model.
        num_assistant_tokens: Number of speculative tokens for assistant generation.
        num_assistant_tokens_schedule: Schedule for assistant tokens ("constant", "heuristic", etc.).
        assistant_confidence_threshold: Confidence threshold for assistant model.
        prompt_lookup_num_tokens: Number of candidate tokens for prompt lookup.
        max_matching_ngram_size: Maximum ngram size for prompt matching.
        assistant_early_exit: Layer index for early exit assistant.
        assistant_lookbehind: Number of assistant tokens to consider for alignment.
        target_lookbehind: Number of target tokens to consider for alignment.
        compile_config: Configuration for compilation of forward pass.
        disable_compile: Whether to disable automatic compilation.
    """

    # Length control
    max_length: int
    max_new_tokens: int
    min_length: int
    min_new_tokens: int
    early_stopping: bool | str
    max_time: float
    stop_strings: str | list[str]

    # Generation strategy
    do_sample: bool
    num_beams: int

    # Cache parameters
    use_cache: bool
    cache_implementation: str
    cache_config: dict[str, Any]
    return_legacy_cache: bool

    # Logit manipulation
    temperature: float
    top_k: int
    top_p: float
    min_p: float
    typical_p: float
    epsilon_cutoff: float
    eta_cutoff: float
    repetition_penalty: float
    encoder_repetition_penalty: float
    length_penalty: float
    no_repeat_ngram_size: int
    bad_words_ids: list[list[int]]
    renormalize_logits: bool
    forced_bos_token_id: int
    forced_eos_token_id: int | list[int]
    remove_invalid_values: bool
    exponential_decay_length_penalty: tuple[int, float]
    suppress_tokens: list[int]
    begin_suppress_tokens: list[int]
    sequence_bias: dict[tuple[int], float]
    token_healing: bool
    guidance_scale: float
    watermarking_config: dict[str, Any]

    # Output variables
    num_return_sequences: int
    output_attentions: bool
    output_hidden_states: bool
    output_scores: bool
    output_logits: bool
    return_dict_in_generate: bool

    # Special tokens
    pad_token_id: int
    bos_token_id: int
    eos_token_id: int | list[int]

    # Encoder-decoder specific
    encoder_no_repeat_ngram_size: int
    decoder_start_token_id: int | list[int]

    # Assistant generation
    is_assistant: bool
    num_assistant_tokens: int
    num_assistant_tokens_schedule: str
    assistant_confidence_threshold: float
    prompt_lookup_num_tokens: int
    max_matching_ngram_size: int
    assistant_early_exit: int
    assistant_lookbehind: int
    target_lookbehind: int

    # Performance and compilation
    compile_config: dict[str, Any]
    disable_compile: bool


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "AutoProcessorOptions",
    "AutoTokenizerOptions",
    "ChatTemplateOptions",
    "ModelLoadingOptions",
]
