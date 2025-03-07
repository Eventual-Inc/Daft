from daft import Series, udf


@udf(
    return_dtype=str,
    concurrency=1,
    batch_size=1024,
)
class vLLM:
    """A simple expression that runs vLLM over an input column of strings.

    NOTE: This requires `vllm` to be installed. See: [vllm documentation](https://docs.vllm.ai/en/latest/getting_started/installation/index.html).

    ## Concurrency

    By default, this will run only 1 instance of a vLLM model for the entire query.

    To override this behavior, call `vllm = vLLM.with_concurrency(N)`. This is especially useful if you have `N` number of GPUs on your
    cluster and you wish to have each replica of vLLM use 1 GPU:

    ```python
    vllm = vLLM.override_options(num_gpus=1).with_concurrency(8)
    ```


    ## Choosing a model

    [Full list of supported models from vllm](https://docs.vllm.ai/en/latest/models/supported_models.html#supported-models)

    This UDF uses the `"facebook/opt-125m"` model by default. To select the model you intend to use, you can parametrize this UDF like so:

    ```python
    vllm = vLLM.with_init_args(model="facebook/opt-125m")
    ```

    Please note that you need to ensure that your model fits on your GPUs.

    ## Tuning batch size

    By default, this UDF has a batch size of 1024. Depending on your model/GPU, you may wish to tune this parameter for memory.

    Setting a smaller batch size can reduce memory usage, trading off performance as the model will be called more times.

    ```python
    vllm = vLLM.with_init_args(batch_size=128)
    ```

    ## GPUs

    To use GPUs, create a new overridden instance of this with `vllm = vLLM.override_options(num_gpus=1)`. Note that if you do not do this,
    vLLM may fail to find the GPUs on your machine and you will get errors that look like: `RuntimeError: No CUDA GPUs are available`.

    Usage:

    >>> # CPU-based usage:
    >>> import daft
    >>>
    >>> df = daft.read_csv("prompts.csv")
    >>> df = df.with_column("response", vLLM(df["prompt"]))
    >>>
    >>> # Runs 1 replica of vLLM on CPUs by default
    >>> df.collect()

    >>> # GPU-based usage:
    >>> import daft
    >>>
    >>> # Tells Daft to run 4 replicas of vLLM, each using 1 GPU
    >>> vllm_gpu = vLLM.override_options(num_gpus=1).with_concurrency(4)
    >>>
    >>> df = daft.read_csv("prompts.csv")
    >>> df = df.with_column("response", vllm_gpu(df["prompt"]))
    >>>
    >>> # Runs 4 replicas of vLLM on 1 GPU each as specified
    >>> df.collect()
    """

    def __init__(self, model: str = "facebook/opt-125m", sampling_params: dict = {}):
        from vllm import LLM, SamplingParams

        self.llm = LLM(model=model)
        self.sampling_params = SamplingParams(**sampling_params)

    def __call__(self, input_prompt_column: Series):
        prompts = input_prompt_column.to_pylist()
        outputs = self.llm.generate(prompts, self.sampling_params)

        return [output.outputs[0].text for output in outputs]
