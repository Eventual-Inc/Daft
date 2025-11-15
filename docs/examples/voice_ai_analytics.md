# Simplifying Voice AI Analytics with Daft

## Transcription, Summaries, and Embeddings at Scale

This tutorial walks through how to build a Voice AI analytics pipeline using Daft and Faster-Whisper from raw audio to searchable, multilingual transcripts. You'll learn how to:

- Transcribe long-form audio using Faster-Whisper with built-in VAD for speaker segmentation
- Use Daft's dataframe engine to orchestrate and parallelize multimodal processing at scale
- Generate summaries, translations, and embeddings directly from transcripts

In short, Daft simplifies multimodal AI pipelines letting developers process, enrich, and query audio data with the same ease as tabular data.

## Introduction to Voice AI

Behind every AI meeting note, podcast description, and voice agent lies an AI pipeline that transcribes raw audio into text and enriches those transcripts to make it retrieval performant for downstream applications.

Voice AI encompasses a broad range of tasks:

1. **Voice Activity Detection (VAD)** - Detects when speech is present in an audio signal
2. **Speech-to-Text (STT)** - The core method of extracting transcriptions from audio
3. **Speaker Diarization** - Identifies and segments which speaker is talking when
4. **LLM Text Generation** - For summaries, translations, and more
5. **Text-to-Speech (TTS)** - Brings LLM responses and translations to life in spoken form
6. **Turn Detection** - Useful for live voice chat

In this tutorial we will focus on **Speech-to-Text (STT)** and **LLM Text Generation**, exploring common techniques for preprocessing and enriching human speech from audio to support downstream applications like meeting summaries, short-form editing, and embeddings.

## Challenges in Processing Audio for AI Pipelines

Audio is inherently different from traditional analytics processing. Most multimodal AI workloads require some level of preprocessing before inference, but since audio isn't stored in neat rows and columns in a table, running frontier models on audio data comes with some extra challenges.

**Before we can run our STT models on audio data we'll need to:**

- Read and preprocess raw audio files into a form that the model can receive
- Handle memory constraints (e.g., one hour of 48 kHz/24-bit stereo audio can be 518 MB)
- Decode, buffer, and resample audio files into chunks
- Manage streaming architectures with message queues for back pressure

**Traditional approaches face challenges:**

- Scaling parallelism requires multiprocessing/threading (error-prone, GIL limitations)
- Memory management needs custom generators/lazy loading (overflows common)
- Pipelining stages are hardcoded (modifications tedious, no retry mechanisms)
- Storing and querying outputs requires custom scripts (performance degradation)

**Daft solves these issues by:**

- Providing a unified dataframe interface for multimodal data
- Handling distributed parallelism automatically
- Managing memory efficiently with Apache Arrow format
- Enabling lazy evaluation for optimal query planning

## Setup and Imports

Let's start by importing the necessary libraries and setting up our environment.

First, install the required dependencies:

```bash
uv pip install daft faster-whisper soundfile sentence-transformers python-dotenv openai
```

Then import the necessary modules:

```python
from dataclasses import asdict
import os

import daft
from daft import DataType, col
from daft.functions import format, file, unnest
from daft.functions.ai import prompt, embed_text
from daft.ai.openai.provider import OpenAIProvider
from faster_whisper import WhisperModel, BatchedInferencePipeline

# Load environment variables
from dotenv import load_dotenv
load_dotenv()
```

## Define Constants and Configuration

Let's define the parameters we'll use throughout this tutorial.

```python
# Define Constants
SAMPLE_RATE = 16000
DTYPE = "float32"
BATCH_SIZE = 16

# Define Parameters
SOURCE_URI = "hf://datasets/Eventual-Inc/sample-files/audio/*.mp3"
DEST_URI = ".data/voice_ai_analytics"
LLM_MODEL_ID = "openai/gpt-oss-120b"
EMBEDDING_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
CONTEXT = "Daft: Unified Engine for Data Analytics, Engineering & ML/AI (github.com/Eventual-Inc/Daft) YouTube channel video. Transcriptions can have errors like 'DAF' referring to 'Daft'."
PRINT_SEGMENTS = True
```

## Building a High-Performance Transcription Pipeline with Faster-Whisper

[Faster-Whisper](https://github.com/SYSTRAN/faster-whisper) comes with built-in VAD from Silero for segmenting long-form audio into neat chunks. This makes it so we don't need to worry about the length of video or handle any windowing ourselves since Whisper only operates over 30 sec chunks. We also want to take full advantage of faster-whisper's `BatchedInferencePipeline` to improve our throughput.

### Creating the FasterWhisperTranscriber Class

We'll define a `FasterWhisperTranscriber` class and decorate it with `@daft.cls()`. This converts any standard Python class into a distributed massively parallel user-defined-function, enabling us to take full advantage of Daft's rust-backed performance.

**Key design decisions:**

- We separate model loading from inference in the `__init__` method
- Models can easily reach multiple GB in size, so we initialize during class instantiation to avoid repeated downloads
- We input a `daft.File` and return a dictionary that will be materialized as a `daft.DataType.struct()`
- Faster-whisper supports reading files directly, so we use `daft.File` for simplified preprocessing

Note: Jump to the bottom of this document to see how [`TranscriptionResult`]() is defined.

```python
@daft.cls()
class FasterWhisperTranscriber:
    def __init__(self, model="distil-large-v3", compute_type="float32", device="auto"):
        self.model = WhisperModel(model, compute_type=compute_type, device=device)
        self.pipe = BatchedInferencePipeline(self.model)

    @daft.method(return_dtype=TranscriptionResult)
    def transcribe(self, audio_file: daft.File):
        """Transcribe Audio Files with Voice Activity Detection (VAD) using Faster Whisper"""
        with audio_file.to_tempfile() as tmp:
            segments_iter, info = self.pipe.transcribe(
                str(tmp.name),
                vad_filter=True,
                vad_parameters=dict(min_silence_duration_ms=500, speech_pad_ms=200),
                word_timestamps=True,
                without_timestamps=False,
                temperature=0,
                batch_size=BATCH_SIZE,
            )
            segments = [asdict(seg) for seg in segments_iter]
            text = " ".join([seg["text"] for seg in segments])

            return {"transcript": text, "segments": segments, "info": asdict(info)}
```

### Setting Up OpenAI Provider for LLM Operations

We'll use OpenRouter as our LLM provider for summaries and translations. Let's configure it:

```python
# Create an OpenAI provider, attach, and set as the default
openrouter_provider = OpenAIProvider(
    name="OpenRouter",
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ.get("OPENROUTER_API_KEY"),
)
daft.attach_provider(openrouter_provider)
daft.set_provider("OpenRouter")
```

### Understanding Daft's DataFrame Interface

Before we dive into transcription, let's understand why Daft's dataframe interface is powerful:

1. **Tabular Operations**: Perform traditional operations within a managed data model - harder to mess up data structures
2. **Automatic Parallelism**: Abstract complexity of orchestrating processing for distributed parallelism - maximum CPU and GPU utilization by default
3. **Lazy Evaluation**: Operations aren't materialized until we invoke collection - enables query optimization and decouples transformations from load

Daft's execution engine runs on a push-based processing model, enabling the engine to optimize each operation by planning everything from query through the logic and finally writing to disk.

## Step 1: Transcription

Now let's transcribe our audio files. We'll:
1. Discover audio files from the source URI
2. Wrap paths as `daft.File` objects
3. Transcribe using our FasterWhisperTranscriber
4. Unpack the results into separate columns

```python
# Instantiate Transcription UDF
fwt = FasterWhisperTranscriber()

# Transcribe the audio files
df_transcript = (
    # Discover the audio files
    daft.from_glob_path(SOURCE_URI)
    # Wrap the path as a daft.File
    .with_column("audio_file", file(col("path")))
    # Transcribe the audio file with Voice Activity Detection (VAD) using Faster Whisper
    .with_column("result", fwt.transcribe(col("audio_file")))
    # Unpack Results
    .select("path", "audio_file", unnest(col("result")))
).collect()

print(
    "\n\nRunning Transcription with Voice Activity Detection (VAD) using Faster Whisper..."
)
```

```python
# Show the transcript
df_transcript.select(
    "path",
    "info",
    "transcript",
    "segments",
).show(format="fancy", max_width=40)
```

Great! We've successfully transcribed our audio files. The dataframe now contains:
- `path`: The source file path
- `transcript`: The full transcription text
- `segments`: A list of transcription segments with timestamps
- `info`: Metadata about the transcription (language, duration, etc.)

## Step 2: Summarization

Moving on to our downstream enrichment stages, summarization is a common and simple means of leveraging an LLM for publishing, socials, or search. With Daft, generating a summary from your transcripts is as simple as adding a column.

We'll also demonstrate how easy it is to add translations - since all the data is organized and accessible, we just need to declare what we want!

```python
# Summarize the transcripts and translate to Chinese
df_summaries = (
    df_transcript
    # Summarize the transcripts
    .with_column(
        "summary",
        prompt(
            format(
                "Summarize the following transcript from a YouTube video belonging to {}: \n {}",
                daft.lit(CONTEXT),
                col("transcript"),
            ),
            model=LLM_MODEL_ID,
        ),
    ).with_column(
        "summary_chinese",
        prompt(
            format(
                "Translate the following text to Simplified Chinese: <text>{}</text>", col("summary")
            ),
            system_message="You will be provided with a piece of text. Your task is to translate the text to Simplified Chinese exactly as it is written. Return the translated text only, no other text or formatting.",
            model=LLM_MODEL_ID,
        ),
    )
)

print("\n\nGenerating Summaries...")
```

```python
# Show the summaries and the transcript
df_summaries.select(
    "path",
    "transcript",
    "summary",
    "summary_chinese",
).show(format="fancy", max_width=40)
```

Excellent! We now have summaries in both English and Chinese. This demonstrates how easy it is to add multilingual support to your pipeline.

## Step 3: Generating Subtitles

A common downstream task is preparing subtitles. Since our segments come with start and end timestamps, we can easily add another section to our Voice AI pipeline for translation. We'll explode the segments (one row per segment) and translate each segment to Simplified Chinese.

```python
# Explode the segments, embed, and translate to simplified Chinese for subtitles
df_segments = (
    df_transcript.explode("segments")
    .select(
        "path",
        unnest(col("segments")),
    )
    .with_column(
        "segment_text_chinese",
        prompt(
            format("Translate the following text to Simplified Chinese: <text>{}</text>", col("text")),
            system_message="You will be provided with a transcript segment. Your task is to translate the text to Simplified Chinese exactly as it is written. Return the translated text only, no other text or formatting.",
            model=LLM_MODEL_ID,
        ),
    )
)

print("\n\nGenerating Chinese Subtitles...")
```

```python
# Show the segments and translations
df_segments.select(
    "path",
    col("text"),
    "segment_text_chinese",
).show(format="fancy", max_width=40)
```

Perfect! These segments can now be used to make content more accessible for wider audiences, which is a great way to increase reach. Each segment has:
- Original text with timestamps (`start`, `end`)
- Chinese translation
- Ready to use for subtitle generation

## Step 4: Embedding Segments for Later Retrieval

Our final stage is embeddings. If you're going through the trouble of transcription, you might as well make that content available as part of your knowledge base. Meeting notes might not be the most advanced AI use-case anymore, but it still provides immense value for tracking decisions and key moments in discussions.

Adding an embeddings stage is as simple as calling `embed_text()`:

```python
# Embed the segments
df_segments = (
    df_segments.with_column(
        "segment_embeddings",
        embed_text(
            col("text"),
            provider="transformers",
            model=EMBEDDING_MODEL_ID,
        ),
    )
)

print("\n\nGenerating Embeddings for Segments...")
```

```python
# Show the segments with embeddings
df_segments.select(
    "path",
    "text",
    "segment_embeddings",
).show(format="fancy", max_width=40)
```

Excellent! Daft's native embedding DataType intelligently stores embedding vectors for you, regardless of their size. Now you have:
- Transcript segments with timestamps
- Embeddings ready for semantic search
- Translations for multilingual support

## Summary

We've successfully built a complete Voice AI Analytics pipeline that:

1. ✅ **Ingests** a directory of audio files
2. ✅ **Transcribes** speech to text using Faster-Whisper with VAD
3. ✅ **Generates** summaries from the transcripts
4. ✅ **Translates** transcript segments to Chinese for subtitles
5. ✅ **Embeds** transcriptions for future semantic search

## Extensions and Next Steps

From here there are several directions you could take:

### 1. **Q/A Chatbot**
Leverage the embeddings to host a Q/A chatbot that enables listeners to engage with content across episodes:
- "What did Sam Harris say about free will in episode 267?"
- "Find all discussions about AI safety across my subscribed podcasts"

### 2. **Recommendation Engine**
Build recommendation engines that surface hidden gems based on semantic similarity rather than just metadata tags.

### 3. **Dynamic Highlight Reels**
Create dynamic highlight reels that auto-generate shareable clips based on sentiment spikes and topic density.

### 4. **RAG Workflow**
Leverage Daft's `cosine_distance` function to put together a full RAG (Retrieval-Augmented Generation) workflow for an interactive experience.

### 5. **Analytics Dashboards**
Use the same tooling to power analytics dashboards showcasing trending topics, or supply content for automated newsletters.

Since everything you store is queryable and performant, the only limit is your imagination!

## Key Takeaways

- **Daft simplifies multimodal AI pipelines** - Process, enrich, and query audio data with the same ease as tabular data
- **Automatic parallelism** - Maximum CPU and GPU utilization by default
- **Lazy evaluation** - Optimized query planning and efficient resource usage
- **Easy extensibility** - Adding new stages (summaries, translations, embeddings) is just another line of code
- **No manual orchestration** - No need to handle VAD, batching, or multiprocessing manually

## Conclusion

At Eventual, we're simplifying multimodal AI so you don't have to. Managing voice AI pipelines or processing thousands of hours of podcast audio ultimately comes down to a few universal needs:

- **Transcripts** so your content is accessible and searchable
- **Summaries** so your listeners can skim and find what matters
- **Translations** so you can localize your content to your audience
- **Embeddings** so people can ask questions like "Which episode talked about reinforcement learning?"

Traditionally, delivering all of this meant juggling multiple tools, data formats, and scaling headaches — a brittle setup that doesn't grow with your workload. With Daft, you get one unified engine to process, store, and query multimodal data efficiently.

**Fewer moving parts means fewer failure points, less debugging, and a much shorter path from raw audio to usable insights.**

---

*For more examples and to get help, check out:*
- **GitHub**: [github.com/Eventual-Inc/Daft](https://github.com/Eventual-Inc/Daft)
- **Slack**: Join our community for support and discussions


## Appendix: `TranscriptionResult` Definition

```python
from daft import DataType

WordStruct = DataType.struct(
    {
        "start": DataType.float64(),
        "end": DataType.float64(),
        "word": DataType.string(),
        "probability": DataType.float64(),
    }
)

SegmentStruct = DataType.struct(
    {
        "id": DataType.int64(),
        "seek": DataType.int64(),
        "start": DataType.float64(),
        "end": DataType.float64(),
        "text": DataType.string(),
        "tokens": DataType.list(DataType.int64()),
        "avg_logprob": DataType.float64(),
        "compression_ratio": DataType.float64(),
        "no_speech_prob": DataType.float64(),
        "words": DataType.list(WordStruct),
        "temperature": DataType.float64(),
    }
)

TranscriptionOptionsStruct = DataType.struct(
    {
        "beam_size": DataType.int64(),
        "best_of": DataType.int64(),
        "patience": DataType.float64(),
        "length_penalty": DataType.float64(),
        "repetition_penalty": DataType.float64(),
        "no_repeat_ngram_size": DataType.int64(),
        "log_prob_threshold": DataType.float64(),
        "no_speech_threshold": DataType.float64(),
        "compression_ratio_threshold": DataType.float64(),
        "condition_on_previous_text": DataType.bool(),
        "prompt_reset_on_temperature": DataType.float64(),
        "temperatures": DataType.list(DataType.float64()),
        "initial_prompt": DataType.python(),
        "prefix": DataType.string(),
        "suppress_blank": DataType.bool(),
        "suppress_tokens": DataType.list(DataType.int64()),
        "without_timestamps": DataType.bool(),
        "max_initial_timestamp": DataType.float64(),
        "word_timestamps": DataType.bool(),
        "prepend_punctuations": DataType.string(),
        "append_punctuations": DataType.string(),
        "multilingual": DataType.bool(),
        "max_new_tokens": DataType.float64(),
        "clip_timestamps": DataType.python(),
        "hallucination_silence_threshold": DataType.float64(),
        "hotwords": DataType.string(),
    }
)

VadOptionsStruct = DataType.struct(
    {
        "threshold": DataType.float64(),
        "neg_threshold": DataType.float64(),
        "min_speech_duration_ms": DataType.int64(),
        "max_speech_duration_s": DataType.float64(),
        "min_silence_duration_ms": DataType.int64(),
        "speech_pad_ms": DataType.int64(),
    }
)

LanguageProbStruct = DataType.struct(
    {
        "language": DataType.string(),
        "probability": DataType.float64(),
    }
)

InfoStruct = DataType.struct(
    {
        "language": DataType.string(),
        "language_probability": DataType.float64(),
        "duration": DataType.float64(),
        "duration_after_vad": DataType.float64(),
        "all_language_probs": DataType.list(LanguageProbStruct),
        "transcription_options": TranscriptionOptionsStruct,
        "vad_options": VadOptionsStruct,
    }
)

TranscriptionResult = DataType.struct(
    {
        "transcript": DataType.string(),
        "segments": DataType.list(SegmentStruct),
        "info": InfoStruct,
    }
)

```
