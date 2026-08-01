# functions media

## audio_file

```python
audio_file(url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression
```

Converts a string containing a file reference to a `daft.AudioFile` reference.

Args:
    url (String Expression): the url of the file
    verify:
        If True, verify that the file exists and is a audio file.
        If **ANY** files are not audios, this will produce an error.

    io_config (IOConfig, default=None): The IO configuration to use.

Returns:
    Expression (File[Audio] Expression): An expression containing the file reference.

## audio_metadata

```python
audio_metadata(file_expr: Expression) -> Expression
```

Get metadata for a audio file.

Args:
    file_expr (AudioFile Expression): The audio file to get metadata for.

Returns:
    Expression (Struct Expression): A struct containing the metadata
    of the audio file.

    The struct contains the following fields:
        - sample_rate: int - The sample rate of the audio file
        - channels: int - The number of channels in the audio file
        - frames: int - The number of frames in the audio file
        - format: str - The format of the audio file
        - subtype: str | None - The subtype of the audio file

## convert_image

```python
convert_image(image: Expression, mode: str | ImageMode) -> Expression
```

Convert an image expression to the specified mode.

Args:
    image (Image Expression): image to convert.
    mode (str | ImageMode): The mode to convert the image into.

Returns:
    Expression (Image Expression): An expression representing the converted image.

## crop

```python
crop(image: Expression, bbox: tuple[int, int, int, int] | Expression) -> Expression
```

Crops images with the provided bounding box.

Args:
    image (Image Expression): to crop.
    bbox (tuple[int, int, int, int] | List Expression):
        Either a tuple of (x, y, width, height)
        parameters for cropping, or a List Expression where each element is a length 4 List
        which represents the bounding box for the crop

Returns:
    Expression (Image Expression): An expression representing the cropped image

## decode_image

```python
decode_image(bytes: Expression, on_error: Literal['raise', 'null']='raise', mode: str | ImageMode | None=ImageMode.RGB) -> Expression
```

Decodes the binary data in this column into images.

This can only be applied to binary columns that contain encoded images (e.g. PNG, JPEG, etc.)

Args:
    bytes (Binary Expression): image to decode.
    on_error (str, default="raise"):
        Whether to raise when encountering an error, or log a warning and return a null
    mode (str | ImageMode | None, default=ImageMode.RGB):
        What mode to convert the images into before storing it in the column. By default, images are decoded as RGB. If this is set to None, the mode will be inferred from the underlying data.

Returns:
    Expression (Image Expression): An expression representing the decoded image.

## decode_image_file

```python
decode_image_file(file_expr: Expression, mode: str | None=None, on_error: Literal['raise', 'null']='raise') -> Expression
```

Decode image files from a File column into an Image column.

Args:
    file_expr (File Expression): The file expression to decode.
    mode (str | None, default=None): Target image mode (e.g. "RGB", "RGBA").
        If None, the mode is inferred from the image data.
    on_error (str, default="raise"): Error handling strategy.
        "raise" raises on decode failure, "null" returns null.

Returns:
    Expression (Image Expression): The decoded image.

## download

```python
download(expr: Expression, max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression
```

Treats each string as a URL, and downloads the bytes contents as a bytes column.

Args:
    expr: The expression to download.
    max_connections: The maximum number of connections to use per thread to use for downloading URLs. Defaults to 32.
    on_error: Behavior when a URL download error is encountered - "raise" to raise the error immediately or "null" to log
        the error but fallback to a Null value. Defaults to "raise".
    io_config: IOConfig to use when accessing remote storage. Note that the S3Config's `max_connections` parameter will be overridden
        with `max_connections` that is passed in as a kwarg.

Returns:
    Expression: a Binary expression which is the bytes contents of the URL, or None if an error occurred during download

Note:
    If you are observing excessive S3 issues (such as timeouts, DNS errors or slowdown errors) during URL downloads,
    you may wish to reduce the value of ``max_connections`` (defaults to 32) to reduce the amount of load you are placing
    on your S3 servers.

    Alternatively, if you are running on machines with lower number of cores but very high network bandwidth, you can increase
    ``max_connections`` to get higher throughput with additional parallelism

## encode_image

```python
encode_image(image: Expression, image_format: str | ImageFormat) -> Expression
```

Encode an image column as the provided image file format, returning a binary column of encoded bytes.

Args:
    image (Image Expression): The image to encode.
    image_format (str | ImageFormat): The image file format into which the images will be encoded.

Returns:
    Expression (Binary Expression): An expression representing a binary column of encoded image bytes.

## file

```python
file(url: Expression, io_config: IOConfig | None=None) -> Expression
```

Converts a string containing a file reference to a `daft.File` reference.

Args:
    url (String Expression): the url of the file
    io_config (IOConfig, default=None): The IO configuration to use.

Returns:
    Expression (File Expression): An expression containing the file reference.

## file_exists

```python
file_exists(file: Expression) -> Expression
```

Returns whether the file exists.

Args:
    file (File Expression): expression to evaluate.

Returns:
    Expression (Boolean Expression): expression indicating whether the file exists

## file_path

```python
file_path(file: Expression) -> Expression
```

Returns the path (URL) of the file as a string.

Args:
    file (File Expression): expression to evaluate.

Returns:
    Expression (String Expression): expression containing the file path

## file_size

```python
file_size(file: Expression) -> Expression
```

Returns the size of the file in bytes.

Args:
    file (File Expression): expression to evaluate.

Returns:
    Expression (UInt64 Expression): expression containing the file size in bytes

## guess_mime_type

```python
guess_mime_type(bytes_expr: Expression) -> Expression
```

Guess the MIME type of binary data by inspecting magic bytes.

Detects common file formats including: PNG, JPEG, GIF, WEBP, PDF, ZIP,
MP3, WAV, OGG, MP4, MPEG, HDF5, and HTML.

Note: HDF5 detection follows the registered media type and signature documented by IANA:
https://www.iana.org/assignments/media-types/application/vnd.hdfgroup.hdf5.

Returns None if the format cannot be determined.

Args:
    bytes_expr: Binary expression containing raw bytes.

Returns:
    Expression: String expression containing MIME type (e.g., "image/png") or None.

## hdf5_attrs

```python
hdf5_attrs(file_expr: Expression, h5path: str='/') -> Expression
```

Read HDF5 attributes for a group or dataset.

Expression wrapper for ``Hdf5File.attrs(h5path)``.

Args:
    file_expr: ``Hdf5File`` expression.
    h5path: Group or dataset path. Defaults to the root group ``/``.

Returns:
    Expression containing a Python dictionary of attribute names to values.

## hdf5_file

```python
hdf5_file(url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression
```

Converts a string containing a file reference to a `daft.Hdf5File` reference.

Args:
    url (String Expression): the url of the file
    verify:
        If True, verify that the file exists and is an HDF5 file.
        If **ANY** files are not HDF5 files, this will produce an error.

    io_config (IOConfig, default=None): The IO configuration to use.

Returns:
    Expression (File[Hdf5] Expression): An expression containing the file reference.

## hdf5_keys

```python
hdf5_keys(file_expr: Expression, group: str='/') -> Expression
```

List member names directly under an HDF5 group.

Expression wrapper for ``Hdf5File.keys()``, mirroring h5py
``Group.keys()`` while returning a concrete list of strings.

Args:
    file_expr: ``Hdf5File`` expression.
    group: HDF5 group within the file. Defaults to the root group ``/``.

Returns:
    Expression containing a list of child names under the group.

## hdf5_metadata

```python
hdf5_metadata(file_expr: Expression, group: str='/') -> Expression
```

Collect metadata for groups and datasets under an HDF5 group.

Expression wrapper for ``Hdf5File.metadata(group)``.

Args:
    file_expr: ``Hdf5File`` expression.
    group: HDF5 group within the file. Defaults to the root group ``/``.

Returns:
    Expression containing a list of object metadata structs.

## image_attribute

```python
image_attribute(image: Expression, name: Literal['width', 'height', 'channel', 'mode'] | ImageProperty) -> Expression
```

Get a property of the image, such as 'width', 'height', 'channel', or 'mode'.

Args:
    image (Image Expression): to retrieve the property from.
    name: The name of the property to retrieve.

Returns:
    Expression: An Expression representing the requested property.

## image_channel

```python
image_channel(image: Expression) -> Expression
```

Gets the number of channels in an image.

Args:
    image (Image Expression): image to retrieve the number of channels from.

Returns:
    Expression (UInt32 Expression): An Expression representing the number of channels in the image.

## image_file

```python
image_file(url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression
```

Converts a string containing a file reference to a `daft.ImageFile` reference.

Args:
    url (String Expression): the url of the file
    verify:
        If True, verify that the file exists and is an image file.
        If **ANY** files are not images, this will produce an error.

    io_config (IOConfig, default=None): The IO configuration to use.

Returns:
    Expression (File[Image] Expression): An expression containing the file reference.

## image_file_metadata

```python
image_file_metadata(file_expr: Expression) -> Expression
```

Extract image metadata (width, height, format, mode) from a File column.

Args:
    file_expr (File Expression): The file expression to extract metadata from.

Returns:
    Expression (Struct Expression): A struct containing:
        - width: uint32 - Image width in pixels
        - height: uint32 - Image height in pixels
        - format: str - Image format (e.g. "png", "jpeg")
        - mode: str - Image mode (e.g. "RGB", "RGBA", "L")

## image_hash

```python
image_hash(image: Expression, *, method: Literal['phash', 'phash_simple', 'dhash', 'dhash_vertical', 'ahash', 'whash', 'crop_resistant', 'colorhash']='phash', hash_size: int=8, binbits: int=3, segments: int=3) -> Expression
```

Compute a perceptual hash of an image column for near-duplicate detection.

Returns a ``FixedSizeBinary`` column.

Output size by method:

- Single-segment methods: ``hash_size * hash_size`` bits.
- ``"crop_resistant"``: ``segments * segments * hash_size * hash_size`` bits.
- ``"colorhash"``: ``14 * binbits`` bits (14 colour/intensity bins).

Two hashes with a low Hamming distance indicate visually similar images.

Args:
    image (Image Expression): image column to hash.
    method (str, default="phash"): Hash algorithm to use. One of:

        - ``"phash"``: Full 2D DCT perceptual hash -- most robust (default).
        - ``"phash_simple"``: Row-wise DCT only, compared to mean -- faster variant.
        - ``"dhash"``: Horizontal difference/gradient hash -- fast and accurate.
        - ``"dhash_vertical"``: Vertical difference hash -- compares top/bottom neighbours.
        - ``"ahash"``: Average hash -- fastest, least robust.
        - ``"whash"``: Multi-level Haar wavelet hash, bit-exact with ``imagehash.whash``.
                      Requires ``hash_size`` to be a power of 2.
        - ``"crop_resistant"``: Segment-based hash robust against cropping
                                (``segments × segments`` grid).
        - ``"colorhash"``: Color distribution hash in HSV space.

    hash_size (int, default=8): Grid size for spatial hash methods. The output
        has ``hash_size * hash_size`` bits per segment. Common values: 8 (64-bit),
        16 (256-bit). Must be a power of 2 for ``"whash"``. Ignored for ``"colorhash"``.
    binbits (int, default=3): Bits per bin for ``"colorhash"``. The output has
        ``14 * binbits`` bits total (default: 42 bits = 6 bytes). Ignored for
        all other methods.
    segments (int, default=3): Grid dimension for ``"crop_resistant"``. The image is
        divided into ``segments × segments`` equal tiles, each hashed independently.
        The total output has ``segments * segments * hash_size * hash_size`` bits.
        Ignored for all other methods.

Returns:
    Expression (FixedSizeBinary Expression): Hash bytes for each image.

## image_height

```python
image_height(image: Expression) -> Expression
```

Gets the height of an image in pixels.

Args:
    image (Image Expression): image to retrieve the height from.

Returns:
    Expression (UInt32 Expression): An Expression representing the height of the image.

## image_mode

```python
image_mode(image: Expression) -> Expression
```

Gets the mode of an image.

Args:
    image (Image Expression): image to retrieve the mode from.

Returns:
    Expression (UInt32 Expression): An Expression representing the mode of the image.

## image_to_tensor

```python
image_to_tensor(image: Expression) -> Expression
```

Convert an image expression to a tensor, inferring dtype and shape.

This is safer than casting to a tensor dtype manually, since Daft can infer the correct
pixel dtype (e.g. UInt8) and determine whether a fixed-shape tensor is appropriate.

## image_width

```python
image_width(image: Expression) -> Expression
```

Gets the width of an image in pixels.

Args:
    image (Image Expression): image to retrieve the width from.

Returns:
    Expression (UInt32 Expression): An Expression representing the width of the image.

## parse_url

```python
parse_url(expr: Expression) -> Expression
```

Parse string URLs and extract URL components.

Returns:
    Expression: a Struct expression containing the parsed URL components:
        - scheme (str): The URL scheme (e.g., "https", "http")
        - username (str): The username, if present
        - password (str): The password, if present
        - host (str): The hostname or IP address
        - port (int): The port number, if specified
        - path (str): The path component
        - query (str): The query string, if present
        - fragment (str): The fragment/anchor, if present

## resample

```python
resample(file_expr: Expression, sample_rate: int) -> Expression
```

Resample a audio file.

Args:
    file_expr (AudioFile Expression): The audio file to resample.
    sample_rate (int): The sample rate to resample to.

Returns:
    Expression (Tensor[Python] Expression): The resampled audio file.

## resize

```python
resize(image: Expression, w: int, h: int) -> Expression
```

Resize image into the provided width and height.

Args:
    image (Image Expression): expression to resize.
    w (int): Desired width of the resized image.
    h (int): Desired height of the resized image.

Returns:
    Expression (Image Expression): An expression representing an image column of the resized images.

## upload

```python
upload(expr: Expression, location: str | Expression, max_connections: int=32, on_error: Literal['raise', 'null']='raise', io_config: IOConfig | None=None) -> Expression
```

Uploads a column of binary data to the provided location(s) (also supports S3, local etc).

Files will be written into the location (folder(s)) with a generated UUID filename, and the result
will be returned as a column of string paths that is compatible with the ``download()`` Expression.

Args:
    expr: The expression to upload.
    location: a folder location or column of folder locations to upload data into
    max_connections: The maximum number of connections to use per thread to use for uploading data. Defaults to 32.
    on_error: Behavior when a URL upload error is encountered - "raise" to raise the error immediately or "null" to log
        the error but fallback to a Null value. Defaults to "raise".
    io_config: IOConfig to use when uploading data

Returns:
    Expression: a String expression containing the written filepath

## video_file

```python
video_file(url: Expression, verify: bool=False, io_config: IOConfig | None=None) -> Expression
```

Converts a string containing a file reference to a `daft.VideoFile` reference.

Args:
    url (String Expression): the url of the file
    verify:
        If True, verify that the file exists and is a video file.
        If **ANY** files are not videos, this will produce an error.

    io_config (IOConfig, default=None): The IO configuration to use.

Returns:
    Expression (File[Video] Expression): An expression containing the file reference.

## video_frames

```python
video_frames(file_expr: Expression, *, start_time: float | Expression=0, end_time: float | None | Expression=None, width: int | None=None, height: int | None=None, is_key_frame: bool | None=None, sample_interval_seconds: float | None=None) -> Expression
```

Decode all video frames within a time range, with per-frame metadata.

Mirrors the per-frame schema of ``daft.read_video_frames()``.

Args:
    file_expr (VideoFile Expression): The video file to decode frames from.
    start_time (float | Expression, optional): Start of the time range in seconds. Defaults to 0.
        If an expression is provided, the start time will be dynamic per row.
    end_time (float | None | Expression, optional): End of the time range in seconds. Defaults to None (all frames).
        If an expression is provided, the end time will be dynamic per row.
    width (int | None, optional): Target width for resizing frames. Must be provided with ``height``.
    height (int | None, optional): Target height for resizing frames. Must be provided with ``width``.
    is_key_frame (bool | None, optional): If True, decode only keyframes. If False,
        decode only non-keyframes. If None, decode all frames.
    sample_interval_seconds (float | None, optional): If provided and > 0, sample frames at
        approximately this time interval in seconds based on ``frame_time``. The algorithm
        picks the first frame whose timestamp is >= the next target time (``start_time``,
        ``start_time + interval``, ``start_time + 2*interval``, ...). Frames without valid
        timestamps are skipped. Same semantics as the source-side
        :func:`daft.read_video_frames`. Defaults to None (no sampling).

Returns:
    Expression (List[Struct] Expression): List of structs, each containing:
        - frame_index (int): 0-based index of the frame in the video stream
        - frame_time (float): Presentation time in seconds
        - frame_time_base (str): Time base as a fraction string
        - frame_pts (int): Presentation timestamp in stream time_base units
        - frame_dts (int): Decode timestamp in stream time_base units
        - frame_duration (int): Duration in stream time_base units
        - is_key_frame (bool): Whether this frame is a keyframe
        - data (Image): The decoded frame as an image

## video_keyframes

```python
video_keyframes(file_expr: Expression, *, start_time: float=0, end_time: float | None=None) -> Expression
```

Get keyframes for a video file.

Args:
file (VideoFile): The video file to get keyframes for.
start_time (float, optional): The start time of the keyframes. Defaults to 0.
end_time (float | None, optional): The end time of the keyframes. Defaults to None.

Returns:
Expression (List Expression): List of keyframes.

## video_metadata

```python
video_metadata(file_expr: Expression) -> Expression
```

Get metadata for a video file.

Args:
    file_expr (VideoFile Expression): The video file to get metadata for.

Returns:
    Expression (Struct Expression): A struct containing the metadata (width, height, fps, frame_count, time_base)
