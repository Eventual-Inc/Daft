"""Image Functions."""

from __future__ import annotations

from typing import Literal

from daft.daft import ImageFormat, ImageMode, ImageProperty
from daft.datatype import DataType
from daft.expressions import Expression


def resize(image: Expression, w: int, h: int) -> Expression:
    """Resize image into the provided width and height.

    Args:
        image (Image Expression): expression to resize.
        w (int): Desired width of the resized image.
        h (int): Desired height of the resized image.

    Returns:
        Expression (Image Expression): An expression representing an image column of the resized images.
    """
    return Expression._call_builtin_scalar_fn("image_resize", image, w=w, h=h)


def crop(image: Expression, bbox: tuple[int, int, int, int] | Expression) -> Expression:
    """Crops images with the provided bounding box.

    Args:
        image (Image Expression): to crop.
        bbox (tuple[int, int, int, int] | List Expression):
            Either a tuple of (x, y, width, height)
            parameters for cropping, or a List Expression where each element is a length 4 List
            which represents the bounding box for the crop

    Returns:
        Expression (Image Expression): An expression representing the cropped image
    """
    if not isinstance(bbox, Expression):
        if len(bbox) != 4 or not all([isinstance(x, int) for x in bbox]):
            raise ValueError(f"Expected `bbox` to be either a tuple of 4 ints or an Expression but received: {bbox}")
        bbox = Expression._to_expression(bbox).cast(DataType.fixed_size_list(DataType.uint64(), 4))
    return Expression._call_builtin_scalar_fn("image_crop", image, bbox)


def encode_image(image: Expression, image_format: str | ImageFormat) -> Expression:
    """Encode an image column as the provided image file format, returning a binary column of encoded bytes.

    Args:
        image (Image Expression): The image to encode.
        image_format (str | ImageFormat): The image file format into which the images will be encoded.

    Returns:
        Expression (Binary Expression): An expression representing a binary column of encoded image bytes.
    """
    if isinstance(image_format, str):
        image_format = ImageFormat.from_format_string(image_format.upper())
    if not isinstance(image_format, ImageFormat):
        raise ValueError(f"image_format must be a string or ImageFormat variant, but got: {image_format}")
    return Expression._call_builtin_scalar_fn("image_encode", image, image_format=image_format)


def decode_image(
    bytes: Expression,
    on_error: Literal["raise", "null"] = "raise",
    mode: str | ImageMode | None = ImageMode.RGB,
) -> Expression:
    """Decodes the binary data in this column into images.

    This can only be applied to binary columns that contain encoded images (e.g. PNG, JPEG, etc.)

    Args:
        bytes (Binary Expression): image to decode.
        on_error (str, default="raise"):
            Whether to raise when encountering an error, or log a warning and return a null
        mode (str | ImageMode | None, default=ImageMode.RGB):
            What mode to convert the images into before storing it in the column. By default, images are decoded as RGB. If this is set to None, the mode will be inferred from the underlying data.

    Returns:
        Expression (Image Expression): An expression representing the decoded image.
    """
    return Expression._call_builtin_scalar_fn("image_decode", bytes, on_error=on_error, mode=mode)


def convert_image(image: Expression, mode: str | ImageMode) -> Expression:
    """Convert an image expression to the specified mode.

    Args:
        image (Image Expression): image to convert.
        mode (str | ImageMode): The mode to convert the image into.

    Returns:
        Expression (Image Expression): An expression representing the converted image.

    """
    if isinstance(mode, str):
        mode = ImageMode.from_mode_string(mode.upper())
    if not isinstance(mode, ImageMode):
        raise ValueError(f"mode must be a string or ImageMode variant, but got: {mode}")
    return Expression._call_builtin_scalar_fn("to_mode", image, mode=mode)


def image_to_tensor(image: Expression) -> Expression:
    """Convert an image expression to a tensor, inferring dtype and shape.

    This is safer than casting to a tensor dtype manually, since Daft can infer the correct
    pixel dtype (e.g. UInt8) and determine whether a fixed-shape tensor is appropriate.
    """
    return Expression._call_builtin_scalar_fn("to_tensor", image)


def image_attribute(
    image: Expression, name: Literal["width", "height", "channel", "mode"] | ImageProperty
) -> Expression:
    """Get a property of the image, such as 'width', 'height', 'channel', or 'mode'.

    Args:
        image (Image Expression): to retrieve the property from.
        name: The name of the property to retrieve.

    Returns:
        Expression: An Expression representing the requested property.
    """
    if isinstance(name, str):
        name = ImageProperty.from_property_string(name)
    return Expression._call_builtin_scalar_fn("image_attribute", image, name)


def image_width(image: Expression) -> Expression:
    """Gets the width of an image in pixels.

    Args:
        image (Image Expression): image to retrieve the width from.

    Returns:
        Expression (UInt32 Expression): An Expression representing the width of the image.

    Example:
        >>> import daft
        >>> from daft.functions import image_width
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("width", image_width(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(image, "width")


def image_height(image: Expression) -> Expression:
    """Gets the height of an image in pixels.

    Args:
        image (Image Expression): image to retrieve the height from.

    Returns:
        Expression (UInt32 Expression): An Expression representing the height of the image.

    Example:
        >>> import daft
        >>> from daft.functions import image_height
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("height", image_height(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(image, "height")


def image_channel(image: Expression) -> Expression:
    """Gets the number of channels in an image.

    Args:
        image (Image Expression): image to retrieve the number of channels from.

    Returns:
        Expression (UInt32 Expression): An Expression representing the number of channels in the image.

    Example:
        >>> import daft
        >>> from daft.functions import image_channel
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("channel", image_channel(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(image, "channel")


def image_hash(
    image: Expression,
    *,
    method: Literal[
        "phash", "phash_simple", "dhash", "dhash_vertical", "ahash", "whash", "crop_resistant", "colorhash"
    ] = "phash",
    hash_size: int = 8,
    binbits: int = 3,
) -> Expression:
    """Compute a perceptual hash of an image column for near-duplicate detection.

    Returns a ``FixedSizeBinary`` column.

    Output size by method:

    - Single-segment methods: ``hash_size * hash_size`` bits.
    - ``"crop_resistant"``: ``9 * hash_size * hash_size`` bits (3×3 grid).
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
            - ``"crop_resistant"``: Segment-based hash robust against cropping.
            - ``"colorhash"``: Color distribution hash in HSV space.

        hash_size (int, default=8): Grid size for spatial hash methods. The output
            has ``hash_size * hash_size`` bits per segment. Common values: 8 (64-bit),
            16 (256-bit). Ignored for ``"colorhash"``.
        binbits (int, default=3): Bits per bin for ``"colorhash"``. The output has
            ``14 * binbits`` bits total (default: 42 bits = 6 bytes). Ignored for
            all other methods.

    Returns:
        Expression (FixedSizeBinary Expression): Hash bytes for each image.

    Example:
        >>> import daft
        >>> from daft.functions import image_hash
        >>> df = daft.from_pydict({"img": [...]})  # doctest: +SKIP
        >>> df = df.with_column("hash", image_hash(df["img"], method="phash"))  # doctest: +SKIP
        >>> # colour-distribution hash
        >>> df = df.with_column("chash", image_hash(df["img"], method="colorhash"))  # doctest: +SKIP
    """
    _VALID_METHODS = (
        "phash",
        "phash_simple",
        "dhash",
        "dhash_vertical",
        "ahash",
        "whash",
        "crop_resistant",
        "colorhash",
    )
    if method not in _VALID_METHODS:
        raise ValueError(f"method must be one of {_VALID_METHODS!r}, but got: {method!r}")
    if not isinstance(hash_size, int) or hash_size <= 0:
        raise ValueError(f"hash_size must be a positive integer, but got: {hash_size!r}")
    if not isinstance(binbits, int) or binbits <= 0:
        raise ValueError(f"binbits must be a positive integer, but got: {binbits!r}")
    return Expression._call_builtin_scalar_fn("image_hash", image, method=method, hash_size=hash_size, binbits=binbits)


def image_mode(image: Expression) -> Expression:
    """Gets the mode of an image.

    Args:
        image (Image Expression): image to retrieve the mode from.

    Returns:
        Expression (UInt32 Expression): An Expression representing the mode of the image.

    Example:
        >>> import daft
        >>> from daft.functions import image_mode
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("mode", image_mode(df["images"]))  # doctest: +SKIP
    """
    return image_attribute(image, "mode")


def image_hash(
    image: Expression,
    algorithm: Literal["average", "difference", "perceptual", "wavelet", "crop_resistant"] = "difference",
) -> Expression:
    """Computes a perceptual hash of an image for deduplication.

    Args:
        image (Image Expression): Image to hash.
        algorithm: Hash algorithm to use:
            - "average": Average hash - fast, good for exact duplicates
            - "difference": Difference hash - fast, robust to minor changes (default)
            - "perceptual": Perceptual hash - slower, best quality for near-duplicates
            - "wavelet": Wavelet hash - good balance of speed and quality
            - "crop_resistant": Crop-resistant hash - robust to cropping, returns variable-length binary

    Returns:
        Expression: UInt64 hash for average/difference/perceptual/wavelet;
                    Binary hash for crop_resistant.

    Example:
        >>> import daft
        >>> from daft.functions import image_hash
        >>> # Create a dataframe with an image column
        >>> df = ...  # doctest: +SKIP
        >>> df = df.with_column("hash", image_hash(df["image"], "perceptual"))  # doctest: +SKIP
    """
    valid_algorithms = ("average", "difference", "perceptual", "wavelet", "crop_resistant")
    if algorithm not in valid_algorithms:
        raise ValueError(f"algorithm must be one of {valid_algorithms}, got: {algorithm}")
    return Expression._call_builtin_scalar_fn("image_hash", image, algorithm=algorithm)
