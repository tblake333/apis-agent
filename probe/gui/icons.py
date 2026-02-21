"""
Icon resources for the system tray application.

Generates colored circle icons programmatically using PIL.
"""

from PIL import Image, ImageDraw
from enum import Enum
from typing import Tuple


class StatusColor(Enum):
    """Status colors for tray icons."""
    GREEN = "#22c55e"   # Connected/healthy
    YELLOW = "#eab308"  # Syncing/warning
    RED = "#ef4444"     # Error/disconnected


def hex_to_rgb(hex_color: str) -> Tuple[int, int, int]:
    """Convert hex color to RGB tuple."""
    hex_color = hex_color.lstrip('#')
    return tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))


def create_status_icon(color: StatusColor, size: int = 64) -> Image.Image:
    """
    Create a circular status icon with the specified color.

    Args:
        color: StatusColor enum value
        size: Icon size in pixels (default 64 for good scaling)

    Returns:
        PIL Image with transparent background and colored circle
    """
    # Create transparent RGBA image
    image = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    # Get RGB color
    rgb = hex_to_rgb(color.value)

    # Draw filled circle with slight padding for anti-aliasing
    padding = 2
    draw.ellipse(
        [padding, padding, size - padding - 1, size - padding - 1],
        fill=(*rgb, 255)
    )

    # Add subtle highlight for 3D effect
    highlight_color = tuple(min(255, c + 60) for c in rgb) + (100,)
    draw.ellipse(
        [padding + 4, padding + 4, size // 2, size // 2],
        fill=highlight_color
    )

    return image


def get_connected_icon(size: int = 64) -> Image.Image:
    """Get green icon for connected/healthy status."""
    return create_status_icon(StatusColor.GREEN, size)


def get_syncing_icon(size: int = 64) -> Image.Image:
    """Get yellow icon for syncing/warning status."""
    return create_status_icon(StatusColor.YELLOW, size)


def get_error_icon(size: int = 64) -> Image.Image:
    """Get red icon for error/disconnected status."""
    return create_status_icon(StatusColor.RED, size)


# Pre-generated icons for quick access
class Icons:
    """Container for pre-generated status icons."""

    _connected: Image.Image = None
    _syncing: Image.Image = None
    _error: Image.Image = None

    @classmethod
    def connected(cls) -> Image.Image:
        """Get connected (green) icon."""
        if cls._connected is None:
            cls._connected = get_connected_icon()
        return cls._connected

    @classmethod
    def syncing(cls) -> Image.Image:
        """Get syncing (yellow) icon."""
        if cls._syncing is None:
            cls._syncing = get_syncing_icon()
        return cls._syncing

    @classmethod
    def error(cls) -> Image.Image:
        """Get error (red) icon."""
        if cls._error is None:
            cls._error = get_error_icon()
        return cls._error


if __name__ == "__main__":
    # Test icon generation
    import os

    test_dir = "test_icons"
    os.makedirs(test_dir, exist_ok=True)

    Icons.connected().save(f"{test_dir}/green.png")
    Icons.syncing().save(f"{test_dir}/yellow.png")
    Icons.error().save(f"{test_dir}/red.png")

    print(f"Test icons saved to {test_dir}/")
