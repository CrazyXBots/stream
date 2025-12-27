def get_size(size):
    """
    Convert bytes to human readable format
    """
    try:
        size = int(size)
    except (TypeError, ValueError):
        return "0B"

    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            return f"{size:.2f}{unit}"
        size /= 1024

    return f"{size:.2f}PB"
