def mask_name(name):
    return name[:2] + "***" if isinstance(name, str) else "***"
