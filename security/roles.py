ROLE_PERMISSIONS = {
    "admin": ["run_etl", "decrypt"],
    "engineer": ["run_etl"],
    "viewer": []
}

def check_permission(role, action):
    if action not in ROLE_PERMISSIONS.get(role, []):
        raise PermissionError(f"Role {role} not allowed to {action}")
