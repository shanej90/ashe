############################################
# FUNCTIONS TO MAKE IT EASIER TO DEAL WITH PROJECT DIRECTORIES
################################################

#imports
import os

#identify project root
def find_project_root(marker_folder = "bronze_files"):
    
    """
    Recursively searches parent directories to locate the project root based on a specified marker folder.

    This function starts from the directory of the current script and moves upward through parent 
    directories until it finds one that contains the specified marker folder. It is useful for ensuring 
    consistent, root-relative paths in modular Python projects.

    Parameters:
        marker_folder (str): The name of a folder that signifies the project root 
                             (e.g., 'bronze_files', '.git'). Defaults to 'bronze_files'.

    Returns:
        str: Absolute path to the project root directory containing the marker folder.

    Raises:
        RuntimeError: If no directory containing the marker folder is found before reaching the filesystem root.
    """
    
    path = os.path.abspath(__file__)
    
    while True:
        path = os.path.dirname(path)
        if os.path.exists(os.path.join(path, marker)):
            return path
        if path == os.path.dirname(path):  # Reached root of filesystem
            raise RuntimeError("Project root not found. Please ensure specified marker folder exists.")