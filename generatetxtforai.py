import os

def export_files_to_txt(root_folder, output_file="output.txt"):
    # Folders to ignore (add more if needed)
    IGNORE_DIRS = {"venv", "__pycache__", ".venv", ".git", ".mypy_cache"}

    with open(output_file, "w", encoding="utf-8") as out:
        for root, dirs, files in os.walk(root_folder):

            # Remove ignored folders from walk
            dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]

            for file in files:
                if file.endswith(".html") or file.endswith(".py"):
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, root_folder)

                    # Read file content with fallback encoding
                    try:
                        with open(full_path, "r", encoding="utf-8") as f:
                            content = f.read()
                    except UnicodeDecodeError:
                        with open(full_path, "r", encoding="latin1") as f:
                            content = f.read()

                    # Write header
                    out.write("\n" + "=" * 80 + "\n")
                    out.write(f"FILE: {relative_path}\n")
                    out.write("=" * 80 + "\n\n")

                    # Write content
                    out.write(content)
                    out.write("\n\n")

    print(f"Done! Output saved to: {output_file}")


# Example usage:
export_files_to_txt(os.curdir, "combined_output.txt")