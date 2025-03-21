import subprocess

for _ in range(3):
    subprocess.Popen(["gnome-terminal", "--", "bash", "-c", "go run . ; exec bash"])
