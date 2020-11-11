from setuptools import find_packages, setup

with open("requirements.txt", "r", encoding="utf-8") as f:
    requires = []
    for line in f:
        req = line.split("#", 1)[0].strip()
        if req and not req.startswith("--"):
            requires.append(req)

setup(
    name="can_tools",
    version="0.2",
    url="https://github.com/valorumdata/cmdc-tools",
    packages=find_packages(exclude=["tests"]),
    install_requires=requires,
)
