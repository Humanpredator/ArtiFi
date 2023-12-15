from setuptools import find_packages, setup

with open("requirements.txt", "r") as file:
    requirements = file.read().splitlines()

with open("README.md", "r", encoding="utf-8") as readme_file:
    long_description = readme_file.read()

setup(
    name="Artifi",  # Replace with your package name
    version="0.1.0",  # Initial version, follow Semantic Versioning (SemVer)
    author="Humanpredator",
    author_email="humanpredator@reality.org.in",
    description="A Automation Tool Made By Noob",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Humanpredator/ArtiFi",  # Replace with your repository URL
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU GENERAL PUBLIC LICENSE",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
)
