import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="async-bitmex-ws",  # Replace with your own username
    version="0.9.9",
    author="Antas",
    author_email="",
    description="Asynchronous bitmex websocket api development kit",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/monk-after-90s/async_bitmex_websocket.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
