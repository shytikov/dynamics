import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='pandyn',
    version='0.1',
    scripts=['pandyn.py'],
    author="Alexey Shytikov",
    author_email="alexey.shytikov@gmail.com",
    description="A pandas helper to interact with MS Dynamics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shytikov/pandyn",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
